package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
)

const containerBatchCalls = 256

// Run through benchmark/scripts/container-batch-get.sh. The fixture lives on
// the data volume; writing measurements to another mount stays outside timing.
func TestContainerBatchGet(t *testing.T) {
	output := os.Getenv("TARP_CONTAINER_BATCH_OUTPUT")
	if output == "" {
		t.Skip("opt-in container BatchGet measurement")
	}
	root := os.Getenv("TARP_CONTAINER_BATCH_ROOT")
	image := os.Getenv("TARP_CONTAINER_BATCH_IMAGE")
	revision := os.Getenv("TARP_CONTAINER_BATCH_REVISION")
	backend := os.Getenv("TARP_CONTAINER_BATCH_BACKEND")
	if root == "" || !strings.Contains(image, "@sha256:") || revision == "" || backend == "" ||
		runtime.Version() != os.Getenv("TARP_CONTAINER_BATCH_GO") {
		t.Fatal("incomplete container provenance or mismatched Go version; run container-batch-get.sh")
	}
	dir, err := os.MkdirTemp(root, "tarp-batch-get-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	encoder := json.NewEncoder(file)
	write := func(t *testing.T, row any) {
		t.Helper()
		if err := encoder.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	kernel, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		t.Fatal(err)
	}
	// Each setting runs first, second and third twice across the six rounds.
	// A zero option exercises the public default (8 on this release).
	orders := [][]int{{1, 0, 32}, {0, 32, 1}, {32, 1, 0}, {32, 0, 1}, {0, 1, 32}, {1, 32, 0}}
	write(t, map[string]any{
		"kind": "environment", "schema": 1, "time_utc": time.Now().UTC(), "release": revision,
		"image": image, "go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"kernel": strings.TrimSpace(string(kernel)), "backend": backend, "gomaxprocs": runtime.GOMAXPROCS(0),
		"rounds": len(orders), "requests_per_trial": containerBatchCalls, "slots_per_request": 32,
		"primary_io_counter": "/proc/self/io read_bytes", "device_counter": "mount block device sectors read (512 bytes/sector), when visible",
	})
	for _, size := range []int{64, 64 << 10} {
		dbDir := filepath.Join(dir, fmt.Sprintf("db-%d", size))
		fixture := prepareContainerBatchFixture(t, dbDir, size, containerBatchCalls)
		storage, err := openContainerBatchIO(dbDir)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			defer func() {
				if err := storage.Close(); err != nil {
					t.Error(err)
				}
			}()
			write(t, map[string]any{"kind": "fixture", "value_bytes": size, "filesystem": storage.Filesystem, "device": storage.Device})
			for round, limits := range orders {
				seed := int64(72_000 + round)
				traces := make(map[string][]containerBatchRequest)
				for _, mode := range []string{"distinct", "hit", "duplicate"} {
					traces[mode] = fixture.trace(seed, mode)
				}
				for position, option := range limits {
					t.Run(fmt.Sprintf("round-%d/option-%d", round+1, option), func(t *testing.T) {
						measureContainerBatchLimit(t, fixture, storage, option, round, position, seed, size, traces, write)
					})
				}
			}
		})
	}
	if !t.Failed() {
		write(t, map[string]any{"kind": "complete"})
	}
}

// A new DB shares the same committed files and request trace across option settings.
func measureContainerBatchLimit(t *testing.T, fixture *containerBatchFixture, storage *containerBatchIO, option, round, position int, seed int64, size int, traces map[string][]containerBatchRequest, write func(*testing.T, any)) {
	t.Helper()
	db := fixture.open(t, option)
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	options := engine.Observe((*engine.DB)(db)).Options()
	before := db.Stats()
	if before.SelectedSegments != uint64(len(storage.files)) || before.ObsoletePutBytes != 0 {
		t.Fatal("expected all private DATA files to be selected and live before measurement")
	}
	// Exercise the read paths before sampling process-attributed I/O.
	if _, err := runContainerBatchRequests(db, traces["distinct"][:8], 8); err != nil {
		t.Fatal(err)
	}
	clients := []int{1, 8}
	if (round+position)%2 != 0 {
		slices.Reverse(clients)
	}
	for _, n := range clients {
		scenarios := []string{"warm", "evicted", "hit", "duplicate"}
		if (round+position+n)%2 != 0 {
			slices.Reverse(scenarios)
		}
		for _, scenario := range scenarios {
			trace := traces["distinct"]
			if scenario == "hit" || scenario == "duplicate" {
				trace = traces[scenario]
			}
			row := containerBatchTrial{Kind: "trial", Round: round + 1, Seed: seed, ValueBytes: size,
				Clients: n, Option: option, Limit: options.BatchReadConcurrency, Scenario: scenario}
			row.measure(t, db, storage, trace)
			write(t, row)
		}
	}
	after := db.Stats()
	if after.SelectedSegments != before.SelectedSegments || after.SelectedDataBytes != before.SelectedDataBytes ||
		after.LiveKeys != before.LiveKeys || after.WriteCommits != before.WriteCommits {
		t.Fatal("fixture layout or contents changed during a read-only comparison")
	}
}

type containerBatchTrial struct {
	Kind              string               `json:"kind"`
	Round             int                  `json:"round"`
	Seed              int64                `json:"seed"`
	ValueBytes        int                  `json:"value_bytes"`
	Clients           int                  `json:"clients"`
	Option            int                  `json:"option"`
	Limit             int                  `json:"limit"`
	Scenario          string               `json:"scenario"`
	IOStatus          string               `json:"io_status"`
	IONote            string               `json:"io_note,omitempty"`
	ReadBytes         *uint64              `json:"process_read_bytes"`
	DeviceBytes       *uint64              `json:"device_read_bytes"`
	WallNS            int64                `json:"wall_ns,omitempty"`
	RequestsPerSecond float64              `json:"requests_per_second,omitempty"`
	Latency           *sampledLatency      `json:"latency,omitempty"`
	ByClient          []sampledLatency     `json:"by_client,omitempty"`
	Stats             *containerBatchStats `json:"stats,omitempty"`
}

func (row *containerBatchTrial) measure(t *testing.T, db *tarp.DB, storage *containerBatchIO, trace []containerBatchRequest) {
	t.Helper()
	switch row.Scenario {
	case "evicted":
		if err := storage.Evict(); err != nil {
			row.IOStatus, row.IONote = "unsupported", err.Error()
			return
		}
	case "warm", "duplicate":
		if err := storage.Warm(); err != nil {
			t.Fatal(err)
		}
	}
	deviceBefore, deviceStartErr := storage.DeviceReadBytes()
	before, startErr := storage.ReadBytes()
	run, err := runContainerBatchRequests(db, trace, row.Clients)
	after, endErr := storage.ReadBytes()
	deviceAfter, deviceEndErr := storage.DeviceReadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if deviceStartErr == nil && deviceEndErr == nil && deviceAfter >= deviceBefore {
		delta := deviceAfter - deviceBefore
		row.DeviceBytes = &delta
	}
	switch {
	case startErr != nil:
		row.IOStatus, row.IONote = "unsupported", startErr.Error()
	case endErr != nil:
		row.IOStatus, row.IONote = "unsupported", endErr.Error()
	case after < before:
		row.IOStatus, row.IONote = "unsupported", "process read_bytes decreased"
	default:
		delta := after - before
		row.ReadBytes = &delta
		row.IOStatus = "no-read-observed"
		if delta > 0 {
			row.IOStatus = "read-observed"
		} else if row.Scenario == "evicted" {
			row.IOStatus, row.IONote = "unsupported", "no process read bytes after fixture eviction"
		}
	}
	row.Stats = &run.StatsDelta
	row.WallNS = run.WallNS
	row.RequestsPerSecond = float64(len(trace)) * float64(time.Second) / float64(run.WallNS)
	var all []int64
	for _, samples := range run.SamplesNS {
		row.ByClient = append(row.ByClient, latency(samples, int64(len(samples)), 0))
		all = append(all, samples...)
	}
	total := latency(all, int64(len(all)), time.Duration(run.WallNS))
	total.SamplesNS = nil // caller-ordered samples are already in by_client
	row.Latency = &total
}
