package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

// TestLocalityForeground compares durable public Put on identical cloned
// workloads. Side-file arms approximate immutable-range copy I/O only; there
// is no candidate index, crash protocol, or candidate foreground write path.
const (
	localityRows      = 8192
	localityValueSize = 512
	localityEpochPuts = 64
	localityEpochs    = 4
)

type localityArm struct {
	name   string
	budget int // bytes of immutable source copied per epoch, plus the same output
}

type localityInterval struct {
	Start int64 `json:"start_ns"`
	End   int64 `json:"end_ns"`
}

func (w localityInterval) overlaps(other localityInterval) bool {
	return w.Start < other.End && other.Start < w.End
}

type localityEpoch struct {
	localityInterval
	Offset          int64               `json:"range_offset"`
	ReadBytes       int                 `json:"side_read_bytes"`
	WriteBytes      int                 `json:"side_write_bytes"`
	Read            localityInterval    `json:"read_syscall"`
	Write           localityInterval    `json:"write_syscall"`
	Sync            localityInterval    `json:"file_sync_syscall"`
	DirSync         localityInterval    `json:"dir_sync_syscall"`
	Compact         *tarp.CompactResult `json:"compact,omitempty"`
	WorkerOverlaps  int                 `json:"put_calls_overlapping_worker"`
	SyscallOverlaps int                 `json:"put_calls_overlapping_side_syscalls"`
}

func localityKey(id int) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(id))
}

func localityValue(id, version int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(uint64(id), seed^uint64(version)))
	v := make([]byte, localityValueSize)
	for i := 0; i < len(v); i += 8 {
		binary.LittleEndian.PutUint64(v[i:], r.Uint64())
	}
	binary.BigEndian.PutUint64(v, uint64(id))
	binary.BigEndian.PutUint64(v[8:], uint64(version))
	return v
}

func localityFixture(t *testing.T, root string) (string, []byte) {
	t.Helper()
	data := make([]byte, 0, localityRows*localityValueSize)
	dir := filepath.Join(root, "fixture")
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	entries := make([]tarp.Entry, 0, 256)
	for i := range localityRows {
		value := localityValue(i, 0, 150)
		data = append(data, value...)
		entries = append(entries, tarp.Entry{Key: localityKey(i), Value: value})
		if len(entries) == cap(entries) {
			if err := db.BatchPut(entries); err != nil {
				t.Fatal(err)
			}
			entries = entries[:0]
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, data
}

type localityTrace struct {
	updates []tarp.Entry
	digest  string
}

func localityUpdates(spread bool, seed uint64) localityTrace {
	r := rand.New(rand.NewPCG(seed, 150))
	h := sha256.New()
	updates := make([]tarp.Entry, localityEpochs*localityEpochPuts)
	for i := range updates {
		id := 0
		if spread {
			id = r.IntN(localityRows)
		}
		e := tarp.Entry{Key: localityKey(id), Value: localityValue(id, i+1, seed)}
		h.Write(e.Key)
		h.Write(e.Value)
		updates[i] = e
	}
	return localityTrace{updates, fmt.Sprintf("%x", h.Sum(nil))}
}

// Each copy reads a page-cache-warm immutable range, creates a separate output,
// writes the exact range, and syncs the file and its directory. All files are
// on the same filesystem as the Tarp DB. No filesystem cache eviction is used.
func localityCopyRange(origin time.Time, w *localityEpoch, source, directory *os.File, path string, buf []byte) error {
	w.Read.Start = time.Since(origin).Nanoseconds()
	n, err := source.ReadAt(buf, w.Offset)
	w.Read.End = time.Since(origin).Nanoseconds()
	w.ReadBytes = n
	if err != nil {
		return err
	}
	out, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	w.Write.Start = time.Since(origin).Nanoseconds()
	n, err = out.Write(buf)
	w.Write.End = time.Since(origin).Nanoseconds()
	w.WriteBytes = n
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return errors.Join(err, out.Close())
	}
	w.Sync.Start = time.Since(origin).Nanoseconds()
	err = out.Sync()
	w.Sync.End = time.Since(origin).Nanoseconds()
	if err := errors.Join(err, out.Close()); err != nil {
		return err
	}
	w.DirSync.Start = time.Since(origin).Nanoseconds()
	err = directory.Sync()
	w.DirSync.End = time.Since(origin).Nanoseconds()
	return err
}

func localityCheckReopen(t *testing.T, dir string, initial []byte, trace localityTrace) {
	t.Helper()
	want := make([][]byte, localityRows)
	for i := range want {
		want[i] = initial[i*localityValueSize : (i+1)*localityValueSize]
	}
	for _, update := range trace.updates {
		want[binary.BigEndian.Uint64(update.Key)] = update.Value
	}
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	count := 0
	for e, err := range db.Scan(t.Context()) {
		if err != nil {
			t.Fatal(err)
		}
		if len(e.Key) != 8 {
			t.Fatalf("invalid key length %d", len(e.Key))
		}
		id := binary.BigEndian.Uint64(e.Key)
		if id >= uint64(len(want)) || want[id] == nil || !bytes.Equal(want[id], e.Value) {
			t.Fatalf("unexpected or stale reopened row %d", id)
		}
		want[id] = nil
		count++
	}
	if count != len(want) {
		t.Fatalf("reopened %d/%d rows", count, len(want))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func localityForegroundTrial(t *testing.T, fixture string, initial []byte, source *os.File, trace localityTrace, traceName string, seed uint64, arm localityArm, round int, output, cpus string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "db")
	measurementCloneClosed(t, fixture, dir)
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	directory, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	// Warm the same source page cache and start with freshly opened DB handles
	// for every arm. Preparation and reopen verification are outside timing.
	if _, err := io.Copy(io.Discard, io.NewSectionReader(source, 0, int64(len(initial)))); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, arm.budget)
	putStart := make([]int64, len(trace.updates))
	putNS := make([]int64, len(trace.updates))
	epochs := make([]localityEpoch, localityEpochs)
	var foregroundNS, sideRead, sideWrite int64
	var overlapped, syscallOverlap int
	origin := time.Now()
	for epoch := range epochs {
		w := &epochs[epoch]
		lo := epoch * localityEpochPuts
		hi := lo + localityEpochPuts
		path := filepath.Join(root, fmt.Sprintf("range-%d", epoch))
		if arm.budget != 0 {
			offset := int(binary.BigEndian.Uint64(trace.updates[lo].Key)) * localityValueSize
			w.Offset = int64(offset / arm.budget * arm.budget)
		}
		ready, start, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			close(ready)
			<-start
			w.Start = time.Since(origin).Nanoseconds()
			var err error
			switch {
			case arm.budget != 0:
				err = localityCopyRange(origin, w, source, directory, path, buf)
			case arm.name == "compact":
				var result tarp.CompactResult
				result, err = db.Compact(t.Context())
				w.Compact = &result
			}
			w.End = time.Since(origin).Nanoseconds()
			done <- err
		}()
		<-ready
		first := time.Now()
		for i := lo; i < hi; i++ {
			begin := time.Now()
			err = db.Put(trace.updates[i].Key, trace.updates[i].Value)
			putNS[i] = time.Since(begin).Nanoseconds()
			putStart[i] = begin.Sub(origin).Nanoseconds()
			if i == lo {
				// Give every worker a committed change and the remaining 63
				// writes to overlap. In particular, Compact cannot take its
				// no-new-records shortcut just because it won a scheduling race.
				close(start)
			}
			if err != nil {
				break
			}
		}
		foregroundNS += time.Since(first).Nanoseconds()
		if err := errors.Join(err, <-done); err != nil {
			t.Fatal(err)
		}
		if w.Compact != nil && w.Compact.DroppedKeys != 0 {
			t.Fatal("manual checkpoint dropped live keys")
		}
		sideRead += int64(w.ReadBytes)
		sideWrite += int64(w.WriteBytes)
		for i := lo; i < hi; i++ {
			put := localityInterval{putStart[i], putStart[i] + putNS[i]}
			if arm.name != "default" && put.overlaps(w.localityInterval) {
				w.WorkerOverlaps++
			}
			if arm.budget != 0 && (put.overlaps(w.Read) || put.overlaps(w.Write) || put.overlaps(w.Sync) || put.overlaps(w.DirSync)) {
				w.SyscallOverlaps++
			}
		}
		overlapped += w.WorkerOverlaps
		syscallOverlap += w.SyscallOverlaps
	}
	wall := time.Since(origin)
	stats := db.Stats()
	if stats.WriteCommits != uint64(len(trace.updates)) || stats.WrittenRecords != uint64(len(trace.updates)) {
		t.Fatalf("incorrect foreground publication counters: %+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	localityCheckReopen(t, dir, initial, trace)
	if arm.budget != 0 {
		for i, w := range epochs {
			got, err := os.ReadFile(filepath.Join(root, fmt.Sprintf("range-%d", i)))
			if err != nil || !bytes.Equal(got, initial[w.Offset:w.Offset+int64(arm.budget)]) {
				t.Fatalf("immutable range %d failed verification: %v", i, err)
			}
		}
	}
	samples := latency(putNS, int64(len(putNS)), time.Duration(foregroundNS))
	valueBytes := float64(len(putNS) * localityValueSize)
	row := map[string]any{
		"trial": round, "trace": traceName, "trace_seed": seed, "trace_sha256": trace.digest, "arm": arm.name,
		"go_version": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0), "cpus_allowed_list": cpus,
		"initial_rows": localityRows, "reopen_rows_verified": localityRows, "side_outputs_verified": arm.budget != 0,
		"value_bytes": localityValueSize, "put_calls_per_epoch": localityEpochPuts, "side_budget_output_bytes_per_epoch": arm.budget,
		"side_read_bytes": sideRead, "side_write_bytes": sideWrite, "side_output_per_put_value_byte": float64(sideWrite) / valueBytes,
		"side_read_plus_write_per_put_value_byte": float64(sideRead+sideWrite) / valueBytes,
		"put_latency": samples, "put_start_ns": putStart, "maintenance_epochs": epochs,
		"put_calls_overlapping_worker": overlapped, "put_calls_overlapping_side_syscalls": syscallOverlap,
		"wall_ns_including_worker_joins": wall.Nanoseconds(), "updates_per_s_including_worker_joins": float64(len(putNS)) / wall.Seconds(),
		"updates_per_s_during_put_loops": float64(len(putNS)) * 1e9 / float64(foregroundNS), "tarp_stats": stats,
	}
	file, err := os.OpenFile(filepath.Join(output, fmt.Sprintf("%02d-%s-%s.json", round, traceName, arm.name)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(json.NewEncoder(file).Encode(row), file.Close()); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s %s round=%d puts=%d p50/p95/p99_ns=%d/%d/%d worker_overlap=%d side_syscall_overlap=%d wall=%s", traceName, arm.name, round, len(putNS), samples.P50NS, samples.P95NS, samples.P99NS, overlapped, syscallOverlap, wall)
}

func TestLocalityForeground(t *testing.T) {
	if os.Getenv("TARP_LOCALITY_FOREGROUND") != "1" {
		t.Skip("set TARP_LOCALITY_FOREGROUND=1 to run the foreground experiment")
	}
	output := os.Getenv("TARP_LOCALITY_FOREGROUND_OUTPUT")
	if output == "" {
		t.Fatal("set TARP_LOCALITY_FOREGROUND_OUTPUT to an empty archive directory")
	}
	if err := os.MkdirAll(output, 0700); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "linux" || runtime.GOMAXPROCS(0) < 2 || runtime.NumCPU() < 2 {
		t.Fatal("run in the Linux container pinned to at least two CPUs, with GOMAXPROCS >= 2")
	}
	status, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Fatal(err)
	}
	var cpus string
	for line := range strings.Lines(string(status)) {
		if rest, ok := strings.CutPrefix(line, "Cpus_allowed_list:"); ok {
			cpus = strings.TrimSpace(rest)
		}
	}
	if cpus == "" {
		t.Fatal("could not determine Linux CPU affinity")
	}
	rounds := 3
	if s := os.Getenv("TARP_LOCALITY_FOREGROUND_ROUNDS"); s != "" {
		rounds, err = strconv.Atoi(s)
		if err != nil || rounds < 1 {
			t.Fatal("invalid TARP_LOCALITY_FOREGROUND_ROUNDS")
		}
	}
	root := t.TempDir()
	fixture, initial := localityFixture(t, root)
	path := filepath.Join(root, "immutable-source")
	source, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Write(initial); err != nil {
		t.Fatal(err)
	}
	if err := source.Sync(); err != nil {
		t.Fatal(err)
	}
	arms := []localityArm{{"default", 0}, {"side-near", 16 << 10}, {"side-stress", 1 << 20}, {"compact", 0}}
	for round := range rounds {
		seed := uint64(150 + round)
		for j := range 2 {
			spread := (j+round)%2 != 0
			traceName := "hot-first"
			if spread {
				traceName = "spread"
			}
			trace := localityUpdates(spread, seed)
			for i := range arms {
				arm := arms[(i+round+j)%len(arms)]
				t.Run(fmt.Sprintf("%02d/%s/%s", round, traceName, arm.name), func(t *testing.T) {
					localityForegroundTrial(t, fixture, initial, source, trace, traceName, seed, arm, round, output, cpus)
				})
			}
		}
	}
}
