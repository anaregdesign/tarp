package benchmark

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

// TestKeyCheckpointMeasurement is an opt-in paired measurement. Each arm
// opens a private copy of the same closed fixture. The foreground rows include
// all scheduled checkpoint joins and separately identify calls that overlap.
func TestKeyCheckpointMeasurement(t *testing.T) {
	if os.Getenv("TARP_KEY_CHECKPOINT_MEASURE") != "1" {
		t.Skip("set TARP_KEY_CHECKPOINT_MEASURE=1 to run")
	}
	edges := edgePrefixData(true)
	var keyBytes int
	for i := range edges {
		e := &edges[i]
		// Preserve the source prefix and unique destination. Long encoded
		// keys make the sort exceed 4 MiB even before descriptor charges.
		e.Key = append(e.Key, bytes.Repeat(e.Value[:32], 8)...)
		e.Value = bytes.Repeat(e.Value, 1+int(e.Value[0])%6)
		keyBytes += len(e.Key)
	}
	if keyBytes <= 4<<20 {
		t.Fatalf("key bytes %d do not exceed the sort window", keyBytes)
	}
	opts := tarp.Options{IndexMode: tarp.IndexBTree, MaxRecordBytes: 64 << 10, TargetSegmentBytes: 4 << 20}
	seed := newEdgePrefixFixture(t, opts, edges)
	seedDir := engine.Observe((*engine.DB)(seed)).Dir()
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	sorted := slices.Clone(edges)
	slices.SortFunc(sorted, func(a, b tarp.Entry) int { return bytes.Compare(a.Key, b.Key) })
	for _, index := range []struct {
		name string
		mode tarp.IndexMode
	}{{"btree", tarp.IndexBTree}, {"memory", tarp.IndexMemory}} {
		t.Run(index.name, func(t *testing.T) {
			opts := opts
			opts.IndexMode = index.mode
			var orderedDir string
			for _, layout := range []string{"source", "ordered"} {
				dir := filepath.Join(t.TempDir(), "checkpoint")
				measurementCloneClosed(t, seedDir, dir)
				db, err := tarp.OpenExisting(t.Context(), dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				if layout == "source" {
					_, err = db.Compact(t.Context())
				} else {
					_, err = db.CompactByKey(t.Context())
					orderedDir = dir
				}
				elapsed := time.Since(start)
				if err != nil {
					t.Fatal(err)
				}
				measureKeyCheckpointPrefixes(t, db, dir, index.name, layout, edges, keyBytes, elapsed)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, pattern := range []string{"first", "hot", "spread"} {
				t.Run(pattern, func(t *testing.T) {
					const rounds = 3
					arms := [...]string{"foreground", "source", "ordered"}
					totals := make(map[string]int)
					for round := range rounds {
						trials := make(map[string]checkpointWriterTrial, len(arms))
						for j := range arms {
							arm := arms[(j+round)%len(arms)]
							trial := measureKeyCheckpointWrites(t, orderedDir, opts, sorted, index.name, pattern, arm, round)
							totals[arm] += len(trial.overlapSlots)
							trials[arm] = trial
						}
						for _, arm := range arms[1:] {
							reportPairedCheckpoint(t, index.name, pattern, arm, round, trials["foreground"], trials[arm])
						}
					}
					for _, arm := range arms[1:] {
						n := totals[arm]
						keyCheckpointRow(t, map[string]any{"kind": "overlap_coverage", "index": index.name, "pattern": pattern,
							"arm": arm, "overlapping_puts": n, "target": 1000, "target_met": n >= 1000})
					}
				})
			}
		})
	}
}

func keyCheckpointRow(t *testing.T, row map[string]any) {
	t.Helper()
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("KEY_CHECKPOINT_MEASURE %s", data)
}

func measureKeyCheckpointPrefixes(t *testing.T, db *tarp.DB, dir, index, layout string, edges []tarp.Entry, keyBytes int, checkpoint time.Duration) {
	t.Helper()
	(edgePrefixQuery{}).verify(t, db, edges, true, db)
	device, err := openContainerBatchIO(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := device.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := device.Evict(); err != nil {
		t.Fatal(err)
	}
	before := readPrefixIOBytes(t, device)
	if err := device.Warm(); err != nil {
		t.Fatal(err)
	}
	control := prefixIOBytesSince(t, device, before)
	if control == 0 {
		t.Fatal("fixture-only eviction followed by a full DATA read charged zero process read_bytes")
	}
	for _, tail := range []int{0, 192, 384} {
		q := edgePrefixQuery{prefix: edgeTailPrefix(tail)}
		n := q.verify(t, db, edges, true, db)
		for trial := 1; trial <= 3; trial++ {
			if err := device.Evict(); err != nil {
				t.Fatal(err)
			}
			before := readPrefixIOBytes(t, device)
			start := time.Now()
			got, err := q.run(t.Context(), db, true, db, nil)
			elapsed := time.Since(start)
			charged := prefixIOBytesSince(t, device, before)
			if err != nil || got.matches != n {
				t.Fatalf("ScanPrefix(%x) = %d, %v; want %d", q.prefix, got.matches, err, n)
			}
			keyCheckpointRow(t, map[string]any{
				"kind": "cold_prefix", "index": index, "arm": layout, "tail": tail, "trial": trial, "matches": n,
				"elapsed_ns": elapsed.Nanoseconds(), "process_read_bytes": charged, "full_read_control_bytes": control,
				"checkpoint_ns": checkpoint.Nanoseconds(), "live_keys": len(edges), "encoded_key_bytes": keyBytes,
				"data_files": len(device.files), "filesystem": device.Filesystem, "device": device.Device,
			})
		}
	}
}

type checkpointPut struct {
	slot  int
	value []byte
}

// Precompute the byte-identical trace for paired arms. Include the call number
// in every value so an old version cannot satisfy the final verification.
func keyCheckpointTrace(edges []tarp.Entry, pattern string, round, count int) []checkpointPut {
	trace := make([]checkpointPut, count)
	for i := range trace {
		slot := 0
		switch pattern {
		case "hot":
			slot = (i*37 + round*17) % 96
		case "spread":
			slot = (i*7919 + round*997) % len(edges)
		}
		value := bytes.Clone(edges[slot].Value)
		binary.BigEndian.PutUint64(value, uint64(i+1))
		trace[i] = checkpointPut{slot: slot, value: value}
	}
	return trace
}

type checkpointInterval struct {
	start, end time.Time
	err        error
}

// Retain one latency and at most one overlap slot per foreground call until
// the three paired arms complete. The same slot always represents the same Put.
type checkpointWriterTrial struct {
	samples      []int64
	overlapSlots []int
	serviceRate  float64
	totalRate    float64
}

func reportPairedCheckpoint(t *testing.T, index, pattern, arm string, round int, baseline, trial checkpointWriterTrial) {
	t.Helper()
	row := map[string]any{
		"kind": "paired_foreground", "index": index, "pattern": pattern, "arm": arm, "trial": round + 1,
		"put_calls_per_second_ratio": trial.serviceRate / baseline.serviceRate,
		"puts_per_second_ratio":      trial.totalRate / baseline.totalRate, "overlapping_puts": len(trial.overlapSlots),
	}
	if len(trial.overlapSlots) != 0 {
		matched, during := make([]int64, len(trial.overlapSlots)), make([]int64, len(trial.overlapSlots))
		var baselineNS, overlapNS int64
		for i, slot := range trial.overlapSlots {
			matched[i], during[i] = baseline.samples[slot], trial.samples[slot]
			baselineNS += matched[i]
			overlapNS += during[i]
		}
		base99 := latency(matched, int64(len(matched)), 0).P99NS
		trial99 := latency(during, int64(len(during)), 0).P99NS
		row["matched_baseline_put_p99_ns"] = base99
		row["overlap_put_p99_ratio"] = float64(trial99) / float64(base99)
		row["overlap_put_calls_per_second_ratio"] = float64(baselineNS) / float64(overlapNS)
	}
	keyCheckpointRow(t, row)
}

// Measure one sorted-base trial. The initial mutations ensure that even the
// first requested checkpoint has work. All arms receive the same foreground
// calls. Normal automatic cleaning and parity protection stay enabled.
func measureKeyCheckpointWrites(t *testing.T, source string, opts tarp.Options, edges []tarp.Entry, index, pattern, arm string, round int) checkpointWriterTrial {
	t.Helper()
	const seedPuts, epochs, perEpoch = 64, 4, 256
	dir := filepath.Join(t.TempDir(), "db")
	measurementCloneClosed(t, source, dir)
	// The IO seam reports actual encoded bytes accepted. The named points
	// belong exclusively to the manual DATA checkpoint and its own parity.
	var dataWrites, parityWrites atomic.Int64
	core, err := engine.OpenWithIO(dir, engine.Options(opts), &storage.IO{Wrote: func(point string, n int) {
		switch point {
		case "compact.segment.write", "compact.write":
			dataWrites.Add(int64(n))
		case "compact.parity.write":
			parityWrites.Add(int64(n))
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	db := (*tarp.DB)(core)
	defer db.Close()
	// Re-establish the sorted base on every new handle, outside timing, so
	// measured calls include updates to the certified base on all arms.
	if _, err := db.CompactByKey(t.Context()); err != nil {
		t.Fatal(err)
	}
	trace := keyCheckpointTrace(edges, pattern, round, seedPuts+epochs*perEpoch)
	for _, put := range trace[:seedPuts] {
		if err := db.Put(edges[put.slot].Key, put.value); err != nil {
			t.Fatal(err)
		}
	}
	var samples, overlapping []int64
	var overlapSlots []int
	var checkpointTime, joinTime, writerBusy, overlapBusy, overlappingCallTime time.Duration
	// Ignore writes from unmeasured base certification and fixture setup.
	beforeDATA, beforePARITY := dataWrites.Load(), parityWrites.Load()
	ctx := t.Context()
	start := time.Now()
	for epoch := range epochs {
		done := make(chan checkpointInterval, 1)
		if arm != "foreground" {
			ready := make(chan struct{})
			go func() {
				window := checkpointInterval{start: time.Now()}
				close(ready)
				window.err = runKeyCheckpoint(ctx, db, arm)
				window.end = time.Now()
				done <- window
			}()
			<-ready
		}
		var calls [perEpoch]repackCall
		var putErr error
		for i, put := range trace[seedPuts+epoch*perEpoch:][:perEpoch] {
			calls[i].begin = time.Now()
			putErr = db.Put(edges[put.slot].Key, put.value)
			calls[i].end = time.Now()
			if putErr != nil {
				break
			}
		}
		var window checkpointInterval
		if arm != "foreground" {
			window = <-done
			if window.err != nil {
				t.Fatal(window.err)
			}
			checkpointTime += window.end.Sub(window.start)
			// The writer cannot begin the next epoch until this checkpoint
			// finishes. Count only its remaining execution after the final Put.
			if putErr == nil {
				joinTime += max(0, window.end.Sub(calls[perEpoch-1].end))
			}
		}
		if putErr != nil {
			t.Fatal(putErr)
		}
		for _, call := range calls {
			duration := call.end.Sub(call.begin)
			writerBusy += duration
			samples = append(samples, duration.Nanoseconds())
			if arm != "foreground" && call.begin.Before(window.end) && call.end.After(window.start) {
				overlapping = append(overlapping, duration.Nanoseconds())
				overlapSlots = append(overlapSlots, len(samples)-1)
				overlappingCallTime += duration
				// Clip both boundary calls to the checkpoint interval. One
				// writer issues the Puts serially, so these spans are disjoint.
				start, end := call.begin, call.end
				if window.start.After(start) {
					start = window.start
				}
				if window.end.Before(end) {
					end = window.end
				}
				overlapBusy += end.Sub(start)
			}
		}
	}
	elapsed := time.Since(start)
	writtenDATA, writtenPARITY := dataWrites.Load()-beforeDATA, parityWrites.Load()-beforePARITY
	if (arm == "foreground") != (writtenDATA == 0 && writtenPARITY == 0) || (writtenDATA == 0) != (writtenPARITY == 0) {
		t.Fatalf("arm %s counted %d checkpoint DATA bytes and %d PARITY bytes", arm, writtenDATA, writtenPARITY)
	}
	if writerBusy+joinTime > elapsed || overlapBusy > checkpointTime || overlapBusy > overlappingCallTime {
		t.Fatalf("invalid timing: elapsed %s, busy %s, join %s, checkpoint %s, overlap busy %s, overlapping calls %s", elapsed, writerBusy, joinTime, checkpointTime, overlapBusy, overlappingCallTime)
	}
	// Validate from a fresh Full open; the writer's value cache must never
	// supply the oracle. Untouched records and row count are checked as well.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.OpenValidation = tarp.Full
	image, err := tarp.OpenExisting(ctx, dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	expected := slices.Clone(edges)
	for _, put := range trace {
		expected[put.slot].Value = put.value
	}
	(edgePrefixQuery{}).verify(t, image, expected, true, image)
	q := latency(samples, int64(len(samples)), elapsed)
	during := latency(overlapping, int64(len(overlapping)), checkpointTime)
	trial := checkpointWriterTrial{samples: samples, overlapSlots: overlapSlots,
		serviceRate: float64(len(samples)) / writerBusy.Seconds(), totalRate: float64(len(samples)) / elapsed.Seconds()}
	keyCheckpointRow(t, map[string]any{
		"kind": "foreground", "index": index, "pattern": pattern, "arm": arm, "trial": round + 1,
		"puts": len(samples), "seed_puts": seedPuts, "elapsed_ns": elapsed.Nanoseconds(), "puts_per_second": trial.totalRate,
		"put_p50_ns": q.P50NS, "put_p99_ns": q.P99NS, "overlapping_puts": len(overlapping), "overlap_put_p99_ns": during.P99NS,
		"writer_busy_ns": writerBusy.Nanoseconds(), "put_calls_per_second": trial.serviceRate,
		"checkpoint_window_ns": checkpointTime.Nanoseconds(), "checkpoint_join_ns": joinTime.Nanoseconds(),
		"overlap_writer_busy_ns": overlapBusy.Nanoseconds(), "overlapping_put_call_ns": overlappingCallTime.Nanoseconds(),
		"checkpoint_data_written_bytes": writtenDATA, "checkpoint_parity_written_bytes": writtenPARITY,
		"epochs": epochs, "verified_live_keys": len(expected),
	})
	return trial
}

func runKeyCheckpoint(ctx context.Context, db *tarp.DB, arm string) error {
	switch arm {
	case "source":
		_, err := db.Compact(ctx)
		return err
	case "ordered":
		_, err := db.CompactByKey(ctx)
		return err
	default:
		return fmt.Errorf("unknown checkpoint arm %q", arm)
	}
}
