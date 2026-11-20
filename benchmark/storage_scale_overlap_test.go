package benchmark

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/scaleprobe"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

const (
	overlapCapturedKeys = 16 << 10
	overlapBatchSize    = 512
	overlapWindow       = 3 * time.Second
)

func overlapEntries(first, step, count int) []engine.Entry {
	entries := make([]engine.Entry, count)
	for i := range entries {
		id := uint64(first + i*step)
		key := binary.BigEndian.AppendUint64([]byte("account/"), id)
		value := make([]byte, 64)
		binary.BigEndian.PutUint64(value, id)
		entries[i] = engine.Entry{Key: key, Value: value}
	}
	return entries
}

func TestStorageScaleOverlap(t *testing.T) {
	round := storageScaleRound(t)
	opts := engine.Options{IndexMode: engine.IndexMemory, MaxRecordBytes: 256, ValueCacheBudgetBytes: 64 << 10}
	seed := filepath.Join(t.TempDir(), "seed")
	db, err := engine.OpenWithHooks(seed, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for first := 0; first < overlapCapturedKeys; first += overlapBatchSize {
		if err := db.BatchPut(overlapEntries(first, 1, overlapBatchSize)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	arms := []string{"disjoint-control", "disjoint-compact", "overlapping-compact", "overlapping-control"}
	for i := range arms {
		arm := arms[(i+round-1)%len(arms)]
		t.Run(arm, func(t *testing.T) {
			measureStorageScaleOverlap(t, round, arm, seed, opts)
		})
	}
}

func measureStorageScaleOverlap(t *testing.T, round int, arm, seed string, opts engine.Options) {
	dir := filepath.Join(t.TempDir(), "db")
	measurementCloneClosed(t, seed, dir)
	compact := arm == "disjoint-compact" || arm == "overlapping-compact"
	overlap := arm == "overlapping-control" || arm == "overlapping-compact"

	copyStarted := make(chan struct{})
	var copying bool // Only the Compact goroutine emits this event.
	fs := &storage.IO{Hook: func(point string) error {
		if point == "compact.record:after" && !copying {
			copying = true
			close(copyStarted)
		}
		return nil
	}}
	db, err := engine.OpenWithHooks(dir, opts, fs, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	type compactResult struct {
		start, end time.Time
		err        error
	}
	done := make(chan compactResult, 1)
	before := scaleprobe.RelocationConflicts.Load()
	disjoint := overlapEntries(overlapCapturedKeys, 1, overlapBatchSize)
	writes := disjoint
	if overlap {
		// Every checkpoint batch of consecutive physical records contains
		// several of these keys. The foreground always stages the full set.
		writes = overlapEntries(0, overlapCapturedKeys/overlapBatchSize, overlapBatchSize)
	}
	if compact {
		go func() {
			start := time.Now()
			_, err := db.Compact(ctx)
			done <- compactResult{start, time.Now(), err}
		}()
		select {
		case <-copyStarted:
		case result := <-done:
			t.Fatalf("Compact ended before copying any record: %v", result.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// All four arms install the same tail before timing the writer. These
	// keys are absent from the captured prefix, making disjoint moves exact.
	if err := db.BatchPut(disjoint); err != nil {
		t.Fatal(err)
	}

	var samples []int64
	start := time.Now()
	deadline := start.Add(overlapWindow)
	var began []time.Time
	for time.Now().Before(deadline) {
		version := uint64(len(samples) + 1)
		for i := range writes {
			binary.BigEndian.PutUint64(writes[i].Value[8:], version)
		}
		call := time.Now()
		if err := db.BatchPut(writes); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(call).Nanoseconds())
		began = append(began, call)
	}
	wall := time.Since(start)
	if len(samples) == 0 {
		t.Fatal("writer made no progress")
	}
	var compactElapsed time.Duration
	var concurrent int
	if compact {
		result := <-done
		if result.err != nil {
			t.Fatal(result.err)
		}
		compactElapsed = result.end.Sub(result.start)
		for _, call := range began {
			if call.Before(result.end) {
				concurrent++
			}
		}
		if concurrent == 0 {
			t.Fatal("no timed batch overlapped Compact")
		}
	}
	conflicts := scaleprobe.RelocationConflicts.Load() - before
	if compact && overlap && conflicts == 0 {
		t.Fatal("no checkpoint relocation encountered staged writer keys")
	}
	if !overlap && conflicts != 0 {
		t.Fatalf("disjoint relocations took %d writer-lock fallbacks", conflicts)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	verifyOverlapDatabase(t, dir, opts, overlap, len(samples))
	lat := latency(samples, int64(len(samples)), wall)
	metrics := map[string]any{
		"captured_keys": overlapCapturedKeys, "entries_per_batch": overlapBatchSize,
		"window_ns": int64(overlapWindow), "writer_wall_ns": wall.Nanoseconds(),
		"writer_batches": len(samples), "batches_started_during_compact": concurrent,
		"entries_per_second": float64(len(samples)*overlapBatchSize) / wall.Seconds(),
		"batch_p50_ns":       lat.P50NS, "batch_p95_ns": lat.P95NS, "batch_p99_ns": lat.P99NS,
		"compact_ns": compactElapsed.Nanoseconds(), "relocation_fallbacks": conflicts,
	}
	if concurrent > 0 {
		metrics["overlapping_batch_p99_ns"] = latency(samples[:concurrent], int64(concurrent), 0).P99NS
	}
	storageScaleRow(t, round, "overlap", arm, metrics)
}

func verifyOverlapDatabase(t *testing.T, dir string, opts engine.Options, overlap bool, batches int) {
	t.Helper()
	db, err := engine.OpenWithHooks(dir, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	wantSequence := uint64(overlapCapturedKeys + (1+batches)*overlapBatchSize)
	if seq := engine.Observe(db).Frontier().Sequence; seq != wantSequence {
		t.Fatalf("recovered sequence %d, expected %d", seq, wantSequence)
	}
	seen := make([]bool, overlapCapturedKeys+overlapBatchSize)
	count := 0
	for entry, err := range db.Scan(t.Context()) {
		if err != nil {
			t.Fatal(err)
		}
		if len(entry.Key) != 16 || string(entry.Key[:8]) != "account/" {
			t.Fatalf("unexpected recovered key %x", entry.Key)
		}
		id := binary.BigEndian.Uint64(entry.Key[8:])
		if id >= uint64(len(seen)) || seen[id] {
			t.Fatalf("unexpected or duplicate recovered ID %d", id)
		}
		seen[id] = true
		count++
		var want [64]byte
		binary.BigEndian.PutUint64(want[:], id)
		updated := id >= overlapCapturedKeys
		if overlap {
			updated = id < overlapCapturedKeys && id%(overlapCapturedKeys/overlapBatchSize) == 0
		}
		if updated {
			binary.BigEndian.PutUint64(want[8:], uint64(batches))
		}
		if !bytes.Equal(entry.Value, want[:]) {
			t.Fatalf("wrong recovered value for key %d", id)
		}
	}
	if count != len(seen) {
		t.Fatalf("recovered %d keys, expected %d", count, len(seen))
	}
}
