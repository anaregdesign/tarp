package benchmark

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/scaleprobe"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

type cleanerSpaceSample struct {
	RetiredDataBytes uint64 `json:"retired_data_bytes"`
	WindowNS         int64  `json:"window_ns"`
	BatchPuts        int    `json:"batch_puts"`
	Rotations        int64  `json:"rotations"`
	DataBytes        int64  `json:"data_file_bytes"`
	DataFiles        int    `json:"data_files"`
	LiveBytes        uint64 `json:"live_record_bytes"`
	SelectedBytes    uint64 `json:"selected_data_bytes"`
	ObsoleteBytes    uint64 `json:"obsolete_put_bytes"`
	UnlinkedBytes    uint64 `json:"worker_unlinked_bytes"`
}

// A best-effort on-disk census. The worker can retire files during ReadDir.
func sampleCleanerSpace(t *testing.T, db *engine.DB, puts int, rotations int64, start time.Time) cleanerSpaceSample {
	t.Helper()
	stats := db.Stats()
	row := cleanerSpaceSample{RetiredDataBytes: scaleprobe.RetiredDataBytes.Load(), WindowNS: time.Since(start).Nanoseconds(), BatchPuts: puts, Rotations: rotations, LiveBytes: stats.LiveRecordBytes,
		SelectedBytes: stats.SelectedDataBytes, ObsoleteBytes: stats.ObsoletePutBytes, UnlinkedBytes: db.AutoCompactionStatus().UnlinkedBytes}
	entries, err := os.ReadDir(engine.Observe(db).Dir())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "DATA-") {
			continue
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		row.DataBytes += info.Size()
		row.DataFiles++
	}
	return row
}

func TestStorageScaleCleaner(t *testing.T) {
	round := storageScaleRound(t)
	gaps := [...]time.Duration{0, time.Millisecond}
	for i := range gaps {
		gap := gaps[(round-1+i)%len(gaps)]
		measureCleanerChurn(t, round, gap)
	}
}

func measureCleanerChurn(t *testing.T, round int, gap time.Duration) {
	t.Helper()
	const keyCount, batchSize, steps = 256, 16, 512
	keys, value, opts := measurementInput(256, keyCount)
	opts.TargetSegmentBytes = 16 << 10
	dir := filepath.Join(t.TempDir(), "db")
	var rotations atomic.Int64
	fs := &storage.IO{Hook: func(point string) error {
		if point == "rotation.close:after" {
			rotations.Add(1)
		}
		return nil
	}}
	admit := make(chan struct{})
	db, err := engine.OpenWithHooks(dir, opts, fs, &engine.Hooks{CleanerStart: admit})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	expected := make([][]byte, keyCount)
	batch := make([]engine.Entry, batchSize)
	for i := 0; i < keyCount; i += batchSize {
		for j := range batch {
			batch[j] = engine.Entry{Key: keys[i+j], Value: value}
			expected[i+j] = value
		}
		if err := db.BatchPut(batch); err != nil {
			t.Fatal(err)
		}
	}
	startRotations := rotations.Load()
	scaleprobe.IdleSequence.Store(0)
	scaleprobe.RetiredDataBytes.Store(0)
	close(admit)
	start := time.Now()
	timeline := []cleanerSpaceSample{sampleCleanerSpace(t, db, 0, 0, start)}
	var puts, deletes []int64
	var lastWrite time.Time
	for step := range steps {
		base := step * batchSize % keyCount
		v := bytes.Clone(value)
		binary.LittleEndian.PutUint64(v, uint64(step+1))
		for j := range batch {
			batch[j] = engine.Entry{Key: keys[base+j], Value: v}
			expected[base+j] = v
		}
		if step%8 == 0 {
			call := time.Now()
			if err := db.BatchDelete(keys[base : base+batchSize]); err != nil {
				t.Fatal(err)
			}
			deletes = append(deletes, time.Since(call).Nanoseconds())
		}
		call := time.Now()
		if err := db.BatchPut(batch); err != nil {
			t.Fatal(err)
		}
		lastWrite = time.Now()
		puts = append(puts, lastWrite.Sub(call).Nanoseconds())
		if (step+1)%32 == 0 {
			timeline = append(timeline, sampleCleanerSpace(t, db, step+1, rotations.Load()-startRotations, start))
		}
		if gap > 0 && step+1 < steps {
			time.Sleep(gap)
		}
	}
	writeWindow := lastWrite.Sub(start)
	// The last overwrites invalidate sealed records and wake the worker. Wait
	// for the worker to observe this exact frontier with no reclaimable work.
	finalSequence := engine.Observe(db).Frontier().Sequence
	for scaleprobe.IdleSequence.Load() != finalSequence {
		if status := db.AutoCompactionStatus(); status.LastError != nil {
			t.Fatal(status.LastError)
		}
		if time.Since(lastWrite) > time.Minute {
			t.Fatalf("cleaner did not drain at sequence %d: %+v", finalSequence, db.AutoCompactionStatus())
		}
		time.Sleep(time.Millisecond)
	}
	drainNS := max(int64(0), scaleprobe.IdleAtNS.Load()-lastWrite.Sub(scaleprobe.ClockStart).Nanoseconds())
	final := sampleCleanerSpace(t, db, steps, rotations.Load()-startRotations, start)
	if final.Rotations < 32 || final.RetiredDataBytes == 0 || final.LiveBytes == 0 || db.Stats().LiveKeys != keyCount {
		t.Fatalf("insufficient sustained churn or wrong live set: %+v", final)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := engine.OpenWithHooks(dir, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if reopened.Stats().LiveKeys != keyCount {
		t.Fatal("wrong recovered cardinality")
	}
	for i, k := range keys {
		got, err := reopened.Get(k)
		if err != nil || !bytes.Equal(got, expected[i]) {
			t.Fatalf("recovered key %d: %v", i, err)
		}
	}
	storageScaleRow(t, round, "cleaner", fmt.Sprintf("gap-%s", gap), map[string]any{
		"keys": keyCount, "target_segment_bytes": opts.TargetSegmentBytes, "batch_size": batchSize, "write_window_ns": writeWindow.Nanoseconds(),
		"put": latency(puts, int64(len(puts)), writeWindow), "delete": latency(deletes, int64(len(deletes)), writeWindow),
		"entries_per_second": float64(batchSize*(len(puts)+len(deletes))) / writeWindow.Seconds(),
		"timeline":           timeline, "drained": final, "drain_observed_ns": drainNS, "final_sequence": finalSequence,
	})
}
