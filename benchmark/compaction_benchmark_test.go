package benchmark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

const (
	automaticIncremental = "automatic-incremental"
	manualFullCheckpoint = "manual-full-checkpoint"
)

var compactionModes = [...]string{automaticIncremental, manualFullCheckpoint}

type compactionFixture struct {
	dir       string
	opts      engine.Options
	source    format.File
	frontier  format.Frontier
	stateHash string
	liveKeys  int
}

// Use equal-width keys and a soft target that fits 32 full records at every
// value size. The first sealed log retains one live Put and one Delete.
// The active log holds the final hot value and an independent live key.
func prepareCompactionFixture(t testing.TB, size int) compactionFixture {
	t.Helper()
	keys, value, opts := measurementInput(size, 4)
	opts.MaxRecordBytes = int64(format.RecordHeaderBytes + len(keys[0]) + size)
	opts.TargetSegmentBytes = format.SegmentHeaderBytes + 32*opts.MaxRecordBytes
	db, err := engine.OpenWithIO(filepath.Join(t.TempDir(), "seed"), opts, &storage.IO{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	put := func(key []byte) {
		t.Helper()
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	put(keys[0])
	put(keys[1])
	if err := db.Delete(keys[1]); err != nil {
		t.Fatal(err)
	}
	for i := range 32 {
		value[0] = byte(i)
		put(keys[2])
	}
	put(keys[3])

	probe := engine.Observe(db)
	var source format.File
	probe.Store(func(s *storage.Store) {
		if len(s.Catalog.Files) != 1 {
			t.Fatalf("fixture must contain one sealed log; got %d", len(s.Catalog.Files))
		}
		source = s.Catalog.Files[0]
		if source.ID == 0 || source.Kind != format.Log ||
			s.Files[source.ID].LiveBytes.Load() != opts.MaxRecordBytes ||
			s.Files[source.ID].DeleteBytes.Load() != int64(format.RecordHeaderBytes+len(keys[1])) {
			t.Fatal("fixture must contain a sealed log with a surviving Put and a tombstone")
		}
	})
	frontier := probe.Frontier()
	hash, keysLeft := measurementState(t, db)
	if keysLeft != 3 || frontier.Sequence != 36 {
		t.Fatal("incorrect compaction fixture contents or frontier")
	}
	return compactionFixture{
		dir: probe.Dir(), opts: probe.Options(), source: source,
		frontier: frontier, stateHash: hash, liveKeys: keysLeft,
	}
}

type compactionMeasurement struct {
	Mode               string         `json:"mode"`
	ElapsedNS          int64          `json:"elapsed_ns"`
	Options            engine.Options `json:"options"`
	SealedSourceBytes  int64          `json:"sealed_source_bytes"`
	UnlinkedDataBytes  int64          `json:"unlinked_data_bytes"`
	PublishedDataBytes int64          `json:"published_data_bytes"`
	NetDataBytesFreed  int64          `json:"net_data_bytes_freed"`
	Sequence           uint64         `json:"committed_sequence"`
	Cutoff             uint64         `json:"checkpoint_cutoff"`
	PrunedThrough      uint64         `json:"pruned_through"`
	CleanedIntervals   int            `json:"cleaned_intervals"`
	SnapshotFiles      int            `json:"snapshot_files"`
	ReopenedStateHash  string         `json:"reopened_state_sha256"`
	LiveKeys           int            `json:"live_keys"`
}

// The two arms run on private copies of one closed store. Only worker admission
// through the post-publication retirement directory sync, or the public Compact
// call, belongs to the timing window. Directory I/O for verification is untimed.
func (f compactionFixture) measure(t testing.TB, mode string) compactionMeasurement {
	t.Helper()
	temp, err := os.MkdirTemp(filepath.Dir(f.dir), "sample-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(temp); err != nil {
			t.Error(err)
		}
	}()
	dir := filepath.Join(temp, "db")
	measurementCloneClosed(t, f.dir, dir)
	start, retired := make(chan struct{}), make(chan struct{})
	published := false
	hooks := &storage.IO{CleanerStart: start, Hook: func(point string) error {
		// Both actor-specific events execute on the same worker, in order.
		switch point {
		case "clean.current.dirsync:after":
			published = true
		case "clean.retire.dirsync:after":
			if published {
				close(retired)
				published = false
			}
		}
		return nil
	}}
	db, err := engine.OpenWithIO(dir, f.opts, hooks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if engine.Observe(db).Frontier() != f.frontier {
		t.Fatal("private clone started at a different frontier")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	b, isBenchmark := t.(*testing.B)
	if isBenchmark {
		b.StartTimer()
	}
	now := time.Now()
	switch mode {
	case automaticIncremental:
		close(start)
		select {
		case <-retired:
		case <-engine.Observe(db).CleanerDone():
			err = fmt.Errorf("cleaner stopped before retirement: %+v", db.MaintenanceStatus())
		case <-ctx.Done():
			err = fmt.Errorf("waiting for cleaner retirement: %w (%+v)", ctx.Err(), db.MaintenanceStatus())
		}
	case manualFullCheckpoint:
		_, err = db.Compact(ctx) // keep automatic processing parked on this clone
	default:
		err = fmt.Errorf("unknown compaction mode %q", mode)
	}
	elapsed := time.Since(now)
	if isBenchmark {
		b.StopTimer()
	}
	if err != nil {
		t.Fatal(err)
	}

	result := f.verify(t, db, mode)
	result.ElapsedNS = elapsed.Nanoseconds()
	return result
}

// Join the worker before inspecting final metadata. Check actual source
// removal before reopen gets an opportunity to clean up orphaned files.
func (f compactionFixture) verify(t testing.TB, db *engine.DB, mode string) compactionMeasurement {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	probe := engine.Observe(db)
	dir := probe.Dir()
	result := compactionMeasurement{
		Mode: mode, Options: f.opts, SealedSourceBytes: f.source.Bytes,
	}
	original := []format.File{f.source, {ID: f.frontier.ActiveID, Bytes: f.frontier.ActiveEnd}}
	current := probe.Frontier()
	selected := map[uint64]int64{current.ActiveID: current.ActiveEnd}
	probe.Store(func(s *storage.Store) {
		result.Sequence = current.Sequence
		result.Cutoff, result.PrunedThrough = s.Catalog.Cutoff, s.Catalog.PrunedThrough
		for _, file := range s.Catalog.Files {
			switch file.Kind {
			case format.Cleaned:
				result.CleanedIntervals++
				if file.ID == 0 || file.First != f.source.First || file.Last != f.source.Last {
					t.Fatal("incremental replacement lost the retained records or their original interval")
				}
			case format.Snapshot:
				result.SnapshotFiles++
			}
			if file.ID != 0 {
				selected[file.ID] = file.Bytes
			}
		}
	})
	if result.Sequence != f.frontier.Sequence || result.PrunedThrough != result.Sequence {
		t.Fatalf("post-maintenance frontier: %+v", result)
	}
	switch mode {
	case automaticIncremental:
		if result.CleanedIntervals != 1 || result.SnapshotFiles != 0 || result.Cutoff != 0 || current.ActiveID != f.frontier.ActiveID {
			t.Fatalf("unexpected incremental layout: %+v", result)
		}
	case manualFullCheckpoint:
		if result.SnapshotFiles == 0 || result.CleanedIntervals != 0 || result.Cutoff != f.frontier.Sequence {
			t.Fatalf("unexpected checkpoint layout: %+v", result)
		}
	}
	for _, file := range original {
		if _, kept := selected[file.ID]; kept {
			delete(selected, file.ID)
			continue
		}
		name := filepath.Join(dir, storage.DataName(current.UUID, file.ID))
		if _, err := os.Lstat(name); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired DATA file %s still present or unreadable: %v", name, err)
		}
		result.UnlinkedDataBytes += file.Bytes
	}
	for _, length := range selected {
		result.PublishedDataBytes += length
	}
	result.NetDataBytesFreed = result.UnlinkedDataBytes - result.PublishedDataBytes
	if result.NetDataBytesFreed <= 0 || result.PublishedDataBytes <= 0 {
		t.Fatalf("fixture did not reclaim and replace DATA: %+v", result)
	}

	reopened, err := engine.OpenWithIO(dir, f.opts, &storage.IO{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	result.ReopenedStateHash, result.LiveKeys = measurementState(t, reopened)
	if result.ReopenedStateHash != f.stateHash || result.LiveKeys != f.liveKeys || engine.Observe(reopened).Frontier().Sequence != f.frontier.Sequence {
		t.Fatalf("compaction changed reopened key state or sequence: %+v", result)
	}
	return result
}

func BenchmarkCompaction(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			fixture := prepareCompactionFixture(b, size)
			for _, mode := range compactionModes {
				b.Run(mode, func(b *testing.B) {
					b.StopTimer()
					b.ResetTimer()
					var sample compactionMeasurement
					for i := 0; i < b.N; i++ {
						sample = fixture.measure(b, mode)
					}
					// The fixture and resulting DATA lengths are identical for every iteration.
					b.ReportMetric(float64(sample.PublishedDataBytes), "published-DATA-B/op")
					b.ReportMetric(float64(sample.UnlinkedDataBytes), "unlinked-DATA-B/op")
					b.ReportMetric(float64(sample.NetDataBytesFreed), "net-DATA-B/op")
				})
			}
		})
	}
}

// The smallest fixture gives metadata the largest share of the cleaning cost.
// Verify its eligibility and byte attribution without running a timing profile.
func TestCompactionFixtureDataAccounting(t *testing.T) {
	fixture := prepareCompactionFixture(t, 64)
	for _, mode := range compactionModes {
		t.Run(mode, func(t *testing.T) {
			// Fixed 16-byte keys. The incremental replacement keeps one value
			// and one deletion. The checkpoint keeps three live values and
			// also creates a fresh active log header when sealing the tail.
			record := fixture.opts.MaxRecordBytes
			wantOutput := int64(format.SegmentHeaderBytes) + record + format.RecordHeaderBytes + 16
			wantUnlinked := fixture.source.Bytes
			if mode == manualFullCheckpoint {
				wantOutput = 2*format.SegmentHeaderBytes + 3*record
				wantUnlinked += fixture.frontier.ActiveEnd
			}
			result := fixture.measure(t, mode)
			if result.UnlinkedDataBytes != wantUnlinked || result.PublishedDataBytes != wantOutput ||
				result.NetDataBytesFreed != wantUnlinked-wantOutput {
				t.Fatalf("DATA accounting: got %+v, expected %d unlinked and %d published", result, wantUnlinked, wantOutput)
			}
		})
	}
}
