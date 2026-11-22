package benchmark

import (
	"context"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
	"github.com/anaregdesign/tarp/internal/testfixture"
)

// The selected layout describes the bytes replay must validate at a single
// committed frontier. The registry can additionally contain retired files or
// staging. Neither count is a filesystem block allocation measurement.
type measurementLayout struct {
	LastUserSequence          uint64 `json:"last_user_sequence"`
	CheckpointCutoffSequence  uint64 `json:"checkpoint_cutoff_sequence"`
	PrunedThroughSequence     uint64 `json:"pruned_through_sequence"`
	SealedRawLogSegments      int    `json:"sealed_raw_log_segments"`
	SnapshotSegments          int    `json:"snapshot_segments"`
	CleanedLogSegments        int    `json:"cleaned_log_segments"`
	EmptyCleanedIntervals     int    `json:"empty_cleaned_intervals"`
	SelectedDataSegments      int    `json:"selected_data_segments"`
	SelectedDataBytes         int64  `json:"selected_data_bytes"`
	SelectedRecordBytes       int64  `json:"selected_record_bytes"`
	LiveRecordBytes           int64  `json:"live_record_bytes"`
	RetainedDeleteBytes       int64  `json:"retained_delete_bytes"`
	ObsoletePutBytes          int64  `json:"obsolete_put_bytes"`
	SelectedMetadataBytes     int64  `json:"selected_metadata_bytes"`
	RegisteredDataBytes       int64  `json:"registered_data_bytes"`
	UnselectedPinnedDataBytes int64  `json:"unselected_pinned_data_bytes"`
}

func observedLayout(db *engine.DB) measurementLayout {
	var result measurementLayout
	probe := engine.Observe(db)
	probe.Store(func(s *storage.Store) {
		result = measureLayout(s, int64(db.Stats().LiveRecordBytes))
	})
	return result
}

func measureLayout(s *storage.Store, liveBytes int64) measurementLayout {
	layout := measurementLayout{
		LastUserSequence:         s.Current.LastUserSequence,
		CheckpointCutoffSequence: s.Catalog.CheckpointCutoffSequence,
		PrunedThroughSequence:    s.Catalog.PrunedThroughSequence,
		SelectedDataSegments:     1, SelectedDataBytes: s.Current.ActiveCommittedEndOffset,
		SelectedMetadataBytes: int64(len(s.Manifest)) + format.FrontierBytes,
		RetainedDeleteBytes:   s.Segments[s.Current.ActiveID].DeleteBytes.Load(),
	}
	selected := map[uint64]bool{s.Current.ActiveID: true}
	for _, e := range s.Catalog.Entries {
		if e.ID == 0 {
			layout.EmptyCleanedIntervals++
			continue
		}
		switch e.Kind {
		case format.RawLogKind:
			layout.SealedRawLogSegments++
		case format.SnapshotKind:
			layout.SnapshotSegments++
		case format.CleanedLogKind:
			layout.CleanedLogSegments++
		}
		selected[e.ID] = true
		layout.SelectedDataSegments++
		layout.SelectedDataBytes += e.Bytes
		layout.RetainedDeleteBytes += s.Segments[e.ID].DeleteBytes.Load()
	}
	layout.SelectedRecordBytes = layout.SelectedDataBytes - int64(layout.SelectedDataSegments)*format.SegmentHeaderBytes
	layout.LiveRecordBytes = liveBytes
	layout.ObsoletePutBytes = layout.SelectedRecordBytes - layout.LiveRecordBytes - layout.RetainedDeleteBytes
	for id, segment := range s.Segments {
		size := segment.Size.Load()
		layout.RegisteredDataBytes += size
		if !selected[id] && segment.References() > 0 {
			layout.UnselectedPinnedDataBytes += size
		}
	}
	return layout
}

// Resource collection already sampled the parked layout. Record the census
// once on startup rows without scanning the rebuilt index a second time.
func startupRow(elapsed time.Duration, resources map[string]any) map[string]any {
	layout := resources["layout"]
	delete(resources, "layout")
	return map[string]any{
		"elapsed_ns": elapsed.Nanoseconds(), "replay_layout": layout, "resources": resources,
	}
}

// Park both maintenance workers until Close so the external cold-device
// counters and post-open layout cover only recovery.
func openStartupSample(t *testing.T, dir string, opts engine.Options) (*engine.DB, time.Duration) {
	t.Helper()
	parked := make(chan struct{})
	engineHooks := &engine.Hooks{CleanerStart: parked, ParityStart: parked}
	start := time.Now()
	db, err := engine.OpenWithHooks(dir, opts, nil, engineHooks)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, elapsed
}

// Seal the same default-worker history into a full DATA checkpoint before cloning
// it for the second recovery comparison. The cleaner cannot alter that input.
func measurementCheckpointFixture(t *testing.T, dir string, opts engine.Options) {
	t.Helper()
	db, err := engine.OpenWithHooks(dir, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupSampleDefersParityIOUntilClose(t *testing.T) {
	dir := testfixture.WriteMixedStore(t, 8)
	parityFiles := func(t *testing.T) []string {
		t.Helper()
		files, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	synctest.Test(t, func(t *testing.T) {
		db, _ := openStartupSample(t, dir, engine.Options{IndexMode: engine.IndexMemory})
		synctest.Wait()
		if files := parityFiles(t); len(files) != 0 {
			t.Errorf("startup sample created %d parity files", len(files))
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if files := parityFiles(t); len(files) == 0 {
			t.Error("Close did not protect the sealed files")
		}
	})
}

func TestMeasurementLayoutAccountsForCanonicalRecords(t *testing.T) {
	dir := testfixture.WriteMixedStore(t, 8)
	db, _ := openStartupSample(t, dir, engine.Options{})
	layout := observedLayout(db)
	// The fixture has one segment of each sealed kind and one active log segment.
	// The empty interval covers user sequences 3 and 4 with zero DATA segments.
	if layout.LastUserSequence != 9 || layout.CheckpointCutoffSequence != 2 || layout.PrunedThroughSequence != 8 ||
		layout.SealedRawLogSegments != 1 || layout.CleanedLogSegments != 1 || layout.SnapshotSegments != 1 ||
		layout.EmptyCleanedIntervals != 1 || layout.SelectedDataSegments != 4 {
		t.Fatalf("incorrect replay layout: %+v", layout)
	}
	bytesOf := func(key, value string) int64 {
		return int64(format.RecordHeaderBytes + len(key) + len(value))
	}
	wantedLive := bytesOf("checkpoint", "kept") + bytesOf("empty", "") + bytesOf("tail", "new")
	wantedDeletes := bytesOf("pruned-put", "") + bytesOf("pruned-snapshot-key", "")
	if layout.LiveRecordBytes != wantedLive || layout.RetainedDeleteBytes != wantedDeletes ||
		layout.ObsoletePutBytes != 0 || layout.SelectedRecordBytes != wantedLive+wantedDeletes ||
		layout.SelectedDataBytes != 4*format.SegmentHeaderBytes+layout.SelectedRecordBytes ||
		layout.RegisteredDataBytes != layout.SelectedDataBytes || layout.SelectedMetadataBytes == 0 {
		t.Fatalf("incorrect byte inventory: %+v", layout)
	}
	mustPut(t, db, "tail", "newer")
	if got := observedLayout(db); got.ObsoletePutBytes != bytesOf("tail", "new") || got.RetainedDeleteBytes != wantedDeletes {
		t.Fatalf("incorrect obsolete bytes after overwrite: %+v", got)
	}

	var capture *storage.Capture
	engine.Observe(db).Store(func(s *storage.Store) {
		var err error
		capture, err = s.Capture()
		if err != nil {
			t.Fatal(err)
		}
	})
	defer capture.Release()
	carried := capture.Catalog.Entries[0]
	var wantedPinned int64
	for _, src := range capture.Segments {
		if src.Entry.ID != carried.ID {
			wantedPinned += src.Entry.Bytes
		}
	}
	result, err := db.Compact(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	engine.Observe(db).Store(func(s *storage.Store) {
		if len(s.Catalog.Entries) != 2 || s.Catalog.Entries[0] != carried {
			t.Fatalf("checkpoint did not retain the original snapshot: %+v", s.Catalog.Entries)
		}
	})
	checkpoint := observedLayout(db)
	wantedLive += bytesOf("tail", "newer") - bytesOf("tail", "new")
	if checkpoint.LastUserSequence != 10 || checkpoint.CheckpointCutoffSequence != 10 || checkpoint.PrunedThroughSequence != 10 ||
		checkpoint.SelectedDataSegments != 3 || checkpoint.SnapshotSegments != 2 || checkpoint.EmptyCleanedIntervals != 0 ||
		checkpoint.RetainedDeleteBytes != 0 || checkpoint.ObsoletePutBytes != 0 ||
		checkpoint.SelectedRecordBytes != wantedLive || checkpoint.LiveRecordBytes != wantedLive ||
		checkpoint.SelectedDataBytes != 3*format.SegmentHeaderBytes+wantedLive ||
		checkpoint.UnselectedPinnedDataBytes != wantedPinned || result.PinnedRetiredDataBytes != wantedPinned ||
		checkpoint.RegisteredDataBytes != checkpoint.SelectedDataBytes+checkpoint.UnselectedPinnedDataBytes {
		t.Fatalf("incorrect post-checkpoint inventory: %+v (Compact %+v)", checkpoint, result)
	}
}
