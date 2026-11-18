package benchmark

import (
	"context"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
	"github.com/anaregdesign/tarp/internal/testfixture"
)

// The selected layout describes the bytes replay must validate at a single
// committed frontier. The registry can additionally contain retired files or
// staging. Neither count is a filesystem block allocation measurement.
type measurementLayout struct {
	Sequence              uint64 `json:"sequence"`
	CheckpointCutoff      uint64 `json:"checkpoint_cutoff"`
	PrunedThrough         uint64 `json:"pruned_through"`
	RawSealedFiles        int    `json:"raw_sealed_files"`
	SnapshotFiles         int    `json:"snapshot_files"`
	CleanedFiles          int    `json:"cleaned_files"`
	EmptyCleanedIntervals int    `json:"empty_cleaned_intervals"`
	SelectedDataFiles     int    `json:"selected_data_files"`
	SelectedDataBytes     int64  `json:"selected_data_bytes"`
	SelectedRecordBytes   int64  `json:"selected_record_bytes"`
	LivePutBytes          int64  `json:"live_put_bytes"`
	RetainedDeleteBytes   int64  `json:"retained_delete_bytes"`
	ObsoletePutBytes      int64  `json:"obsolete_put_bytes"`
	SelectedMetadataBytes int64  `json:"selected_metadata_bytes"`
	RegisteredDataBytes   int64  `json:"registered_data_bytes"`
	UnselectedPinnedBytes int64  `json:"unselected_pinned_data_bytes"`
}

func observedLayout(db *engine.DB) measurementLayout {
	var result measurementLayout
	probe := engine.Observe(db)
	probe.Store(func(s *storage.Store) {
		probe.State(func(index map[string]storage.Location, _ *cache.Cache) {
			result = measureLayout(s, index)
		})
	})
	return result
}

func measureLayout(s *storage.Store, index map[string]storage.Location) measurementLayout {
	layout := measurementLayout{
		Sequence: s.Current.Sequence, CheckpointCutoff: s.Catalog.Cutoff, PrunedThrough: s.Catalog.PrunedThrough,
		SelectedDataFiles: 1, SelectedDataBytes: s.Current.ActiveEnd,
		SelectedMetadataBytes: int64(len(s.Manifest)) + format.FrontierBytes,
		RetainedDeleteBytes:   s.Files[s.Current.ActiveID].DeleteBytes.Load(),
	}
	selected := map[uint64]bool{s.Current.ActiveID: true}
	for _, e := range s.Catalog.Files {
		if e.ID == 0 {
			layout.EmptyCleanedIntervals++
			continue
		}
		switch e.Kind {
		case format.Log:
			layout.RawSealedFiles++
		case format.Snapshot:
			layout.SnapshotFiles++
		case format.Cleaned:
			layout.CleanedFiles++
		}
		selected[e.ID] = true
		layout.SelectedDataFiles++
		layout.SelectedDataBytes += e.Bytes
		layout.RetainedDeleteBytes += s.Files[e.ID].DeleteBytes.Load()
	}
	layout.SelectedRecordBytes = layout.SelectedDataBytes - int64(layout.SelectedDataFiles)*format.SegmentHeaderBytes
	for _, loc := range index {
		layout.LivePutBytes += loc.Bytes
	}
	layout.ObsoletePutBytes = layout.SelectedRecordBytes - layout.LivePutBytes - layout.RetainedDeleteBytes
	for id, file := range s.Files {
		size := file.Size.Load()
		layout.RegisteredDataBytes += size
		if !selected[id] && file.References() > 0 {
			layout.UnselectedPinnedBytes += size
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

// Recovery samples time the normal open path while parking the newly launched
// worker until Close. Post-open collection and the independent cold-device
// counters then describe exactly the image that recovery read.
func openStartupSample(t *testing.T, dir string, opts engine.Options) (*engine.DB, time.Duration) {
	t.Helper()
	fs := &storage.IO{CleanerStart: make(chan struct{})}
	start := time.Now()
	db, err := engine.OpenWithIO(dir, opts, fs)
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

// Seal the same default-worker history into a full checkpoint before cloning
// it for the second recovery comparison. The cleaner cannot alter that input.
func measurementCheckpointFixture(t *testing.T, dir string, opts engine.Options) {
	t.Helper()
	db, err := engine.OpenWithIO(dir, opts, &storage.IO{CleanerStart: make(chan struct{})})
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

func TestMeasurementLayoutAccountsForCanonicalRecords(t *testing.T) {
	dir := testfixture.WriteMixedStore(t, 8)
	db, _ := openStartupSample(t, dir, engine.Options{})
	layout := observedLayout(db)
	// The fixture has one physical file for each kind plus its active log.
	// The empty interval covers sequences 3 and 4 with no physical header.
	if layout.Sequence != 9 || layout.CheckpointCutoff != 2 || layout.PrunedThrough != 8 ||
		layout.RawSealedFiles != 1 || layout.CleanedFiles != 1 || layout.SnapshotFiles != 1 ||
		layout.EmptyCleanedIntervals != 1 || layout.SelectedDataFiles != 4 {
		t.Fatalf("incorrect replay layout: %+v", layout)
	}
	bytesOf := func(key, value string) int64 {
		return int64(format.RecordHeaderBytes + len(key) + len(value))
	}
	wantedLive := bytesOf("checkpoint", "kept") + bytesOf("empty", "") + bytesOf("tail", "new")
	wantedDeletes := bytesOf("pruned-put", "") + bytesOf("pruned-snapshot-key", "")
	if layout.LivePutBytes != wantedLive || layout.RetainedDeleteBytes != wantedDeletes ||
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
	result, err := db.Compact(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := observedLayout(db)
	if checkpoint.SelectedDataFiles != 2 || checkpoint.SnapshotFiles != 1 || checkpoint.EmptyCleanedIntervals != 0 ||
		checkpoint.RetainedDeleteBytes != 0 || checkpoint.ObsoletePutBytes != 0 ||
		checkpoint.UnselectedPinnedBytes == 0 || checkpoint.UnselectedPinnedBytes != result.PinnedBytes ||
		checkpoint.RegisteredDataBytes != checkpoint.SelectedDataBytes+checkpoint.UnselectedPinnedBytes {
		t.Fatalf("incorrect post-checkpoint inventory: %+v (Compact %+v)", checkpoint, result)
	}
}
