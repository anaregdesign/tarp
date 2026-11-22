package engine

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Independently replay the selected image. Retired and staged files may still
// occupy the directory while these gauges describe the committed DATA only.
func checkStatsAgainstReplay(t *testing.T, db *DB) {
	t.Helper()
	db.writer.Lock()
	defer db.writer.Unlock()
	image, err := storage.Validate(t.Context(), db.store.Dir)
	if err != nil {
		t.Fatal(err)
	}
	segments := uint64(len(image.Files))
	selected := uint64(image.Current.ActiveCommittedEndOffset)
	var live, tombstones uint64
	for _, entry := range image.Catalog.Entries {
		selected += uint64(entry.Bytes)
	}
	for _, file := range image.Files {
		tombstones += uint64(file.DeleteBytes.Load())
		live += uint64(file.LiveBytes.Load())
	}
	obsolete := selected - segments*format.SegmentHeaderBytes - live - tombstones
	got := db.Stats()
	if got.SelectedSegments != segments || got.SelectedDataBytes != selected || got.LiveRecordBytes != live ||
		got.TombstoneBytes != tombstones || got.ObsoletePutBytes != obsolete {
		t.Fatalf("stats %+v; replay selected %d bytes across %d segments, live %d, tombstones %d, obsolete %d",
			got, selected, segments, live, tombstones, obsolete)
	}
}

func TestDataStatsTrackCleanerOutputsAndIgnorePins(t *testing.T) {
	engineHooks := &Hooks{CleanerStart: make(chan struct{})}
	db := newTestDBWithHooks(t, Options{}, nil, engineHooks)
	checkStatsAgainstReplay(t, db)
	mustPut(t, db, "a", "first")
	mustPut(t, db, "b", "deleted")
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	source := sealForCleaning(t, db)
	mustPut(t, db, "dead", "old")
	empty := sealForCleaning(t, db)
	mustPut(t, db, "dead", "new")
	mustPut(t, db, "a", "second")
	checkStatsAgainstReplay(t, db)

	capture, err := db.store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []format.CatalogEntry{source, empty} {
		if err = db.cleanSegment(t.Context(), file); err != nil {
			capture.Release()
			t.Fatal(err)
		}
		checkStatsAgainstReplay(t, db)
	}
	if _, pinned, _, err := db.store.RetireCleaned(); err != nil || pinned != source.Bytes+empty.Bytes {
		capture.Release()
		t.Fatalf("retire pinned = %d, %v", pinned, err)
	}
	capture.Release()
	checkStatsAgainstReplay(t, db)
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	checkStatsAgainstReplay(t, db)
}

func TestDataStatsTrackTailDuringCheckpointAndReopen(t *testing.T) {
	copied, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	pause := sync.OnceFunc(func() {
		close(copied)
		<-resume
	})
	dir := filepath.Join(t.TempDir(), "db")
	fs := &storage.IO{Hook: func(p string) error {
		if p == "compact.record:after" {
			pause()
		}
		return nil
	}}
	engineHooks := &Hooks{CleanerStart: make(chan struct{})}
	opts := testOptions()
	db, err := OpenWithHooks(dir, opts, fs, engineHooks)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Keep the staging goroutine releasable if an assertion fails.
	defer unblock()
	mustPut(t, db, "a", "first")
	mustPut(t, db, "b", "first")
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := db.Compact(t.Context())
		done <- err
	}()
	<-copied
	checkStatsAgainstReplay(t, db)
	for _, key := range []string{"a", "c", "d", "e"} {
		mustPut(t, db, key, "tail")
	}
	if err := db.Delete([]byte("c")); err != nil {
		t.Fatal(err)
	}
	checkStatsAgainstReplay(t, db)
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	checkStatsAgainstReplay(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithHooks(dir, opts, nil, &Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	checkStatsAgainstReplay(t, reopened)
}
