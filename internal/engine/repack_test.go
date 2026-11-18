package engine

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Wait for physical retirement as well as the catalog replacement. Once the
// old sources are gone, recovery can succeed only from the packed layout.
func waitForDataFiles(t *testing.T, db *DB, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		files, err := filepath.Glob(filepath.Join(db.store.Dir, "DATA-*"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("found %d DATA files, want %d; worker status %+v", len(files), want, db.MaintenanceStatus())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAutomaticRepackAfterSingleFileCleaning(t *testing.T) {
	opts := Options{MaxRecordBytes: 4096, TargetSegmentBytes: 8192}
	parked := &storage.IO{CleanerStart: make(chan struct{})}
	db := newTestDB(t, opts, parked)
	dir := db.store.Dir

	// Five sealed logs each lose their large hot value. Four retain a
	// small value, while the middle one becomes a metadata-only interval.
	var lastSealed uint64
	large := strings.Repeat("x", 3000)
	for _, key := range []string{"a", "b", "", "c", "d"} {
		mustPut(t, db, "hot", large)
		if key != "" {
			mustPut(t, db, key, key)
		}
		switch key {
		case "a":
			mustPut(t, db, "deleted", "old")
		case "c":
			if err := db.Delete([]byte("deleted")); err != nil {
				t.Fatal(err)
			}
		}
		lastSealed = sealForCleaning(t, db).Last
	}
	mustPut(t, db, "hot", "tail")
	active := db.store.Current.ActiveID
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	live, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	waitForDataFiles(t, live, 2) // one packed log plus the original active log

	backup := filepath.Join(t.TempDir(), "backup")
	if err := live.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := live.Close(); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(t.Context(), backup, restored, opts); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, restored} {
		image, err := OpenWithIO(path, opts, parked)
		if err != nil {
			t.Fatal(err)
		}
		defer image.Close()
		files := image.store.Catalog.Files
		before := image.store.Current
		if len(files) != 1 || files[0].Kind != format.Cleaned || files[0].First != 1 || files[0].Last != lastSealed ||
			before.Sequence != lastSealed+1 || before.ActiveID != active || image.store.Catalog.Cutoff != 0 {
			t.Fatalf("unexpected repacked layout: %+v, frontier %+v", image.store.Catalog, before)
		}
		if image.store.Files[files[0].ID].DeleteBytes.Load() == 0 {
			t.Fatal("repacked interval dropped its tombstone")
		}
		for _, key := range []string{"a", "b", "c", "d"} {
			wantValue(t, image, key, key)
		}
		wantValue(t, image, "hot", "tail")
		if _, err := image.Get([]byte("deleted")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted key after repacking: %v", err)
		}
		if again, retry := image.cleanPass(t.Context()); again || retry || image.store.Current != before {
			t.Fatal("idle cleaner rewrote an already packed interval")
		}
	}
}

func TestAutomaticRepackKeepsSnapshotsAndLogIntervalsSeparate(t *testing.T) {
	small := Options{MaxRecordBytes: 128, TargetSegmentBytes: 192}
	db := newTestDB(t, small, &storage.IO{CleanerStart: make(chan struct{})})
	value := strings.Repeat("v", 48) // fits one record per 192-byte snapshot
	for _, key := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, key, value)
	}
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "e", "sealed tail")
	tail := sealForCleaning(t, db)
	mustPut(t, db, "f", "active tail")
	before := db.store.Current
	dir := db.store.Dir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The same snapshot files now fit into one segment without a full checkpoint.
	larger := Options{MaxRecordBytes: 128, TargetSegmentBytes: 4096}
	live, err := Open(dir, larger)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	waitForDataFiles(t, live, 3)
	live.writer.Lock()
	files := slices.Clone(live.store.Catalog.Files)
	cutoff, after := live.store.Catalog.Cutoff, live.store.Current
	live.writer.Unlock()
	if len(files) != 2 || files[0].Kind != format.Snapshot || files[0].First != 0 || files[0].Last != 0 ||
		files[1] != tail || cutoff != 4 || after.ActiveID != before.ActiveID || after.Sequence != before.Sequence {
		t.Fatalf("snapshot repack changed the logical boundaries: cutoff %d, files %+v, frontier %+v", cutoff, files, after)
	}
	for _, key := range []string{"a", "b", "c", "d"} {
		wantValue(t, live, key, value)
	}
	wantValue(t, live, "e", "sealed tail")
	wantValue(t, live, "f", "active tail")
}
