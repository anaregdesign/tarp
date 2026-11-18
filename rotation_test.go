package tarp

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestSoftRotationBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		secondBytes int
		rotate      bool
	}{{"exact fit", 0, false}, {"one byte over", 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t, Options{MaxDataFileBytes: 512, MaxRecordBytes: 100, TargetSegmentBytes: 200}, nil)
			db.writer.Lock()
			startID, startGeneration := db.store.Current.ActiveID, db.store.Current.Generation
			err := db.store.Rotate() // A header-only active log must stay selected.
			noRotation := db.store.Current.ActiveID == startID && db.store.Current.Generation == startGeneration
			db.writer.Unlock()
			if err != nil || !noRotation {
				t.Fatalf("empty rotation: %v", err)
			}
			// 64-byte header, 87-byte first record, 49- or 50-byte second record.
			mustPut(t, db, "a", strings.Repeat("x", 38))
			mustPut(t, db, "b", strings.Repeat("y", tc.secondBytes))
			db.writer.Lock()
			end := db.store.Current
			db.writer.Unlock()
			if (end.ActiveID != startID) != tc.rotate || end.Sequence != 2 {
				t.Fatalf("active %d -> %d, sequence %d", startID, end.ActiveID, end.Sequence)
			}
		})
	}
}

func TestSoftTargetDoesNotLimitExistingFiles(t *testing.T) {
	db := newTestDB(t, Options{TargetSegmentBytes: 5 << 20}, nil)
	value := strings.Repeat("x", 512<<10)
	for i := range 18 {
		mustPut(t, db, fmt.Sprint(i), value)
	}
	dir := db.store.Dir
	db.writer.Lock()
	files := db.store.Catalog.Files
	current := db.store.Current
	db.writer.Unlock()
	if len(files) != 1 || files[0].Bytes <= 4<<20 || current.ActiveEnd <= 4<<20 {
		t.Fatalf("fixture: %v, active %d", files, current.ActiveEnd)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{MaxDataFileBytes: 4 << 20}); !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatalf("hard bound: %v", err)
	}
	target := filepath.Join(t.TempDir(), "restored")
	if err := Restore(context.Background(), backup, target, Options{}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, target} {
		image, err := Open(path, Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { image.Close() })
		image.writer.Lock()
		before := image.store.Current
		image.writer.Unlock()
		if before != current {
			t.Fatal("changing target rewrote the original selection")
		}
		mustPut(t, image, "next", "one")
		image.writer.Lock()
		after := image.store.Current
		image.writer.Unlock()
		if after.ActiveID == before.ActiveID || after.Sequence != before.Sequence+1 {
			t.Fatalf("append failed to rotate oversized active log: %#v", after)
		}
	}
}
