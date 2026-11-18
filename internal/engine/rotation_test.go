package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestSoftRotationBoundary(t *testing.T) {
	for _, tc := range []struct {
		name        string
		secondBytes int
		rotate      bool
	}{{"exact fit", 0, false}, {"one byte over", 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t, Options{MaxRecordBytes: 100, TargetSegmentBytes: 200}, nil)
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
	smaller := Options{TargetSegmentBytes: 128, MaxRecordBytes: 64}
	db := newTestDB(t, Options{TargetSegmentBytes: 260, MaxRecordBytes: 64}, nil)
	value := strings.Repeat("x", 11)
	for i := range 6 {
		mustPut(t, db, fmt.Sprint(i), value)
	}
	dir := db.store.Dir
	db.writer.Lock()
	files := db.store.Catalog.Files
	current := db.store.Current
	db.writer.Unlock()
	if len(files) != 1 || files[0].Bytes <= smaller.TargetSegmentBytes || current.ActiveEnd <= smaller.TargetSegmentBytes {
		t.Fatalf("fixture: %v, active %d", files, current.ActiveEnd)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restored")
	if err := Restore(context.Background(), backup, target, smaller); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, target} {
		image, err := Open(path, smaller)
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
		for i := range 6 {
			wantValue(t, image, fmt.Sprint(i), value)
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

// Inject failures at each publication boundary. Once the CURRENT rename is
// attempted the user operation has an unknown outcome, even when our test hook
// tells us which image this process will see on reopen.
func TestRotatingAppendFaultOutcomes(t *testing.T) {
	for _, tc := range []struct {
		point    string
		outcome  Outcome
		newImage bool
	}{
		{"segment.write:after", NotApplied, false},
		{"data.write:after", NotApplied, false},
		{"segment.sync:after", NotApplied, false},
		{"catalog.sync:after", NotApplied, false},
		{"catalog.dirsync:after", NotApplied, false},
		{"current.sync:after", NotApplied, false},
		{"current.rename:before", Unknown, false},
		{"current.rename:after", Unknown, true},
		{"current.dirsync:before", Unknown, true},
		{"current.dirsync:after", Unknown, true},
		{"rotation.close:after", Committed, true},
	} {
		t.Run(tc.point, func(t *testing.T) {
			armed := false
			fs := &storage.IO{CleanerStart: make(chan struct{}), Hook: func(p string) error {
				if armed && p == tc.point {
					return syscall.EIO
				}
				return nil
			}}
			opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: 128}
			db := newTestDB(t, opts, fs)
			mustPut(t, db, "k", "old")
			before := db.store.Current
			armed = true
			err := db.Put([]byte("k"), []byte("new"))
			var op *OpError
			if !errors.As(err, &op) || op.Outcome != tc.outcome || !errors.Is(err, syscall.EIO) ||
				errors.Is(err, ErrCommitUncertain) != (tc.outcome == Unknown) {
				t.Fatalf("rotation returned %v; want %s with EIO", err, tc.outcome)
			}
			if err := db.Put([]byte("k"), []byte("next")); !errors.Is(err, ErrUnhealthy) {
				t.Fatalf("failed rotation allowed another write: %v", err)
			}
			dir := db.store.Dir
			if err := db.Close(); !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			image, err := OpenWithIO(dir, opts, &storage.IO{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			frontier := image.store.Current
			if tc.newImage {
				wantValue(t, image, "k", "new")
				if frontier.ActiveID == before.ActiveID || frontier.Generation != before.Generation+1 || frontier.Sequence != before.Sequence+1 {
					t.Fatalf("rotating record and catalog published separately: %+v -> %+v", before, frontier)
				}
			} else {
				wantValue(t, image, "k", "old")
				if frontier != before {
					t.Fatalf("previous frontier changed: %+v -> %+v", before, frontier)
				}
			}
			entries, err := os.ReadDir(dir)
			// CURRENT, LOCK, one manifest, the active log and all selected sealed logs.
			if err != nil || len(entries) != 4+len(image.store.Catalog.Files) {
				t.Fatalf("unexpected objects after recovery: %v, %v", entries, err)
			}
		})
	}
}
