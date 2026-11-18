package tarp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestCompactionReclaimsAndPreservesEmptyCutoff(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	for round := 0; round < 6; round++ {
		for i := 0; i < 4; i++ {
			mustPut(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("round-%d-%s", round, strings.Repeat("x", 20)))
		}
	}
	if err := db.Delete([]byte("k0")); err != nil {
		t.Fatal(err)
	}
	cutoff := db.store.Current.Sequence
	result, err := db.Compact(context.Background())
	if err != nil || result.ReclaimedBytes == 0 || db.store.Catalog.Cutoff != cutoff {
		t.Fatalf("compact: %+v %v", result, err)
	}
	for i := 1; i < 4; i++ {
		wantValue(t, db, fmt.Sprintf("k%d", i), "round-5-"+strings.Repeat("x", 20))
		if err := db.Delete([]byte(fmt.Sprintf("k%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Get([]byte("k0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resurrected key: %v", err)
	}
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	emptySequence := db.store.Current.Sequence
	if len(db.store.Catalog.Files) != 0 || emptySequence == 0 {
		t.Fatalf("empty checkpoint: %+v", db.store.Catalog)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, reopened, "next", "value")
	if reopened.store.Current.Sequence != emptySequence+1 {
		t.Fatal("sequence lost with last deleted key")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackupPinsPointInTimeAcrossWritesAndCompaction(t *testing.T) {
	var armed atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	fs := &storage.IO{Hook: func(p string) error {
		if p == "backup.copy:before" && armed.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return nil
	}}
	db := newTestDB(t, testOptions(), fs)
	mustPut(t, db, "a", "old")
	mustPut(t, db, "d", "keep-in-backup")
	target := filepath.Join(t.TempDir(), "backup")
	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- db.Backup(context.Background(), target) }()
	<-started
	if err := db.Backup(context.Background(), filepath.Join(t.TempDir(), "other")); !errors.Is(err, ErrBusy) {
		t.Fatalf("second backup: %v", err)
	}
	mustPut(t, db, "a", "new")
	if err := db.Delete([]byte("d")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		mustPut(t, db, fmt.Sprint(i), "tail")
	}
	result, err := db.Compact(context.Background())
	if err != nil || result.PinnedBytes == 0 {
		t.Fatalf("pinned compaction: %+v %v", result, err)
	}
	wantValue(t, db, "a", "new")
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(context.Background(), target, restored, testOptions()); err != nil {
		t.Fatal(err)
	}
	image, err := Open(restored, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, image, "a", "old")
	wantValue(t, image, "d", "keep-in-backup")
	if _, err := image.Get([]byte("0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("backup included later tail: %v", err)
	}
	mustPut(t, image, "restored-write", "ok")
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}
	result, err = db.Compact(context.Background())
	if err != nil || result.ReclaimedBytes == 0 || result.PinnedBytes != 0 {
		t.Fatalf("deferred cleanup: %+v %v", result, err)
	}
}

func TestCompactionConcurrentTailWins(t *testing.T) {
	var armed atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
		if p == "compact.write:before" && armed.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return nil
	}})
	mustPut(t, db, "a", "old")
	mustPut(t, db, "b", "deleted")
	armed.Store(true)
	done := make(chan error, 1)
	go func() { _, err := db.Compact(context.Background()); done <- err }()
	<-started
	if _, err := db.Compact(context.Background()); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	mustPut(t, db, "a", "new")
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		mustPut(t, db, fmt.Sprint(i), strings.Repeat("x", 20))
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, reopened, "a", "new")
	if _, err := reopened.Get([]byte("b")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resurrected deleted key: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMissSurvivesRelocationAndRetirement(t *testing.T) {
	var armed atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	opts := testOptions()
	opts.CacheBytes = 140
	db := newTestDB(t, opts, &storage.IO{Hook: func(p string) error {
		if p == "read:after" && armed.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return nil
	}})
	mustPut(t, db, "a", "old")
	mustPut(t, db, "x", "evict")
	armed.Store(true)
	done := make(chan error, 1)
	go func() {
		v, err := db.Get([]byte("a"))
		if err == nil && string(v) != "old" {
			err = fmt.Errorf("relocated read: %q", v)
		}
		done <- err
	}()
	<-started
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wantValue(t, db, "a", "old")
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestBackupDestinationFailuresDoNotPoisonSource(t *testing.T) {
	for _, point := range []string{"backup.copy:before", "backup.file.sync:before", "backup.rename:after", "backup.parent.sync:before"} {
		t.Run(point, func(t *testing.T) {
			var armed atomic.Bool
			db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
				if p == point && armed.CompareAndSwap(true, false) {
					return syscall.EIO
				}
				return nil
			}})
			mustPut(t, db, "a", "v")
			target := filepath.Join(t.TempDir(), "backup")
			armed.Store(true)
			err := db.Backup(context.Background(), target)
			if !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			uncertain := point == "backup.rename:after" || point == "backup.parent.sync:before"
			if errors.Is(err, ErrCommitUncertain) != uncertain {
				t.Fatalf("publication outcome: %v", err)
			}
			wantValue(t, db, "a", "v")
			mustPut(t, db, "a", "still-healthy")
			if uncertain {
				if err := Restore(context.Background(), target, filepath.Join(t.TempDir(), "restored"), testOptions()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestMaintenanceCancellationAndNoSpace(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprint(compact), func(t *testing.T) {
			var armed atomic.Bool
			ctx, cancel := context.WithCancel(context.Background())
			point := "backup.copy:after"
			if compact {
				point = "compact.write:after"
			}
			db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
				if p == point && armed.CompareAndSwap(true, false) {
					cancel()
				}
				return nil
			}})
			defer cancel()
			mustPut(t, db, "a", strings.Repeat("v", 60))
			armed.Store(true)
			var err error
			if compact {
				_, err = db.Compact(ctx)
			} else {
				err = db.Backup(ctx, filepath.Join(t.TempDir(), "backup"))
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation: %v", err)
			}
			wantValue(t, db, "a", strings.Repeat("v", 60))
			mustPut(t, db, "a", "new")
		})
	}
	var armed atomic.Bool
	db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
		if p == "compact.write:after" && armed.CompareAndSwap(true, false) {
			return syscall.ENOSPC
		}
		return nil
	}})
	mustPut(t, db, "a", "old")
	armed.Store(true)
	if _, err := db.Compact(context.Background()); !errors.Is(err, ErrNoSpace) {
		t.Fatal(err)
	}
	wantValue(t, db, "a", "old")
	mustPut(t, db, "a", "new")
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRestoreValidationAndNoReplace(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "a", strings.Repeat("v", 50))
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(context.Background(), backup); !errors.Is(err, os.ErrExist) {
		t.Fatalf("backup replacement: %v", err)
	}
	if err := db.Backup(context.Background(), filepath.Join(db.store.Dir, "nested")); err == nil {
		t.Fatal("accepted nested backup")
	}
	destination := t.TempDir()
	before := directoryBytes(t, backup)
	if err := Restore(context.Background(), backup, destination, testOptions()); !errors.Is(err, os.ErrExist) {
		t.Fatalf("restored over empty existing directory: %v", err)
	}
	opts := testOptions()
	opts.CacheBytes = 140
	if err := Restore(context.Background(), backup, filepath.Join(t.TempDir(), "small"), opts); !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatalf("restore smaller cache: %v", err)
	}
	if !reflect.DeepEqual(before, directoryBytes(t, backup)) {
		t.Fatal("restore changed backup")
	}
	for name := range before {
		if strings.HasPrefix(name, "DATA-") {
			path := filepath.Join(backup, name)
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			b[len(b)-1] ^= 0xff
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if err := Restore(context.Background(), backup, filepath.Join(t.TempDir(), "corrupt"), testOptions()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("restore accepted corruption: %v", err)
	}
}

func TestMaintenanceSourceCorruptionFailsStop(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprint(compact), func(t *testing.T) {
			db := newTestDB(t, testOptions(), nil)
			mustPut(t, db, "a", "cached")
			path := db.index["a"].File.Path
			f, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			offset := int64(format.SegmentHeaderBytes + format.RecordHeaderBytes)
			if _, err := f.WriteAt([]byte("bad"), offset); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if compact {
				_, err = db.Compact(context.Background())
			} else {
				err = db.Backup(context.Background(), filepath.Join(t.TempDir(), "backup"))
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatal(err)
			}
			if _, err := db.Get([]byte("a")); !errors.Is(err, ErrUnhealthy) {
				t.Fatalf("cached read escaped corrupt source: %v", err)
			}
		})
	}
}
