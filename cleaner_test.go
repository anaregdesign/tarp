package tarp

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestCleanerRanksNetSavings(t *testing.T) {
	s := &storage.Store{Files: map[uint64]*storage.File{}, Manifest: make([]byte, 64)}
	// Output is 64-byte header + 256 live bytes + 192 metadata bytes = 512.
	for _, n := range []int64{1023, 1024, 1025} {
		e := format.File{ID: 2, Kind: format.Log, Bytes: n, First: 1, Last: 3}
		f := &storage.File{}
		f.LiveBytes.Store(192)
		f.DeleteBytes.Store(64)
		s.Files[e.ID] = f
		s.Catalog.Files = []format.File{e}
		got, needed := nextCandidate(s)
		if (got.ID == e.ID) != (n >= 1024) || got.ID != 0 && needed != 512 {
			t.Fatalf("source %d: selected %+v, staging %d", n, got, needed)
		}
	}
	s.Files[3] = &storage.File{}
	s.Catalog.Files = append(s.Catalog.Files, format.File{ID: 3, Kind: format.Cleaned, Bytes: 1025, First: 4, Last: 7})
	if got, _ := nextCandidate(s); got.ID != 3 {
		t.Fatal("higher net savings should win", got)
	}
	s.Catalog.Files[1].Bytes = math.MaxInt64
	s.Files[3].LiveBytes.Store(math.MaxInt64 - 64)
	if got, _ := nextCandidate(s); got.ID != 2 {
		t.Fatal("large low-benefit file should be skipped", got)
	}
	s.Catalog.Files[1].Bytes = s.Catalog.Files[0].Bytes
	s.Files[3].LiveBytes.Store(256)
	if got, _ := nextCandidate(s); got.ID != 2 {
		t.Fatal("oldest covered interval should win the tie", got)
	}
}

// The public entry point has no scheduling hooks. Observe actual unlink rather
// than assuming a number of scheduler turns or a disk-space delta.
func waitForUnlink(t *testing.T, db *DB, file string) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if _, err := os.Stat(file); errors.Is(err, os.ErrNotExist) {
			return
		} else if err != nil {
			t.Fatal(err)
		}
		select {
		case <-deadline.C:
			t.Fatalf("source still selected or pinned: %+v", db.MaintenanceStatus())
		default:
			runtime.Gosched()
		}
	}
}

func TestDefaultOpenAutomaticallyCleans(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		name := "new"
		if reopen {
			name = "reopen"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "store")
			var db *DB
			var err error
			if reopen {
				// Build an already eligible store and let public Open see it at startup.
				db, err = openWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
			} else {
				db, err = Open(dir, Options{})
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			source := db.store.Files[db.store.Current.ActiveID].Path
			value := bytes.Repeat([]byte("v"), 512<<10)
			for i := 0; i < 9; i++ {
				value[0] = byte(i)
				if err := db.Put([]byte("hot"), value); err != nil {
					t.Fatal(err)
				}
			}
			if reopen {
				if err = db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(dir, Options{})
				if err != nil {
					t.Fatal(err)
				}
			}
			waitForUnlink(t, db, source)
			got, err := db.Get([]byte("hot"))
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("lost the latest value: %v", err)
			}
			db.writer.Lock()
			seq := db.store.Current.Sequence
			db.writer.Unlock()
			if seq != 9 {
				t.Fatalf("cleaning allocated sequence %d", seq)
			}
			backup, restored := dir+"-backup", dir+"-restored"
			if err := db.Backup(context.Background(), backup); err != nil {
				t.Fatal(err)
			}
			if err := Restore(context.Background(), backup, restored, Options{}); err != nil {
				t.Fatal(err)
			}
			image, err := Open(restored, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			if err := image.Put([]byte("next"), nil); err != nil {
				t.Fatal(err)
			}
			image.writer.Lock()
			next := image.store.Current.Sequence
			image.writer.Unlock()
			if next != 10 {
				t.Fatalf("restored next sequence %d", next)
			}
		})
	}
}

func TestWorkerDefersForHeadroomAndRecovers(t *testing.T) {
	start := make(chan struct{})
	deferral := make(chan struct{})
	var attempts atomic.Int32
	fs := &storage.IO{CleanerStart: start, Hook: func(p string) error {
		if p == "clean.headroom:before" && attempts.Add(1) == 1 {
			close(deferral)
			return syscall.ENOSPC
		}
		return nil
	}}
	db := newTestDB(t, Options{TargetSegmentBytes: 8192, MaxRecordBytes: 4096}, fs)
	source := db.store.Files[db.store.Current.ActiveID].Path
	for i := 0; i < 5; i++ {
		mustPut(t, db, "hot", string(bytes.Repeat([]byte{byte(i)}, 2048)))
	}
	// Direct a single pass through the private seam so the deferral observation
	// cannot race the worker's successful retry.
	_, retry := db.cleanPass(context.Background())
	if !retry || db.MaintenanceStatus().DeferredReason != "space" || db.health() != nil {
		t.Fatalf("expected healthy space deferral: %+v", db.MaintenanceStatus())
	}
	<-deferral
	if _, err := os.Stat(source); err != nil {
		t.Fatal(err)
	}
	close(start)
	waitForUnlink(t, db, source)
}

func TestCloseWhileWorkerWaitsForAdmission(t *testing.T) {
	db := newTestDB(t, testOptions(), &storage.IO{CleanerStart: make(chan struct{})})
	done := make(chan error)
	go func() { done <- db.Close() }()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case <-db.cleaner.done:
	default:
		t.Fatal("worker still running after close")
	}
}

// A waiting worker holds the maintenance slot while yielding to backup. A
// manual checkpoint must be able to wake it and take that slot before backup ends.
func TestManualCheckpointPreemptsCleanerDuringBackup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "source")
	seedCleanerCrashImage(t, dir)
	scan, resumeScan := make(chan struct{}), make(chan struct{})
	backup, resumeBackup := make(chan struct{}), make(chan struct{})
	var scanOnce, backupOnce atomic.Bool
	fs := &storage.IO{Hook: func(p string) error {
		if p == "clean.scan:before" && scanOnce.CompareAndSwap(false, true) {
			close(scan)
			<-resumeScan
		}
		if p == "backup.copy:before" && backupOnce.CompareAndSwap(false, true) {
			close(backup)
			<-resumeBackup
		}
		return nil
	}}
	db, err := openWithIO(dir, cleanerCrashOptions(), fs)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	<-scan
	backupDone := make(chan error, 1)
	go func() { backupDone <- db.Backup(context.Background(), dir+"-backup") }()
	<-backup
	defer func() {
		close(resumeBackup)
		if err := <-backupDone; err != nil {
			t.Error(err)
		}
	}()
	close(resumeScan)
	deadline := time.After(5 * time.Second)
	for db.MaintenanceStatus().DeferredReason != "backup" {
		select {
		case <-deadline:
			t.Fatal("worker did not yield to backup")
		default:
			runtime.Gosched()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := db.Compact(ctx)
	if err != nil || result.PinnedBytes == 0 {
		t.Fatalf("manual checkpoint while backup is pinned: %+v, %v", result, err)
	}
}
