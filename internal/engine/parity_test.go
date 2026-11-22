package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func selectedParity(t *testing.T, db *DB, entry format.CatalogEntry) string {
	t.Helper()
	db.writer.Lock()
	uuid := db.store.Catalog.UUID
	db.writer.Unlock()
	name := strings.Replace(storage.DataName(uuid, entry.ID), "DATA-", "PARITY-", 1)
	return filepath.Join(db.store.Dir, name)
}

func TestProtectParityWakesCleanerToRepackSmallSegments(t *testing.T) {
	startCleaner := make(chan struct{})
	db := newTestDBWithHooks(t, Options{IndexMode: IndexMemory}, nil, &Hooks{CleanerStart: startCleaner})
	// The idle cleaner has already consumed its startup notification.
	<-db.cleaner.wake
	for _, key := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, key, key)
		if err := db.ProtectParity(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.cleaner.wake) == 0 {
		t.Fatal("explicit rotations left the cleaner asleep")
	}
	close(startCleaner)
	waitForDataFiles(t, db, 2) // Repacked DATA and the empty active log.
	for _, key := range []string{"a", "b", "c", "d"} {
		wantValue(t, db, key, key)
	}
}

func TestParityBuildAllowsForegroundRotationAndContextCancellation(t *testing.T) {
	started, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	fs := &storage.IO{Hook: func(point string) error {
		if point == "parity.sync:before" {
			once.Do(func() { close(started); <-resume })
		}
		return nil
	}}
	opts := Options{MaxRecordBytes: 72, TargetSegmentBytes: 144}
	start := make(chan struct{})
	close(start)
	db := newTestDBWithHooks(t, opts, fs, &Hooks{CleanerStart: make(chan struct{}), ParityStart: start})
	mustPut(t, db, "a", "one")
	if names, _ := filepath.Glob(filepath.Join(db.store.Dir, "PARITY-*")); len(names) != 0 {
		t.Fatal("protected the mutable active file")
	}
	mustPut(t, db, "b", "two") // First rotation schedules the worker.
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("automatic parity did not start")
	}
	done := make(chan error, 1)
	go func() { done <- db.Put([]byte("c"), []byte("three")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parity sync blocked foreground writer")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := db.ProtectParity(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting for busy builder: %v", err)
	}
	release()
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	db.writer.Lock()
	entries := db.store.Catalog.Entries
	uuid := db.store.Catalog.UUID
	db.writer.Unlock()
	for _, entry := range entries {
		fd, err := os.Open(selectedParity(t, db, entry))
		if err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(format.ValidateParitySidecar(t.Context(), fd, uuid, entry), fd.Close()); err != nil {
			t.Fatal(err)
		}
	}
	if db.store.Catalog.Index.ID != 0 || db.store.Current.ActiveCommittedEndOffset != format.SegmentHeaderBytes || len(entries) != 3 {
		t.Fatalf("explicit protection sealed the foreground tail: files %v, index %v, active end %v", entries, db.store.Catalog.Index, db.store.Current.ActiveCommittedEndOffset)
	}
}

func TestProtectParityPreservesSourceAndRetriesFailedPublication(t *testing.T) {
	for _, point := range []string{"parity.write:after", "parity.sync:after", "parity.rename:before", "parity.rename:after", "parity.dirsync:after"} {
		t.Run(point, func(t *testing.T) {
			var armed atomic.Bool
			fs := &storage.IO{Hook: func(p string) error {
				if p == point && armed.CompareAndSwap(true, false) {
					return syscall.ENOSPC
				}
				return nil
			}}
			db := newTestDBWithHooks(t, testOptions(), fs, &Hooks{CleanerStart: make(chan struct{}), ParityStart: make(chan struct{})})
			mustPut(t, db, "key", "value")
			entry := sealForCleaning(t, db)
			before := db.store.Current
			armed.Store(true)
			if err := db.ProtectParity(t.Context()); !errors.Is(err, ErrNoSpace) || armed.Load() {
				t.Fatalf("publication error: %v", err)
			}
			if db.store.Current != before {
				t.Fatal("changed committed frontier")
			}
			wantValue(t, db, "key", "value")
			mustPut(t, db, "tail", "next")
			if err := db.ProtectParity(t.Context()); err != nil {
				t.Fatalf("retry: %v", err)
			}
			path := selectedParity(t, db, entry)
			if err := os.WriteFile(path, []byte("damaged"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := db.ProtectParity(t.Context()); err != nil {
				t.Fatalf("regenerate damaged sidecar: %v", err)
			}
			fd, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(format.ValidateParitySidecar(t.Context(), fd, db.store.Catalog.UUID, entry), fd.Close()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestParityCloseProtectsBTreeCheckpointAndOpenRebuildsMissingSidecar(t *testing.T) {
	db := newTestDBWithHooks(t, Options{IndexMode: IndexBTree}, nil, &Hooks{CleanerStart: make(chan struct{}), ParityStart: make(chan struct{})})
	mustPut(t, db, "k", "value")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(db.store.Dir, "PARITY-*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("final B+tree rotation parity: %v, %v", files, err)
	}
	if err := os.Remove(files[0]); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	var once sync.Once
	io := &storage.IO{Hook: func(point string) error {
		if point == "parity.dirsync:after" {
			once.Do(func() { close(ready) })
		}
		return nil
	}}
	reopened, err := OpenWithHooks(db.store.Dir, Options{OpenValidation: Indexed}, io, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("reopen did not rebuild missing sidecar")
	}
	wantValue(t, reopened, "k", "value")
}

func TestParityFailureOnCloseReleasesOwnershipAndPreservesWrites(t *testing.T) {
	fs := &storage.IO{Hook: func(point string) error {
		if point == "parity.dirsync:before" {
			return syscall.ENOSPC
		}
		return nil
	}}
	dir := t.TempDir()
	db, err := OpenWithHooks(dir, testOptions(), fs, &Hooks{CleanerStart: make(chan struct{}), ParityStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "a", "one")
	sealForCleaning(t, db)
	if err := db.Close(); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("close: %v", err)
	}
	reopened, err := Open(dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, reopened, "a", "one")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestParitySourceReadFailurePoisonsOnlyStillSelectedData(t *testing.T) {
	for _, compact := range []bool{false, true} {
		name := "selected"
		if compact {
			name = "replaced"
		}
		t.Run(name, func(t *testing.T) {
			started, resume := make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			defer release()
			var injected atomic.Bool
			fs := &storage.IO{Hook: func(point string) error {
				if point == "parity.scan:before" && injected.CompareAndSwap(false, true) {
					close(started)
					<-resume
					return syscall.EIO
				}
				return nil
			}}
			db := newTestDBWithHooks(t, testOptions(), fs, parkedMaintenance())
			mustPut(t, db, "key", "acknowledged")
			done := make(chan error, 1)
			go func() { done <- db.ProtectParity(t.Context()) }()
			select {
			case <-started:
			case <-time.After(10 * time.Second):
				t.Fatal("did not begin building explicit cut")
			}
			old := db.store.Catalog.Entries[0]
			if compact {
				result, err := db.Compact(t.Context())
				if err != nil || result.PinnedRetiredDataBytes != old.Bytes {
					t.Fatalf("pinned retired bytes %d, want %d: %v", result.PinnedRetiredDataBytes, old.Bytes, err)
				}
			}
			release()
			err := <-done
			if !compact {
				if !errors.Is(err, syscall.EIO) || !errors.Is(db.health(), ErrUnhealthy) {
					t.Fatalf("source error %v, health %v", err, db.health())
				}
				return
			}
			if err != nil {
				t.Fatalf("obsolete source failed recapture: %v", err)
			}
			wantValue(t, db, "key", "acknowledged")
			now := db.store.Catalog.Entries[0]
			if now.Kind != format.SnapshotKind || now.ID == old.ID {
				t.Fatalf("invalid snapshot %+v", now)
			}
			if _, err := os.Stat(selectedParity(t, db, now)); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Compact(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(db.store.Dir, storage.DataName(db.store.Current.UUID, old.ID))); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retired DATA after pin release: %v", err)
			}
		})
	}
}

func TestParityProtectsCleanedDataAndSkipsMetadataOnlyIntervals(t *testing.T) {
	// Neither worker runs. A successful clean must already own durable parity.
	db := newTestDBWithHooks(t, Options{}, nil, parkedMaintenance())
	mustPut(t, db, "obsolete", "old")
	empty := sealForCleaning(t, db)
	mustPut(t, db, "stable", "live")
	mustPut(t, db, "updated", "old")
	sparse := sealForCleaning(t, db)
	mustPut(t, db, "obsolete", "replaced")
	mustPut(t, db, "updated", "new")
	if err := db.cleanSegment(t.Context(), empty); err != nil {
		t.Fatal(err)
	}
	if err := db.cleanSegment(t.Context(), sparse); err != nil {
		t.Fatal(err)
	}
	entries := db.store.Catalog.Entries
	if len(entries) != 2 || entries[0].HasPhysicalFile() || entries[1].Kind != format.CleanedLogKind || !entries[1].HasPhysicalFile() {
		t.Fatalf("invalid fixture %+v", entries)
	}
	found, err := filepath.Glob(filepath.Join(db.store.Dir, "PARITY-*"))
	if err != nil || len(found) != 1 || found[0] != selectedParity(t, db, entries[1]) {
		t.Fatalf("cleaned parity %v, error %v", found, err)
	}
}

func TestProtectParityRejectsStagedOutputCorruptionAndRetries(t *testing.T) {
	var damaged atomic.Bool
	fs := &storage.IO{WriteAt: func(fd *os.File, b []byte, offset int64) (int, error) {
		if strings.HasPrefix(filepath.Base(fd.Name()), "tarp-parity-") && damaged.CompareAndSwap(false, true) {
			b = append([]byte(nil), b...)
			b[len(b)-1] ^= 0x80
		}
		return fd.WriteAt(b, offset)
	}}
	db := newTestDBWithHooks(t, testOptions(), fs, parkedMaintenance())
	mustPut(t, db, "key", "value")
	if err := db.ProtectParity(t.Context()); !errors.Is(err, format.ErrInvalidParity) || db.health() != nil {
		t.Fatalf("damaged stage classification: %v, health %v", err, db.health())
	}
	entry := db.store.Catalog.Entries[0]
	if _, err := os.Stat(selectedParity(t, db, entry)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published invalid sidecar: %v", err)
	}
	wantValue(t, db, "key", "value")
	mustPut(t, db, "new", "commit")
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestParityWorkerProtectsLaterFilesAfterUnrecoverableSource(t *testing.T) {
	var scans atomic.Int32
	fs := &storage.IO{Hook: func(point string) error {
		if point == "parity.scan:before" {
			scans.Add(1)
		}
		return nil
	}}
	db := openMaintenanceDamageDB(t, Options{}, fs)
	mustPut(t, db, "unprotected", "original")
	bad := sealForCleaning(t, db)
	f := db.store.Segments[bad.ID]
	data, err := os.ReadFile(f.Path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(f.Path, data, 0600); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "second", "intact")
	later := sealForCleaning(t, db)
	if err := db.protectSealed(t.Context(), nil); err != nil || db.health() != nil {
		t.Fatalf("worker stopped on damaged DATA: %v, health %v", err, db.health())
	}
	if f.UnavailableData() == nil || db.parity.next != 2 || scans.Load() != 2 {
		t.Fatalf("worker did not advance: unavailable=%v cursor=%d scans=%d", f.UnavailableData(), db.parity.next, scans.Load())
	}
	if _, err := os.Stat(selectedParity(t, db, bad)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("parity built from damaged input: %v", err)
	}
	if _, err := os.Stat(selectedParity(t, db, later)); err != nil {
		t.Fatalf("healthy file left unprotected: %v", err)
	}
	if err := db.ProtectParity(t.Context()); !errors.Is(err, ErrDataUnavailable) || scans.Load() != 2 {
		t.Fatalf("explicit protection: %v, scans %d", err, scans.Load())
	}
	mustPut(t, db, "continued", "value")
	if err := db.Close(); !errors.Is(err, ErrDataUnavailable) || errors.Is(err, ErrUnhealthy) {
		t.Fatalf("close: %v", err)
	}
}
