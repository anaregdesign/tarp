package engine

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
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

func TestRepackGroupLimits(t *testing.T) {
	const target = 4096
	for _, tc := range []struct {
		name         string
		sizes        []int64
		first, count int
	}{
		{"too few", []int64{128, 128, 128}, 0, 0},
		{"exact target", []int64{1024, 1024, 1024, 1024, 128}, 1, 4},
		{"large separator", []int64{128, 128, 1025, 128, 128, 128}, 0, 0},
		{"skip dominant file", []int64{1024, 128, 128, 128, 128}, 2, 4},
		{"balanced at equality", []int64{768, 256, 256, 256}, 1, 4},
		{"eight file limit", []int64{128, 128, 128, 128, 128, 128, 128, 128, 128}, 1, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var files []format.File
			for i, size := range tc.sizes {
				files = append(files, format.File{ID: uint64(i + 1), Kind: format.Log, Bytes: size})
			}
			group, bytes := repackGroup(files, target)
			if len(group) != tc.count || len(group) > 0 && group[0].ID != uint64(tc.first) {
				t.Fatalf("selected %+v, want %d files starting at ID %d", group, tc.count, tc.first)
			}
			var wantBytes int64
			for _, f := range group {
				wantBytes += f.Bytes
			}
			if bytes != wantBytes {
				t.Fatalf("estimated input bytes %d, want %d", bytes, wantBytes)
			}
		})
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
				db, err = OpenWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
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
	db, err := OpenWithIO(dir, cleanerCrashOptions(), fs)
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

func cleanerCrashOptions() Options {
	return Options{MaxRecordBytes: 4096, TargetSegmentBytes: 16 << 10}
}

func seedCleanerCrashImage(t *testing.T, dir string) {
	t.Helper()
	db, err := OpenWithIO(dir, cleanerCrashOptions(), &storage.IO{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"keep", "hot", "gone", "hot", "again", "hot"} {
		if err := db.Put([]byte(key), bytes.Repeat([]byte{byte(i)}, 3000)); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []string{"gone", "again"} {
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	mustPut(t, db, "again", "new")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCleanerPublicationCrashMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess crash sweep")
	}
	for _, phase := range []string{"clean.output.sync", "clean.catalog.sync", "clean.catalog.dirsync",
		"clean.current.sync", "clean.current.rename", "clean.current.dirsync", "clean.retire.unlink", "clean.retire.dirsync", "clean.remap"} {
		for _, edge := range []string{":before", ":after"} {
			if phase == "clean.remap" && edge == ":before" {
				continue
			}
			point := phase + edge
			t.Run(point, func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "db")
				seedCleanerCrashImage(t, dir)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCleanerCrashChild$")
				cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0", "TARP_CLEANER_CRASH_DB="+dir, "TARP_CLEANER_CRASH_POINT="+point)
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 91 {
					t.Fatalf("interruption at %s: %v\n%s", point, err, output)
				}
				// Stop further physical cleaning during inspection so either permissible
				// on-disk selection must stand on its own.
				db, err := OpenWithIO(dir, cleanerCrashOptions(), &storage.IO{CleanerStart: make(chan struct{})})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if db.store.Current.Sequence != 9 || len(db.index) != 3 {
					t.Fatal("lost the acknowledged frontier or key set")
				}
				for k, v := range map[string][]byte{"hot": bytes.Repeat([]byte{5}, 3000), "keep": make([]byte, 3000), "again": []byte("new")} {
					got, err := db.Get([]byte(k))
					if err != nil || !bytes.Equal(got, v) {
						t.Fatalf("recovery changed key %q: %v", k, err)
					}
				}
				if _, err := db.Get([]byte("gone")); !errors.Is(err, ErrNotFound) {
					t.Fatal("deleted key reappeared", err)
				}
				mustPut(t, db, "next", "")
				if db.store.Current.Sequence != 10 {
					t.Fatal("incorrect next sequence")
				}
			})
		}
	}
}

func TestCleanerCrashChild(t *testing.T) {
	dir := os.Getenv("TARP_CLEANER_CRASH_DB")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	point := os.Getenv("TARP_CLEANER_CRASH_POINT")
	fs := &storage.IO{Hook: func(p string) error {
		if p == point {
			os.Exit(91)
		}
		return nil
	}}
	db, err := OpenWithIO(dir, cleanerCrashOptions(), fs)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-db.cleaner.done:
		t.Fatalf("worker stopped before cut %s: %+v", point, db.MaintenanceStatus())
	case <-time.After(4 * time.Second):
		t.Fatalf("cut %s not reached: %+v", point, db.MaintenanceStatus())
	}
}
