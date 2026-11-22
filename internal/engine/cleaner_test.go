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
	s := &storage.Store{Segments: map[uint64]*storage.DataSegment{}, Manifest: make([]byte, 64)}
	// Output is 64-byte header + 256 live bytes + 192 metadata bytes = 512.
	for _, n := range []int64{1023, 1024, 1025} {
		e := format.CatalogEntry{ID: 2, Kind: format.RawLogKind, Bytes: n, FirstSequence: 1, LastSequence: 3}
		f := &storage.DataSegment{}
		f.LiveBytes.Store(192)
		f.DeleteBytes.Store(64)
		s.Segments[e.ID] = f
		s.Catalog.Entries = []format.CatalogEntry{e}
		got, needed, _ := selectCleaningInputs(s, 4096)
		if (len(got) != 0) != (n >= 1024) || len(got) != 0 && (got[0].ID != e.ID || needed != 512) {
			t.Fatalf("source %d: selected %+v, staging %d", n, got, needed)
		}
	}
	s.Segments[3] = &storage.DataSegment{}
	s.Catalog.Entries = append(s.Catalog.Entries, format.CatalogEntry{ID: 3, Kind: format.CleanedLogKind, Bytes: 1025, FirstSequence: 4, LastSequence: 7})
	if got, _, _ := selectCleaningInputs(s, 4096); len(got) != 1 || got[0].ID != 3 {
		t.Fatal("higher net savings should win", got)
	}
	s.Catalog.Entries[1].Bytes = math.MaxInt64
	s.Segments[3].LiveBytes.Store(math.MaxInt64 - 64)
	if got, _, _ := selectCleaningInputs(s, 4096); len(got) != 1 || got[0].ID != 2 {
		t.Fatal("large low-benefit file should be skipped", got)
	}
	s.Catalog.Entries[1].Bytes = s.Catalog.Entries[0].Bytes
	s.Segments[3].LiveBytes.Store(256)
	if got, _, _ := selectCleaningInputs(s, 4096); len(got) != 1 || got[0].ID != 2 {
		t.Fatal("oldest covered interval should win the tie", got)
	}
}

func TestCleanerGroupsObsoleteLogsWithinBounds(t *testing.T) {
	type segment struct {
		kind                uint32
		size, live, deleted int64
	}
	stale := segment{kind: format.RawLogKind, size: 512}
	put := segment{kind: format.RawLogKind, size: 512, live: 32}
	del := segment{kind: format.CleanedLogKind, size: 512, deleted: 32}
	empty := segment{kind: format.CleanedLogKind}
	full := segment{kind: format.RawLogKind, size: 1024, live: 300}
	huge := segment{kind: format.RawLogKind, size: math.MaxInt64, live: math.MaxInt64/2 - 200}
	for _, tc := range []struct {
		name         string
		target       int64
		segments     []segment
		first, count int
		staging      uint64
	}{
		{"exact benefit across empty interval", 1024, []segment{put, empty, del}, 0, 3, 512},
		{"one byte above benefit", 1024, []segment{put, {format.CleanedLogKind, 512, 0, 33}}, 0, 0, 0},
		{"empty output needs no header", 1024, []segment{stale, stale}, 0, 2, 384},
		{"later single wins", 1024, []segment{put, del, {format.RawLogKind, 1024, 0, 0}}, 2, 1, 384},
		{"snapshot boundary", 1024, []segment{stale, {format.SnapshotKind, 512, 0, 0}, stale}, 0, 0, 0},
		{"oversized input", 1024, []segment{stale, {format.RawLogKind, 1025, 200, 0}}, 0, 0, 0},
		{"live separator", 1024, []segment{stale, {format.RawLogKind, 512, 300, 0}, stale}, 0, 0, 0},
		{"bounded output", 1024, []segment{full, full, full, full}, 0, 3, 1348},
		{"bounded scan", 1024, slices.Repeat([]segment{stale}, 10), 0, 8, 384},
		{"bounded scan including empty intervals", 1024, append(append([]segment{stale}, slices.Repeat([]segment{empty}, 7)...), stale), 0, 0, 0},
		{"source size overflow", math.MaxInt64, []segment{huge, huge, huge}, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &storage.Store{Segments: map[uint64]*storage.DataSegment{}, Manifest: make([]byte, 256)}
			for i, seg := range tc.segments {
				entry := format.CatalogEntry{Kind: seg.kind, Bytes: seg.size}
				if seg.size > 0 {
					entry.ID = uint64(i + 1)
					f := &storage.DataSegment{}
					f.LiveBytes.Store(seg.live)
					f.DeleteBytes.Store(seg.deleted)
					s.Segments[entry.ID] = f
				}
				s.Catalog.Entries = append(s.Catalog.Entries, entry)
			}
			got, staging, _ := selectCleaningInputs(s, tc.target)
			want := s.Catalog.Entries[tc.first : tc.first+tc.count]
			if !slices.Equal(got, want) || staging != tc.staging {
				t.Fatalf("selected %+v staging %d, want %+v staging %d", got, staging, want, tc.staging)
			}
		})
	}
}

func TestCleanerCleansConsecutiveCatalogLogsAndReopens(t *testing.T) {
	// Keep the cleaner's publication as the final frontier across Close.
	opts := Options{IndexMode: IndexMemory, MaxRecordBytes: 768, TargetSegmentBytes: 1024}
	parked := &Hooks{CleanerStart: make(chan struct{})}
	var noSpace atomic.Bool
	fs := &storage.IO{Hook: func(point string) error {
		if point == "clean.headroom:before" && noSpace.Swap(false) {
			return syscall.ENOSPC
		}
		return nil
	}}
	db := newTestDBWithHooks(t, opts, fs, parked)
	stale := strings.Repeat("x", 600)
	mustPut(t, db, "gone", stale)
	mustPut(t, db, "one", "1")
	first := sealForCleaning(t, db)
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "hot", stale)
	last := sealForCleaning(t, db)
	mustPut(t, db, "hot", "tail")
	before := db.store.Current

	// Neither source covers its own catalog publication. Preserve both if
	// staging space is unavailable, then rewrite them together on retry.
	noSpace.Store(true)
	if again, retry := db.cleanPass(t.Context()); again || !retry || db.store.Current != before {
		t.Fatalf("headroom deferral changed the source: %+v", db.AutoCompactionStatus())
	}
	if again, retry := db.cleanPass(t.Context()); !again || retry {
		t.Fatalf("group did not publish on retry: %+v", db.AutoCompactionStatus())
	}
	db.cleanPass(t.Context()) // Retire the sources before validating recovery.
	dir := db.store.Dir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	image, err := OpenWithHooks(dir, opts, nil, parked)
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	entries := image.store.Catalog.Entries
	if len(entries) != 1 || entries[0].Kind != format.CleanedLogKind || entries[0].FirstSequence != first.FirstSequence || entries[0].LastSequence != last.LastSequence ||
		entries[0].ID == first.ID || entries[0].ID == last.ID || image.store.Current.PublicationGeneration != before.PublicationGeneration+1 ||
		image.store.Current.ActiveID != before.ActiveID || image.store.Current.LastUserSequence != before.LastUserSequence {
		t.Fatalf("unexpected recovered group: %+v, frontier %+v", entries, image.store.Current)
	}
	if image.store.Segments[entries[0].ID].DeleteBytes.Load() == 0 {
		t.Fatal("cleaned group dropped its tombstone")
	}
	wantValue(t, image, "one", "1")
	wantValue(t, image, "hot", "tail")
	if _, err := image.Get([]byte("gone")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key after reopen: %v", err)
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
			var entries []format.CatalogEntry
			for i, size := range tc.sizes {
				entries = append(entries, format.CatalogEntry{ID: uint64(i + 1), Kind: format.RawLogKind, Bytes: size})
			}
			group, bytes := smallSegmentRepackGroup(entries, target, nil)
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
			t.Fatalf("source still selected or pinned: %+v", db.AutoCompactionStatus())
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
				db, err = OpenWithHooks(dir, Options{}, nil, &Hooks{CleanerStart: make(chan struct{})})
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
			source := db.store.Segments[db.store.Current.ActiveID].Path
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
			seq := db.store.Current.LastUserSequence
			db.writer.Unlock()
			if seq != 9 {
				t.Fatalf("cleaning allocated sequence %d", seq)
			}
			backup, restored := dir+"-backup", dir+"-restored"
			if err := db.Backup(context.Background(), backup); err != nil {
				t.Fatal(err)
			}
			if err := Restore(context.Background(), RestoreRequest{Source: backup, Destination: restored}); err != nil {
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
			next := image.store.Current.LastUserSequence
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
	fs := &storage.IO{Hook: func(p string) error {
		if p == "clean.headroom:before" && attempts.Add(1) == 1 {
			close(deferral)
			return syscall.ENOSPC
		}
		return nil
	}}
	engineHooks := &Hooks{CleanerStart: start}
	db := newTestDBWithHooks(t, Options{TargetSegmentBytes: 8192, MaxRecordBytes: 4096}, fs, engineHooks)
	source := db.store.Segments[db.store.Current.ActiveID].Path
	for i := 0; i < 5; i++ {
		mustPut(t, db, "hot", string(bytes.Repeat([]byte{byte(i)}, 2048)))
	}
	// Direct a single pass through the private seam so the deferral observation
	// cannot race the worker's successful retry.
	_, retry := db.cleanPass(context.Background())
	if !retry || db.AutoCompactionStatus().DeferredReason != "space" || db.health() != nil {
		t.Fatalf("expected healthy space deferral: %+v", db.AutoCompactionStatus())
	}
	<-deferral
	if _, err := os.Stat(source); err != nil {
		t.Fatal(err)
	}
	close(start)
	waitForUnlink(t, db, source)
}

func TestCloseWhileWorkerWaitsForAdmission(t *testing.T) {
	db := newTestDBWithHooks(t, testOptions(), nil, &Hooks{CleanerStart: make(chan struct{})})
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
// manual DATA checkpoint must be able to wake it and take that slot before backup ends.
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
	for db.AutoCompactionStatus().DeferredReason != "backup" {
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
	if err != nil || result.PinnedRetiredDataBytes == 0 {
		t.Fatalf("manual DATA checkpoint while backup is pinned: %+v, %v", result, err)
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
			t.Fatalf("found %d DATA files, want %d; worker status %+v", len(files), want, db.AutoCompactionStatus())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAutomaticRepackAfterSingleFileCleaning(t *testing.T) {
	// Keep the active log outside the repack group across both closes.
	opts := Options{IndexMode: IndexMemory, MaxRecordBytes: 4096, TargetSegmentBytes: 8192}
	parked := &Hooks{CleanerStart: make(chan struct{})}
	db := newTestDBWithHooks(t, opts, nil, parked)
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
		lastSealed = sealForCleaning(t, db).LastSequence
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
	if err := Restore(t.Context(), RestoreRequest{
		Source:      backup,
		Destination: restored,
		Limits:      opts.RestoreLimits(),
	}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, restored} {
		image, err := OpenWithHooks(path, opts, nil, parked)
		if err != nil {
			t.Fatal(err)
		}
		defer image.Close()
		entries := image.store.Catalog.Entries
		before := image.store.Current
		if len(entries) != 1 || entries[0].Kind != format.CleanedLogKind || entries[0].FirstSequence != 1 || entries[0].LastSequence != lastSealed ||
			before.LastUserSequence != lastSealed+1 || before.ActiveID != active || image.store.Catalog.CheckpointCutoffSequence != 0 {
			t.Fatalf("unexpected repacked layout: %+v, frontier %+v", image.store.Catalog, before)
		}
		if image.store.Segments[entries[0].ID].DeleteBytes.Load() == 0 {
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
	// Reopen exactly one sealed tail and one active tail.
	small := Options{IndexMode: IndexMemory, MaxRecordBytes: 128, TargetSegmentBytes: 192}
	db := newTestDBWithHooks(t, small, nil, &Hooks{CleanerStart: make(chan struct{})})
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
	entries := slices.Clone(live.store.Catalog.Entries)
	cutoff, after := live.store.Catalog.CheckpointCutoffSequence, live.store.Current
	live.writer.Unlock()
	if len(entries) != 2 || entries[0].Kind != format.SnapshotKind || entries[0].FirstSequence != 0 || entries[0].LastSequence != 0 ||
		entries[1] != tail || cutoff != 4 || after.ActiveID != before.ActiveID || after.LastUserSequence != before.LastUserSequence {
		t.Fatalf("snapshot repack changed the logical boundaries: cutoff %d, files %+v, frontier %+v", cutoff, entries, after)
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
	db, err := OpenWithHooks(dir, cleanerCrashOptions(), nil, &Hooks{CleanerStart: make(chan struct{})})
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
				db, err := OpenWithHooks(dir, cleanerCrashOptions(), nil, &Hooks{CleanerStart: make(chan struct{})})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if db.store.Current.LastUserSequence != 9 || db.Stats().LiveKeys != 3 {
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
				if db.store.Current.LastUserSequence != 10 {
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
		t.Fatalf("worker stopped before cut %s: %+v", point, db.AutoCompactionStatus())
	case <-time.After(4 * time.Second):
		t.Fatalf("cut %s not reached: %+v", point, db.AutoCompactionStatus())
	}
}

func TestCleanerKeepsUnavailableInputAndReclaimsOtherObsoleteFiles(t *testing.T) {
	db := openMaintenanceDamageDB(t, Options{}, nil)
	oldValue := strings.Repeat("x", 10000)
	mustPut(t, db, "damaged", oldValue)
	damaged := damageProtectedTail(t, db)
	if err := os.Remove(selectedParity(t, db, damaged)); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "healthy", oldValue)
	obsolete := sealForCleaning(t, db)
	mustPut(t, db, "damaged", "replacement")
	mustPut(t, db, "healthy", "new")

	if again, retry := db.cleanPass(t.Context()); !again || retry || !errors.Is(db.AutoCompactionStatus().LastError, ErrDataUnavailable) {
		t.Fatalf("first pass did not isolate damaged input: %+v", db.AutoCompactionStatus())
	}
	if again, retry := db.cleanPass(t.Context()); !again || retry {
		t.Fatalf("healthy cleaning was starved: %+v", db.AutoCompactionStatus())
	}
	if again, retry := db.cleanPass(t.Context()); again || retry || !errors.Is(db.AutoCompactionStatus().LastError, ErrDataUnavailable) {
		t.Fatalf("blocked input is being retried: %+v", db.AutoCompactionStatus())
	}
	if got := db.store.Catalog.Entries; len(got) != 2 || got[0] != damaged || got[1].ID != 0 {
		t.Fatalf("unexpected cleaned catalog %+v", got)
	}
	if _, err := os.Stat(db.store.Segments[damaged.ID].Path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(db.store.Dir, storage.DataName(db.store.Current.UUID, obsolete.ID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("obsolete healthy file not reclaimed: %v", err)
	}
	wantValue(t, db, "damaged", "replacement")
}

func TestRepackGroupUsesHealthyRunAfterUnavailableFile(t *testing.T) {
	var entries []format.CatalogEntry
	for id := uint64(1); id <= 7; id++ {
		entries = append(entries, format.CatalogEntry{ID: id, Kind: format.RawLogKind, Bytes: 128})
	}
	blocked := map[uint64]*storage.UnavailableDataError{3: {FileID: 3, Err: os.ErrNotExist}}
	group, n := smallSegmentRepackGroup(entries, 4096, blocked)
	if !slices.Equal(group, entries[3:]) || n != 4*128 {
		t.Fatalf("unexpected repack across unavailable file: %v, %d", group, n)
	}
}

func TestReadWakesCleanerToCheckpointUnrecoverableKeys(t *testing.T) {
	start := make(chan struct{})
	db := newTestDBWithHooks(t, Options{}, nil, &Hooks{CleanerStart: start, ParityStart: make(chan struct{})})
	mustPut(t, db, "kept", "healthy")
	mustPut(t, db, "lost", strings.Repeat("v", 10000))
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	if db.store.Current.LastUserSequence != db.store.Catalog.CheckpointCutoffSequence {
		t.Fatal("fixture has a post-checkpoint write")
	}
	entry := damageProtectedTail(t, db)
	path := db.store.Segments[entry.ID].Path
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[300] ^= 1 // Two bad blocks in the same record and stripe.
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	Observe(db).ClearCache()
	<-db.cleaner.wake // Drain earlier writes and Open's initial wake.
	if _, err := db.Get([]byte("lost")); !errors.Is(err, ErrDataUnavailable) {
		t.Fatalf("initial failing read: %v", err)
	}
	close(start)
	waitForUnlink(t, db, path)
	// Retirement happens before the checkpoint reports its result to the worker.
	<-db.cleaner.gate
	status := db.AutoCompactionStatus()
	db.cleaner.gate <- struct{}{}
	if status.DroppedKeys != 1 || status.UnlinkedBytes == 0 {
		t.Fatalf("unexpected automatic recovery result: %+v", status)
	}
	if _, err := db.Get([]byte("lost")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("discarded key: %v", err)
	}
	wantValue(t, db, "kept", "healthy")
	mustPut(t, db, "lost", "later")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	image, err := Open(db.store.Dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer image.Close()
	wantValue(t, image, "kept", "healthy")
	wantValue(t, image, "lost", "later")
}

func TestAutomaticCheckpointRejectsInvalidSidecarAfterUnrecoverableStripe(t *testing.T) {
	var scans atomic.Int64
	db := openMaintenanceDamageDB(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "compact.scan:before" {
			scans.Add(1)
		}
		return nil
	}})
	corruptUncachedCheckpointValue(t, db)
	entry := db.store.Catalog.Entries[0]
	path := selectedParity(t, db, entry)
	sidecar, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(sidecar, 1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("lost")); !errors.Is(err, ErrDataUnavailable) {
		t.Fatal(err)
	}
	before := db.store.Current
	if again, retry := db.cleanPass(t.Context()); again || retry || scans.Load() == 0 {
		t.Fatalf("automatic recovery did not defer: %+v", db.AutoCompactionStatus())
	}
	status := db.AutoCompactionStatus()
	if !errors.Is(status.LastError, format.ErrInvalidParity) || !errors.Is(status.LastError, ErrDataUnavailable) ||
		status.DroppedKeys != 0 || db.store.Current != before || db.health() != nil {
		t.Fatalf("invalid proof allowed progress or stopped service: %+v, %v", status, db.health())
	}
	scanCount := scans.Load()
	if again, retry := db.cleanPass(t.Context()); again || retry || scans.Load() != scanCount {
		t.Fatalf("retried invalid recovery proof: %+v", db.AutoCompactionStatus())
	}
	mustPut(t, db, "lost", "replacement")
	wantValue(t, db, "lost", "replacement")
}

func TestManualCompactPreemptsAutomaticRecoveryBeforePublication(t *testing.T) {
	ready, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	db := openMaintenanceDamageDB(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "compact.parity.sync:before" && first.CompareAndSwap(false, true) {
			close(ready)
			<-release
		}
		return nil
	}})
	corruptUncachedCheckpointValue(t, db)
	if _, err := db.Get([]byte("lost")); !errors.Is(err, ErrDataUnavailable) {
		t.Fatalf("initial read: %v", err)
	}
	autoDone := make(chan struct{})
	go func() {
		defer close(autoDone)
		db.cleanPass(t.Context())
	}()
	select {
	case <-ready:
	case <-autoDone:
		t.Fatalf("automatic checkpoint did not stage a replacement: %+v", db.AutoCompactionStatus())
	}
	select {
	case <-db.cleaner.resume:
	default:
	}
	type outcome struct {
		result CompactResult
		err    error
	}
	manualDone := make(chan outcome, 1)
	go func() {
		result, err := db.Compact(t.Context())
		manualDone <- outcome{result, err}
	}()
	<-db.cleaner.resume // Manual Compact has canceled the private worker checkpoint.
	close(release)
	<-autoDone
	manual := <-manualDone
	if manual.err != nil || manual.result.DroppedKeys != 1 || db.AutoCompactionStatus().DroppedKeys != 0 {
		t.Fatalf("manual recovery %+v, worker %+v", manual, db.AutoCompactionStatus())
	}
	wantValue(t, db, "keep", "healthy")
}
