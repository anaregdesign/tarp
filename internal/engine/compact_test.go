package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Hold the writer until the maintenance call passes its initial context check.
func cancelMaintenanceWaitingForWriter(t *testing.T, db *DB, waiting func() bool, run func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	db.writer.Lock()
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !waiting() && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	started := waiting()
	cancel()
	db.writer.Unlock()
	err := <-done
	if !started {
		t.Fatalf("maintenance did not reach the writer: %v", err)
	}
	var op *OpError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &op) || op.Outcome != NotApplied {
		t.Fatalf("expected a canceled, unapplied operation, got %v", err)
	}
}

func TestCompactCanceledWaitingForWriterPreservesFrontier(t *testing.T) {
	for _, state := range []string{"at cutoff", "nonempty log"} {
		t.Run(state, func(t *testing.T) {
			db := newTestDBWithHooks(t, testOptions(), nil, &Hooks{CleanerStart: make(chan struct{})})
			if state == "nonempty log" {
				mustPut(t, db, "key", "value")
			}
			before := db.store.Current
			unselected := filepath.Join(db.store.Dir, storage.DataName(before.UUID, 100))
			if err := os.WriteFile(unselected, []byte("leftover"), 0600); err != nil {
				t.Fatal(err)
			}
			cancelMaintenanceWaitingForWriter(t, db, func() bool {
				// Only this Compact can acquire the maintenance slot.
				return len(db.cleaner.gate) == 0
			}, func(ctx context.Context) error {
				_, err := db.Compact(ctx)
				return err
			})
			if db.store.Current != before {
				t.Fatalf("canceled Compact changed CURRENT from %+v to %+v", before, db.store.Current)
			}
			if _, err := os.Stat(unselected); err != nil {
				t.Fatalf("unselected file after canceled Compact: %v", err)
			}
			mustPut(t, db, "key", "retry")
			if _, err := db.Compact(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(unselected); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unselected file after successful Compact: %v", err)
			}
			wantValue(t, db, "key", "retry")
		})
	}
}

func TestCompactCanceledBetweenOutputFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var headers int
	fs := &storage.IO{Hook: func(point string) error {
		switch point {
		case "compact.segment.write:before":
			headers++
		case "compact.sync:after":
			cancel()
		}
		return nil
	}}
	// Each live record fits in a separate checkpoint output.
	opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: format.SegmentHeaderBytes + 64}
	db := newTestDBWithHooks(t, opts, fs, &Hooks{CleanerStart: make(chan struct{})})
	mustPut(t, db, "a", "one")
	mustPut(t, db, "b", "two")
	_, err := db.Compact(ctx)
	var op *OpError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &op) || op.Outcome != NotApplied || headers != 1 {
		t.Fatalf("Compact wrote %d snapshot headers and returned %v", headers, err)
	}
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	wantValue(t, db, "a", "one")
	wantValue(t, db, "b", "two")
}

func TestCompactReportsCheckpointPublicationOutcome(t *testing.T) {
	// With a nonempty active log the first CURRENT replacement seals it.
	// Only the second replacement selects the requested checkpoint.
	for _, tc := range []struct {
		name, point, phase string
		occurrence         int
		outcome            Outcome
		checkpointSelected bool
	}{
		{"seal data", "segment.sync:after", "rotation.segment", 1, NotApplied, false},
		{"seal before rename", "current.rename:before", "current.rename", 1, NotApplied, false},
		{"seal after rename", "current.rename:after", "current.rename", 1, NotApplied, false},
		{"seal directory sync", "current.dirsync:after", "current.dirsync", 1, NotApplied, false},
		{"close sealed file", "rotation.close:after", "rotation.close", 1, NotApplied, false},
		{"checkpoint preparation", "current.sync:after", "current.sync", 2, NotApplied, false},
		{"checkpoint before rename", "current.rename:before", "current.rename", 2, Unknown, false},
		{"checkpoint after rename", "current.rename:after", "current.rename", 2, Unknown, true},
		{"checkpoint directory sync", "current.dirsync:before", "current.dirsync", 2, Unknown, true},
		{"checkpoint remap", "compact.remap:after", "post-publication", 1, Committed, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			armed, calls := false, 0
			fs := &storage.IO{Hook: func(point string) error {
				if armed && point == tc.point {
					calls++
					if calls == tc.occurrence {
						return syscall.EIO
					}
				}
				return nil
			}}
			engineHooks := &Hooks{CleanerStart: make(chan struct{})}
			db := newTestDBWithHooks(t, testOptions(), fs, engineHooks)
			mustPut(t, db, "k", "acknowledged")
			armed = true
			_, err := db.Compact(t.Context())
			var op *OpError
			if calls != tc.occurrence || !errors.As(err, &op) || op.Operation != "compact" ||
				op.Phase != tc.phase || op.Outcome != tc.outcome || !errors.Is(err, syscall.EIO) ||
				errors.Is(err, ErrCommitUncertain) != (tc.outcome == Unknown) {
				t.Fatalf("fault %s on call %d: got %v; want %s (%s) with EIO", tc.point, calls, err, tc.phase, tc.outcome)
			}
			if _, err := db.Get([]byte("k")); !errors.Is(err, ErrUnhealthy) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("read after publication fault: %v", err)
			}
			if err := db.Close(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("close after publication fault: %v", err)
			}
			reopened, err := OpenWithHooks(db.store.Dir, testOptions(), nil, &Hooks{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			}()
			wantValue(t, reopened, "k", "acknowledged")
			wantCutoff := uint64(0)
			if tc.checkpointSelected {
				wantCutoff = 1
			}
			if reopened.store.Catalog.Cutoff != wantCutoff || reopened.store.Current.Sequence != 1 {
				t.Fatalf("recovered cutoff %d, sequence %d; want %d, 1", reopened.store.Catalog.Cutoff, reopened.store.Current.Sequence, wantCutoff)
			}
		})
	}
}

func TestCompactRetriesPendingCleanupSync(t *testing.T) {
	for _, tc := range []struct {
		name, point string
		occurrence  int
		removed     int64
	}{
		{"after first unlink", "cleanup.unlink:after", 1, 3},
		{"before last unlink", "cleanup.unlink:before", 2, 3},
		{"after last unlink", "cleanup.unlink:after", 2, 8},
		{"before directory sync", "cleanup.dirsync:before", 1, 8},
		{"after directory sync", "cleanup.dirsync:after", 1, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			armed, calls, syncs := false, 0, 0
			fs := &storage.IO{Hook: func(point string) error {
				if !armed {
					return nil
				}
				if point == "cleanup.dirsync:before" {
					syncs++
				}
				if point == tc.point {
					calls++
					if calls == tc.occurrence {
						return syscall.EIO
					}
				}
				return nil
			}}
			engineHooks := parkedMaintenance()
			db := newTestDBWithHooks(t, testOptions(), fs, engineHooks)
			mustPut(t, db, "k", "acknowledged")
			if _, err := db.Compact(t.Context()); err != nil {
				t.Fatal(err)
			}
			// Two unselected files left by an interrupted operation; their
			// names sort in ID order and their byte counts identify each unlink.
			for i, contents := range []string{"one", "three"} {
				name := storage.DataName(db.store.Current.UUID, uint64(100+i))
				if err := os.WriteFile(filepath.Join(db.store.Dir, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			armed = true
			result, err := db.Compact(t.Context())
			var op *OpError
			if !errors.As(err, &op) || op.Operation != "compact" || op.Phase != "cleanup" ||
				op.Outcome != Committed || !errors.Is(err, syscall.EIO) || result.UnlinkedBytes != tc.removed {
				t.Fatalf("interrupted cleanup: result %+v, error %v", result, err)
			}
			syncsBefore := syncs
			result, err = db.Compact(t.Context())
			if err != nil || result.UnlinkedBytes != 8-tc.removed || syncs != syncsBefore+1 {
				t.Fatalf("retry: result %+v, additional syncs %d, error %v", result, syncs-syncsBefore, err)
			}
			result, err = db.Compact(t.Context())
			if err != nil || result != (CompactResult{}) || syncs != syncsBefore+1 {
				t.Fatalf("clean checkpoint: result %+v, additional syncs %d, error %v", result, syncs-syncsBefore, err)
			}
			wantValue(t, db, "k", "acknowledged")
		})
	}
}

func TestCompactCanFinishCleanupAfterWorkerSyncFailure(t *testing.T) {
	manualSyncs, workerSyncs := 0, 0
	fs := &storage.IO{Hook: func(point string) error {
		switch point {
		case "cleanup.dirsync:before":
			manualSyncs++
			if manualSyncs == 1 {
				return syscall.EIO
			}
		case "clean.retire.dirsync:before":
			workerSyncs++
			return syscall.ENOSPC
		}
		return nil
	}}
	engineHooks := &Hooks{CleanerStart: make(chan struct{})}
	db := newTestDBWithHooks(t, testOptions(), fs, engineHooks)
	mustPut(t, db, "k", "acknowledged")
	first, err := db.Compact(t.Context())
	var op *OpError
	if !errors.As(err, &op) || op.Outcome != Committed || !errors.Is(err, syscall.EIO) || first.UnlinkedBytes == 0 {
		t.Fatalf("checkpoint cleanup: result %+v, error %v", first, err)
	}
	// Drive one worker pass while its background goroutine is paused.
	_, retry := db.cleanPass(t.Context())
	status := db.AutoCompactionStatus()
	if !retry || workerSyncs != 1 || !errors.Is(status.LastError, ErrNoSpace) || status.UnlinkedBytes != 0 {
		t.Fatalf("worker retry %t after %d syncs, status %+v", retry, workerSyncs, status)
	}
	result, err := db.Compact(t.Context())
	if err != nil || result != (CompactResult{}) || manualSyncs != 2 {
		t.Fatalf("manual retry: result %+v after %d syncs, error %v", result, manualSyncs, err)
	}
	_, retry = db.cleanPass(t.Context())
	if retry || workerSyncs != 1 {
		t.Fatalf("worker retried a completed sync: retry %t, syncs %d", retry, workerSyncs)
	}
	wantValue(t, db, "k", "acknowledged")
}

func TestCheckpointSyncsEveryCompletedSnapshotBeforePublication(t *testing.T) {
	for _, failFinalSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail-final-sync-%t", failFinalSync), func(t *testing.T) {
			var files, synced int
			var published bool
			fs := &storage.IO{Hook: func(point string) error {
				switch point {
				case "compact.segment.write:after":
					files++
				case "compact.sync:before":
					if failFinalSync && synced == 1 {
						return syscall.ENOSPC
					}
				case "compact.sync:after":
					synced++
				case "current.rename:before":
					if files != 0 {
						if synced != files {
							t.Fatalf("publishing %d snapshot files after %d full-file syncs", files, synced)
						}
						published = true
					}
				}
				return nil
			}}
			engineHooks := &Hooks{CleanerStart: make(chan struct{})}
			opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: 180}
			db := newTestDBWithHooks(t, opts, fs, engineHooks)
			for _, key := range []string{"a", "b", "c", "d"} {
				mustPut(t, db, key, "v")
			}
			_, err := db.Compact(t.Context())
			if files != 2 || published == failFinalSync ||
				(failFinalSync && !errors.Is(err, ErrNoSpace)) || (!failFinalSync && err != nil) {
				t.Fatalf("files=%d synced=%d published=%t err=%v", files, synced, published, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(db.store.Dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if (reopened.store.Catalog.Cutoff == 4) != published {
				t.Fatalf("cutoff after reopen %d, published %t", reopened.store.Catalog.Cutoff, published)
			}
			for _, key := range []string{"a", "b", "c", "d"} {
				wantValue(t, reopened, key, "v")
			}
		})
	}
}

func TestCheckpointSplitsAtSegmentTarget(t *testing.T) {
	// Two single-character keys with empty values fit exactly in each snapshot.
	opts := Options{MaxRecordBytes: 49, TargetSegmentBytes: format.SegmentHeaderBytes + 2*49}
	db := newTestDB(t, opts, nil)
	for _, key := range []string{"a", "b", "c", "d"} {
		mustPut(t, db, key, "")
	}
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.store.Catalog.Files) != 2 {
		t.Fatalf("expected two snapshot files, got %v", db.store.Catalog.Files)
	}
	for _, f := range db.store.Catalog.Files {
		if f.Kind != format.Snapshot || f.Bytes != opts.TargetSegmentBytes {
			t.Fatalf("unexpected snapshot size or kind: %+v", f)
		}
	}
}

func TestRepeatedCompactionDoesNotRetainOldFileRegistry(t *testing.T) {
	db := newTestDBWithHooks(t, testOptions(), nil, parkedMaintenance())
	for i := 0; i < 8; i++ {
		mustPut(t, db, "k", "value")
		if _, err := db.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.store.Files) != len(db.store.Catalog.Files)+1 {
		t.Fatalf("obsolete file metadata retained: %d", len(db.store.Files))
	}
}

func TestCompactionValidatesCapturedSegmentHeaders(t *testing.T) {
	for _, damage := range []string{"checksum", "uuid", "id", "kind"} {
		t.Run(damage, func(t *testing.T) {
			db := openMaintenanceDamageDB(t, testOptions(), nil)
			mustPut(t, db, "k", "acknowledged")
			file := db.store.Files[db.store.Current.ActiveID]
			header := format.Segment{UUID: db.store.Current.UUID, ID: file.ID, Kind: format.Log}
			switch damage {
			case "uuid":
				header.UUID[0] ^= 1
			case "id":
				header.ID++
			case "kind":
				header.Kind = format.Snapshot
			}
			b := format.EncodeSegment(header)
			if damage == "checksum" {
				b[60] ^= 1
			}
			fd, err := os.OpenFile(file.Path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := fd.WriteAt(b, 0)
			if err := errors.Join(writeErr, fd.Sync(), fd.Close()); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Compact(context.Background()); !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrDataUnavailable) || db.health() != nil {
				t.Fatalf("sealed header damage: %v, health %v", err, db.health())
			}
			wantValue(t, db, "k", "acknowledged") // Previously committed value from cache.
			mustPut(t, db, "unrelated", "healthy")
			if _, err := os.Stat(file.Path); err != nil {
				t.Fatalf("corrupt selected evidence removed: %v", err)
			}
		})
	}
}

func TestCleanupAfterFaultDoesNotRetainUnlinkedMetadata(t *testing.T) {
	var armed atomic.Bool
	db := newTestDBWithHooks(t, testOptions(), &storage.IO{Hook: func(p string) error {
		if p == "cleanup.unlink:after" && armed.CompareAndSwap(true, false) {
			return syscall.EIO
		}
		return nil
	}}, parkedMaintenance())
	mustPut(t, db, "k", "old")
	mustPut(t, db, "k", "new")
	old := db.store.Current.ActiveID
	armed.Store(true)
	if _, err := db.Compact(context.Background()); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if _, retained := db.store.Files[old]; retained {
		t.Fatal("successfully unlinked file retained after an after-hook fault")
	}
	wantValue(t, db, "k", "new")
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.store.Files) != len(db.store.Catalog.Files)+1 {
		t.Fatal("deferred cleanup retained file metadata")
	}
}

func TestCheckpointCopiesInSourceOrderAndReusesReaders(t *testing.T) {
	var opens, records int
	ioHooks := &storage.IO{Hook: func(point string) error {
		switch point {
		case "compact.source.open:after":
			opens++
		case "compact.record:after":
			records++
		}
		return nil
	}}
	db := newTestDBWithHooks(t, Options{MaxRecordBytes: 64, TargetSegmentBytes: 180}, ioHooks, &Hooks{CleanerStart: make(chan struct{})})
	// Two records per log. Both records in the first file are overwritten.
	for _, key := range []string{"a", "b", "c", "d", "e", "f", "a", "b"} {
		mustPut(t, db, key, "v")
	}
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	if opens != 3 || records != 6 {
		t.Fatalf("copied %d records with %d source opens; want six records from three logs", records, opens)
	}
	var previous storage.Location
	for _, key := range []string{"c", "d", "e", "f", "a", "b"} {
		loc := testLoc(t, db, key)
		if previous.File != nil && (loc.File.ID < previous.File.ID ||
			loc.File.ID == previous.File.ID && loc.Offset <= previous.Offset) {
			t.Fatalf("key %q copied out of source order", key)
		}
		previous = loc
	}
}

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
	if err != nil || result.UnlinkedBytes == 0 || db.store.Catalog.Cutoff != cutoff {
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

func TestCompactionConcurrentTailWins(t *testing.T) {
	opts := testOptions()
	opts.IndexMode = IndexMemory
	t.Run("memory", func(t *testing.T) { testCompactionConcurrentTail(t, opts) })
	opts.IndexMode = IndexBTree
	t.Run("btree", func(t *testing.T) { testCompactionConcurrentTail(t, opts) })
}

func TestMemoryCheckpointHonorsMutationsBetweenRelocationBatches(t *testing.T) {
	entries := make([]Entry, 180)
	for i := range entries {
		entries[i] = Entry{Key: []byte(fmt.Sprintf("key-%012d", i)), Value: []byte("old")}
	}
	opts := Options{IndexMode: IndexMemory}
	var source *storage.File
	var db *DB
	overwrite, remove := entries[len(entries)-2].Key, entries[len(entries)-1].Key
	firstBatch := int64(0)
	fs := &storage.IO{Hook: func(point string) error {
		if point != "compact.remap:after" || firstBatch != 0 {
			return nil
		}
		firstBatch = int64(len(entries)) - source.LiveKeys.Load()
		if firstBatch <= storage.RelocationBatchCount || firstBatch >= int64(len(entries)-2) {
			return fmt.Errorf("unexpected first memory relocation batch of %d", firstBatch)
		}
		for _, key := range [][]byte{overwrite, remove} {
			if loc, ok := testLookup(t, db, string(key)); !ok || loc.File != source {
				return fmt.Errorf("key %q was already relocated", key)
			}
		}
		if err := db.Put(overwrite, []byte("new")); err != nil {
			return err
		}
		return db.Delete(remove)
	}}
	db = newTestDBWithHooks(t, opts, fs, &Hooks{CleanerStart: make(chan struct{})})
	if err := db.BatchPut(entries); err != nil {
		t.Fatal(err)
	}
	loc, _ := testLookup(t, db, string(overwrite))
	source = loc.File
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	if firstBatch == 0 || source.LiveKeys.Load() != 0 || source.LiveBytes.Load() != 0 {
		t.Fatalf("first batch %d; source still holds %d keys and %d bytes", firstBatch, source.LiveKeys.Load(), source.LiveBytes.Load())
	}
	if _, err := os.Stat(source.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source after relocation: %v", err)
	}
	check := func(db *DB) {
		t.Helper()
		if db.keys.Count() != len(entries)-1 {
			t.Fatalf("unexpected live count %d", db.keys.Count())
		}
		wantValue(t, db, string(overwrite), "new")
		if _, err := db.Get(remove); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted key: %v", err)
		}
		wantValue(t, db, string(entries[0].Key), "old")
	}
	check(db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	check(reopened)
}

func testCompactionConcurrentTail(t *testing.T, opts Options) {
	t.Helper()
	var armed atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	ioHooks := &storage.IO{Hook: func(p string) error {
		// Header creation is staging I/O. A foreground writer must be able to
		// append and rotate while it is blocked.
		if p == "compact.segment.write:before" && armed.CompareAndSwap(true, false) {
			close(started)
			select {
			case <-release:
			case <-time.After(30 * time.Second):
				return errors.New("checkpoint header blocked foreground mutations")
			}
		}
		return nil
	}}
	db := newTestDBWithHooks(t, opts, ioHooks, &Hooks{CleanerStart: make(chan struct{})})
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
	if loc, ok := testLookup(t, db, "a"); !ok || loc.File.Kind != format.Log {
		t.Fatalf("concurrent update relocated into checkpoint: %+v", loc)
	}
	if _, exists := testLookup(t, db, "b"); exists {
		t.Fatal("concurrent delete was relocated into the live index")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, opts)
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

func TestMaintenanceCancellationAndNoSpace(t *testing.T) {
	for _, point := range []string{"backup.copy:after", "compact.scan:before", "compact.write:after"} {
		t.Run(point, func(t *testing.T) {
			var armed atomic.Bool
			ctx, cancel := context.WithCancel(context.Background())
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
			if strings.HasPrefix(point, "compact.") {
				_, err = db.Compact(ctx)
			} else {
				err = db.Backup(ctx, filepath.Join(t.TempDir(), "backup"))
			}
			var op *OpError
			if !errors.Is(err, context.Canceled) || errors.Is(err, ErrCorrupt) || !errors.As(err, &op) || op.Outcome != NotApplied {
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

func TestMaintenanceIsolatesOnlySealedSourceCorruption(t *testing.T) {
	for _, compact := range []bool{false, true} {
		t.Run(fmt.Sprint(compact), func(t *testing.T) {
			db := openMaintenanceDamageDB(t, testOptions(), nil)
			mustPut(t, db, "a", "cached")
			path := testLoc(t, db, "a").File.Path
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
			if compact {
				// Compact seals before reading and can authenticate the source.
				if !errors.Is(err, ErrDataUnavailable) || db.health() != nil {
					t.Fatalf("sealed read failure: %v, health %v", err, db.health())
				}
				mustPut(t, db, "unrelated", "healthy")
			} else if _, err := db.Get([]byte("a")); !errors.Is(err, ErrUnhealthy) {
				t.Fatalf("active source failure: %v", err)
			}
		})
	}
}

func TestCompactScratchFailureLeavesSourceHealthy(t *testing.T) {
	var armed atomic.Bool
	fs := &storage.IO{Hook: func(point string) error {
		if point == "index.moves.flush:before" && armed.CompareAndSwap(true, false) {
			return syscall.EIO
		}
		return nil
	}}
	db := newTestDBWithHooks(t, testOptions(), fs, &Hooks{CleanerStart: make(chan struct{})})
	mustPut(t, db, "k", "source")
	armed.Store(true)
	_, err := db.Compact(t.Context())
	var op *OpError
	if !errors.Is(err, syscall.EIO) || !errors.As(err, &op) || op.Outcome != NotApplied {
		t.Fatalf("scratch failure: %v", err)
	}
	wantValue(t, db, "k", "source")
	mustPut(t, db, "k", "retry")
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	wantValue(t, db, "k", "retry")
}

func TestMemoryCheckpointProgressesPastStagedMutations(t *testing.T) {
	for _, mode := range []string{"overwrite", "delete", "abort"} {
		t.Run(mode, func(t *testing.T) {
			entries := make([]Entry, 350)
			for i := range entries {
				entries[i] = Entry{Key: []byte(fmt.Sprintf("key-%012d", i)), Value: []byte("original")}
			}
			key := entries[len(entries)-1].Key
			staged, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			var db *DB
			var original storage.Location
			var firstRemaining int64
			started, progressed := false, false
			prepares := 0
			fs := &storage.IO{Hook: func(point string) error {
				switch point {
				case "data.sync:before", "index.prepare:before":
					if original.File == nil {
						return nil // The fixture is being written.
					}
					if point == "index.prepare:before" {
						if mode != "abort" {
							return nil
						}
						// Fail the second prepare while the first key is staged.
						prepares++
						if prepares != 2 {
							return nil
						}
					}
					close(staged)
					select {
					case <-release:
					case <-time.After(30 * time.Second):
						return errors.New("no relocation progress during durable write")
					}
					if mode == "abort" {
						return syscall.ENOSPC
					}
				case "compact.remap:after":
					if !started {
						started = true
						firstRemaining = original.File.LiveKeys.Load()
						go func() {
							if mode == "delete" {
								done <- db.Delete(key)
								return
							}
							done <- db.BatchPut([]Entry{{key, []byte("intermediate")}, {key, []byte("updated")}})
						}()
						select {
						case <-staged:
						case <-time.After(30 * time.Second):
							return errors.New("writer did not stage")
						}
					}
					if !progressed && original.File.LiveKeys.Load() < firstRemaining {
						// The writer is still paused. A disjoint batch moved, but
						// its pending key must take the writer-lock fallback.
						// This fixture is copied in source order into a single file.
						head, ok := testLookup(t, db, string(entries[0].Key))
						if !ok || head.File.Kind != format.Snapshot {
							return errors.New("first key did not reach the snapshot")
						}
						next := original
						next.File = head.File
						move := storage.Relocation{Key: string(key), Old: original, Next: next}
						if ok, err := db.relocateUnstaged([]storage.Relocation{move}); ok || err != nil {
							return fmt.Errorf("staged key remapped: %t %v", ok, err)
						}
						progressed = true
						close(release)
						err := <-done
						if mode == "abort" && !errors.Is(err, ErrNoSpace) || mode != "abort" && err != nil {
							return fmt.Errorf("writer outcome: %v", err)
						}
					}
				}
				return nil
			}}
			db = newTestDBWithHooks(t, Options{}, fs, &Hooks{CleanerStart: make(chan struct{})})
			if err := db.BatchPut(entries); err != nil {
				t.Fatal(err)
			}
			original, _ = testLookup(t, db, string(key))
			if _, err := db.Compact(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !progressed || original.File.LiveBytes.Load() != 0 || original.File.LiveKeys.Load() != 0 {
				t.Fatalf("progress=%v, original file retains %d keys", progressed, original.File.LiveKeys.Load())
			}
			if _, err := os.Stat(original.File.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("source retained: %v", err)
			}
			check := func(db *DB) {
				switch mode {
				case "overwrite":
					wantValue(t, db, string(key), "updated")
				case "abort":
					wantValue(t, db, string(key), "original")
				case "delete":
					if _, err := db.Get(key); !errors.Is(err, ErrNotFound) {
						t.Fatalf("deleted: %v", err)
					}
				}
			}
			check(db)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(db.store.Dir, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			check(reopened)
		})
	}
}

// Damage the last payload byte, after a complete sidecar is durable. Streaming
// maintenance may already have written private output before noticing this CRC.
func damageProtectedTail(t *testing.T, db *DB) format.File {
	t.Helper()
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	e := db.store.Catalog.Files[len(db.store.Catalog.Files)-1]
	fd, err := os.OpenFile(db.store.Files[e.ID].Path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := fd.ReadAt(b[:], e.Bytes-1); err != nil {
		fd.Close()
		t.Fatal(err)
	}
	b[0] ^= 1
	_, err = fd.WriteAt(b[:], e.Bytes-1)
	if err := errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	return e
}

func TestCompactRepairsProtectedSourceAndRestartsPrivateOutputs(t *testing.T) {
	db := newTestDBWithHooks(t, Options{}, nil, parkedMaintenance())
	mustPut(t, db, "original", "one")
	mustPut(t, db, "other", "two")
	cut := damageProtectedTail(t, db)
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	if db.store.Catalog.Cutoff != cut.Last || len(db.store.Catalog.Files) != 1 || db.store.Catalog.Files[0].Kind != format.Snapshot {
		t.Fatalf("checkpoint after repair: %+v", db.store.Catalog)
	}
	wantValue(t, db, "original", "one")
	wantValue(t, db, "other", "two")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	wantValue(t, reopened, "other", "two")
}

// Damage tests explicitly verify the expected maintenance and Close errors.
func openMaintenanceDamageDB(t *testing.T, opts Options, fs *storage.IO) *DB {
	t.Helper()
	db, err := OpenWithHooks(t.TempDir(), opts, fs, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMaintenanceKeepsDamagedDataAndLeavesWritersAvailable(t *testing.T) {
	for _, op := range []string{"backup", "compact", "index", "clean"} {
		t.Run(op, func(t *testing.T) {
			db := openMaintenanceDamageDB(t, Options{IndexMode: IndexBTree}, nil)
			mustPut(t, db, "intact", "healthy")
			mustPut(t, db, "unavailable", strings.Repeat("v", 10000))
			entry := damageProtectedTail(t, db)
			file := db.store.Files[entry.ID]
			raw, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			raw[300] ^= 1 // A second bad block in the same stripe.
			if err := os.WriteFile(file.Path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			before := db.store.Current
			switch op {
			case "backup":
				dest := filepath.Join(t.TempDir(), "backup")
				err = db.Backup(t.Context(), dest)
				if _, statErr := os.Stat(dest); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("incomplete backup was published: %v", statErr)
				}
			case "compact":
				_, err = db.Compact(t.Context())
			case "index":
				err = db.CheckpointIndex(t.Context())
			case "clean":
				err = db.cleanSegment(t.Context(), entry)
			}
			var local *storage.UnavailableDataError
			if !errors.As(err, &local) || local.FileID != entry.ID || !errors.Is(err, ErrDataUnavailable) || db.health() != nil {
				t.Fatalf("expected local damage to file %d: %v, health %v", entry.ID, err, db.health())
			}
			if db.store.Current != before {
				t.Fatal("incomplete maintenance published a frontier")
			}
			if data, err := os.ReadFile(file.Path); err != nil || !bytes.Equal(data, raw) {
				t.Fatalf("damaged source lost: %v", err)
			}
			mustPut(t, db, "new", "acknowledged")
			wantValue(t, db, "new", "acknowledged")
			if err := db.Close(); !errors.Is(err, ErrDataUnavailable) || errors.Is(err, ErrUnhealthy) {
				t.Fatalf("close with selected damage: %v", err)
			}
		})
	}
}
