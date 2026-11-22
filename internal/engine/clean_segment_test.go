package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func sealForCleaning(t *testing.T, db *DB) format.CatalogEntry {
	t.Helper()
	db.writer.Lock()
	defer db.writer.Unlock()
	if err := db.sealActive(); err != nil {
		t.Fatal(err)
	}
	return db.store.Catalog.Entries[len(db.store.Catalog.Entries)-1]
}

func TestSegmentCleaningRebasesOnConcurrentTailAndHonorsCapture(t *testing.T) {
	opts := Options{}
	opts.IndexMode = IndexMemory
	t.Run("memory", func(t *testing.T) { testSegmentCleaningRebasesOnConcurrentTail(t, opts) })
	opts.IndexMode = IndexBTree
	t.Run("btree", func(t *testing.T) { testSegmentCleaningRebasesOnConcurrentTail(t, opts) })
}

func testSegmentCleaningRebasesOnConcurrentTail(t *testing.T, opts Options) {
	t.Helper()
	ctx := context.Background()
	paused, resume := make(chan struct{}), make(chan struct{})
	fs := &storage.IO{Hook: func(p string) error {
		if p == "clean.scan:after" {
			close(paused)
			<-resume
		}
		return nil
	}}
	engineHooks := parkedMaintenance()
	db := newTestDBWithHooks(t, opts, fs, engineHooks)
	mustPut(t, db, "raw", "old")
	mustPut(t, db, "del", "old")
	if err := db.Delete([]byte("del")); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "update", "old")
	mustPut(t, db, "keep", "stable")
	source := sealForCleaning(t, db)
	old := db.store.Files[source.ID]
	if err := db.Delete([]byte("raw")); err != nil {
		t.Fatal(err)
	}
	capture, err := db.store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- db.cleanSegment(ctx, source) }()
	select {
	case <-paused:
	case err := <-done:
		t.Fatalf("copy failed before barrier: %v", err)
	}
	mustPut(t, db, "update", "new")
	mustPut(t, db, "del", "reinserted")
	tail := sealForCleaning(t, db)
	mustPut(t, db, "tail", "after rotation")
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	clean := db.store.Catalog.Entries[0]
	if clean.Kind != format.CleanedLogKind || clean.FirstSequence != source.FirstSequence || clean.LastSequence != source.LastSequence ||
		db.store.Catalog.PrunedThroughSequence != 9 || db.store.Current.LastUserSequence != 9 || db.store.Catalog.Entries[1] != tail {
		t.Fatalf("lost range or tail: %+v current=%+v", db.store.Catalog, db.store.Current)
	}
	wantValue(t, db, "keep", "stable")
	wantValue(t, db, "del", "reinserted")
	wantValue(t, db, "update", "new")
	if testLoc(t, db, "keep").File.ID != clean.ID || testLoc(t, db, "update").File.ID == clean.ID ||
		db.store.Files[clean.ID].DeleteBytes.Load() == 0 || old.LiveBytes.Load() != 0 {
		t.Fatal("stale remap or missing accounting")
	}
	if _, pinned, _, err := db.store.RetireCleaned(); err != nil || pinned != source.Bytes {
		t.Fatalf("capture lost its old file: pinned %d err %v", pinned, err)
	}
	backupDir := filepath.Join(t.TempDir(), "backup")
	if err := storage.Backup(ctx, capture, db.store.Config, db.store.IO, backupDir, nil); err != nil {
		t.Fatal(err)
	}
	capture.Release()
	n, pinned, pending, err := db.store.RetireCleaned()
	if err != nil || n != source.Bytes || pinned != 0 || pending {
		t.Fatalf("source reclaim: %d pinned=%d pending=%t err=%v", n, pinned, pending, err)
	}
	if _, err := os.Stat(old.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source still exists: %v", err)
	}
	if n, _, _, err := db.store.RetireCleaned(); err != nil || n != 0 {
		t.Fatalf("counted a retired source twice: %d %v", n, err)
	}
	restoreDir := filepath.Join(t.TempDir(), "restore")
	if err := Restore(ctx, RestoreRequest{Source: backupDir, Destination: restoreDir}); err != nil {
		t.Fatal(err)
	}
	restored, err := OpenWithHooks(restoreDir, opts, nil, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.store.Current.LastUserSequence != 6 {
		t.Fatalf("backup lost its cut: %d", restored.store.Current.LastUserSequence)
	}
	wantValue(t, restored, "update", "old")
	for _, key := range []string{"raw", "del", "tail"} {
		if _, err := restored.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unexpected key %q in capture: %v", key, err)
		}
	}
	dir := db.store.Dir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWithHooks(dir, opts, nil, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	wantValue(t, reopened, "update", "new")
	wantValue(t, reopened, "tail", "after rotation")
	if _, err := reopened.Get([]byte("raw")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("raw tombstone lost its predecessor: %v", err)
	}
	if _, err := reopened.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	mustPut(t, reopened, "next", "")
	if reopened.store.Current.LastUserSequence != 10 {
		t.Fatal("maintenance advanced user sequence")
	}
}

func TestUncertainCleanerStopsQueuedWriterBeforeUnlock(t *testing.T) {
	for _, point := range []string{"clean.current.rename:before", "clean.current.rename:after", "clean.current.dirsync:before", "clean.current.dirsync:after"} {
		t.Run(point, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			fs := &storage.IO{Hook: func(p string) error {
				if p == point {
					close(entered)
					<-release
					return syscall.EIO
				}
				return nil
			}}
			engineHooks := &Hooks{CleanerStart: make(chan struct{})}
			db := newTestDBWithHooks(t, Options{}, fs, engineHooks)
			mustPut(t, db, "old", "obsolete")
			mustPut(t, db, "keep", "retained")
			source := sealForCleaning(t, db)
			mustPut(t, db, "old", "acknowledged")
			dir := db.store.Dir
			before, err := filepath.Glob(filepath.Join(dir, "DATA-*"))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- db.cleanSegment(context.Background(), source) }()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("publication fault unreachable: %v", err)
			}
			queued := make(chan error, 1)
			go func() { queued <- db.Put([]byte("queued"), []byte("forbidden")) }()
			deadline := time.Now().Add(5 * time.Second)
			for db.cleaner.writers.Load() == 0 && time.Now().Before(deadline) {
				runtime.Gosched()
			}
			waiting := db.cleaner.writers.Load() > 0
			close(release)
			if err := <-done; !errors.Is(err, ErrCommitUncertain) || !waiting {
				t.Fatalf("writer was queued=%t, cleaner outcome=%v", waiting, err)
			}
			if err := <-queued; !errors.Is(err, ErrUnhealthy) {
				t.Fatalf("stale frontier available to queued writer: %v", err)
			}
			after, _ := filepath.Glob(filepath.Join(dir, "DATA-*"))
			if len(after) != len(before)+1 {
				t.Fatalf("possible layouts not preserved: before %d after %d", len(before), len(after))
			}
			if err := db.Close(); !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			recovered, err := OpenWithHooks(dir, Options{}, nil, &Hooks{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			wantValue(t, recovered, "old", "acknowledged")
			wantValue(t, recovered, "keep", "retained")
			if _, err := recovered.Get([]byte("queued")); !errors.Is(err, ErrNotFound) {
				t.Fatal("queued write was published", err)
			}
			mustPut(t, recovered, "next", "")
			if recovered.store.Current.LastUserSequence != 4 {
				t.Fatal("incorrect next sequence")
			}
		})
	}
}

func TestCleanStagingFailuresLeaveSelectionAndContentsIntact(t *testing.T) {
	for _, point := range []string{"clean.copy:after", "clean.output.sync:after", "clean.catalog.write:after",
		"clean.catalog.sync:after", "clean.catalog.dirsync:after", "clean.current.sync:after"} {
		t.Run(point, func(t *testing.T) {
			var armed atomic.Bool
			fs := &storage.IO{Hook: func(p string) error {
				if p == point && armed.CompareAndSwap(true, false) {
					return syscall.EDQUOT
				}
				return nil
			}}
			engineHooks := parkedMaintenance()
			db := newTestDBWithHooks(t, Options{}, fs, engineHooks)
			mustPut(t, db, "obsoleted", "old")
			mustPut(t, db, "live", "value")
			entry := sealForCleaning(t, db)
			mustPut(t, db, "obsoleted", "new")
			before := directoryBytes(t, db.store.Dir)
			armed.Store(true)
			err := db.cleanSegment(context.Background(), entry)
			if !errors.Is(err, ErrNoSpace) || db.health() != nil {
				t.Fatalf("unexpected staging failure: %v; health %v", err, db.health())
			}
			if !reflect.DeepEqual(before, directoryBytes(t, db.store.Dir)) {
				t.Fatal("staging failure changed the selected files or leaked an output")
			}
			if err := db.cleanSegment(context.Background(), entry); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCleanerRetriesFailedStagingDiscard(t *testing.T) {
	for _, point := range []string{"clean.discard:before", "clean.discard:after", "clean.discard.dirsync:before"} {
		t.Run(point, func(t *testing.T) {
			var armed bool
			var retireSyncs int
			fs := &storage.IO{Hook: func(p string) error {
				if armed {
					switch p {
					case "clean.copy:after":
						return syscall.EDQUOT
					case point:
						return syscall.EACCES
					case "clean.retire.dirsync:before":
						retireSyncs++
						if retireSyncs == 1 {
							return syscall.EIO
						}
					}
				}
				return nil
			}}
			db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
			mustPut(t, db, "live", "value")
			entry := sealForCleaning(t, db)
			db.cleanPass(t.Context()) // Retire the old manifest before injecting faults.
			before := directoryBytes(t, db.store.Dir)
			armed = true
			err := db.cleanSegment(t.Context(), entry)
			if !errors.Is(err, ErrNoSpace) || !errors.Is(err, syscall.EACCES) || db.health() != nil {
				t.Fatalf("failed staging discard: %v; health %v", err, db.health())
			}
			if point == "clean.discard:before" && len(directoryBytes(t, db.store.Dir)) != len(before)+1 {
				t.Fatal("expected an unpublished file awaiting deletion")
			}

			// No segment qualifies for cleaning. An idle worker must still
			// finish the unlink and retry its directory sync after a failure.
			if _, retry := db.cleanPass(t.Context()); !retry || retireSyncs != 1 {
				t.Fatalf("discard sync failure: retry=%t syncs=%d", retry, retireSyncs)
			}
			if again, retry := db.cleanPass(t.Context()); again || retry || retireSyncs != 2 {
				t.Fatalf("discard completion: again=%t retry=%t syncs=%d", again, retry, retireSyncs)
			}
			if !reflect.DeepEqual(before, directoryBytes(t, db.store.Dir)) {
				t.Fatal("retry changed selected files or left an unpublished output")
			}
			wantValue(t, db, "live", "value")
		})
	}
}

func TestCleanerValidatesDiscardedBytesWithoutOpeningUnrelatedSegments(t *testing.T) {
	db := openMaintenanceDamageDB(t, Options{}, nil)
	mustPut(t, db, "unrelated", "keep")
	unrelated := sealForCleaning(t, db)
	mustPut(t, db, "target", "obsolete")
	source := sealForCleaning(t, db)
	mustPut(t, db, "target", "new")
	hidden := db.store.Files[unrelated.ID].Path
	if err := os.Rename(hidden, hidden+".hold"); err != nil {
		t.Fatal(err)
	}
	if err := db.cleanSegment(context.Background(), source); err != nil {
		t.Fatal("cleaner read unrelated data", err)
	}
	if err := os.Rename(hidden+".hold", hidden); err != nil {
		t.Fatal(err)
	}
	if e := db.store.Catalog.Entries[1]; e.ID != 0 || e.Kind != format.CleanedLogKind || e.FirstSequence != source.FirstSequence || e.LastSequence != source.LastSequence {
		t.Fatalf("invalid empty coverage %+v", e)
	}
	mustPut(t, db, "corrupt", "obsolete")
	corrupt := sealForCleaning(t, db)
	mustPut(t, db, "corrupt", "replacement")
	path := db.store.Files[corrupt.ID].Path
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	i := bytes.LastIndex(b, []byte("obsolete"))
	if i < 0 {
		t.Fatal("corruption fixture is invalid")
	}
	b[i] ^= 1
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err := db.cleanSegment(context.Background(), corrupt); !errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrDataUnavailable) || db.health() != nil {
		t.Fatalf("unrecoverable source: %v, health %v", err, db.health())
	}
	wantValue(t, db, "corrupt", "replacement")
}

func TestCleaningSnapshotAndSparseFileRepeatedlyPreservesDeletes(t *testing.T) {
	ctx := context.Background()
	db := newTestDBWithHooks(t, Options{}, nil, &Hooks{CleanerStart: make(chan struct{})})
	mustPut(t, db, "stable", "checkpoint")
	mustPut(t, db, "deleted", "checkpoint")
	if _, err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "later", "initial")
	mustPut(t, db, "obsolete", "initial")
	log := sealForCleaning(t, db)
	mustPut(t, db, "obsolete", "updated")
	if err := db.cleanSegment(ctx, db.store.Catalog.Entries[0]); err != nil {
		t.Fatal(err)
	}
	snapshot := db.store.Catalog.Entries[0]
	if snapshot.Kind != format.SnapshotKind || testLoc(t, db, "stable").File.ID != snapshot.ID {
		t.Fatal("snapshot was not replaced by a snapshot")
	}
	if err := db.cleanSegment(ctx, log); err != nil {
		t.Fatal(err)
	}
	sparse := db.store.Catalog.Entries[1]
	if err := db.Delete([]byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := db.cleanSegment(ctx, sparse); err != nil {
		t.Fatal(err)
	}
	sparse = db.store.Catalog.Entries[1]
	if sparse.FirstSequence != log.FirstSequence || sparse.LastSequence != log.LastSequence || sparse.Kind != format.CleanedLogKind ||
		sparse.ID == 0 || db.store.Files[sparse.ID].DeleteBytes.Load() != sparse.Bytes-format.SegmentHeaderBytes {
		t.Fatalf("second cleaning lost required delete or covered interval %+v", sparse)
	}
	mustPut(t, db, "stable", "later")
	if err := db.cleanSegment(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if db.store.Catalog.Entries[0] != sparse {
		t.Fatalf("fully discarded snapshot was not removed: %+v", db.store.Catalog.Entries)
	}
	if _, err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	dir := db.store.Dir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := OpenWithHooks(dir, Options{}, nil, &Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Close()
	wantValue(t, got, "stable", "later")
	wantValue(t, got, "obsolete", "updated")
	for _, k := range []string{"deleted", "later"} {
		if _, err := got.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%q resurrected after repeated cleaning: %v", k, err)
		}
	}
	mustPut(t, got, "next", "")
	if got.store.Current.LastUserSequence != 9 {
		t.Fatal("incorrect committed sequence", got.store.Current.LastUserSequence)
	}
}

// A failed foreground commit may have changed CURRENT while the unselected
// cleaner manifest was syncing. Discard only the cleaner's private output.
func TestCleanCatalogPreparationPreservesForegroundOutcome(t *testing.T) {
	for _, mode := range []string{"append", "cancel", "uncertain foreground"} {
		t.Run(mode, func(t *testing.T) {
			ready, release := make(chan struct{}), make(chan struct{})
			var armed, published atomic.Bool
			fs := &storage.IO{Hook: func(point string) error {
				switch point {
				case "clean.catalog.dirsync:after":
					close(ready)
					<-release
				case "current.rename:after":
					if armed.CompareAndSwap(true, false) {
						return syscall.EIO
					}
				case "clean.current.write:before":
					published.Store(true)
				}
				return nil
			}}
			engineHooks := parkedMaintenance()
			db := newTestDBWithHooks(t, Options{}, fs, engineHooks)
			mustPut(t, db, "key", "old")
			source := sealForCleaning(t, db)
			dir := db.store.Dir
			before := directoryBytes(t, dir)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- db.cleanSegment(ctx, source) }()
			select {
			case <-ready:
			case err := <-done:
				t.Fatalf("catalog not staged: %v", err)
			}
			armed.Store(mode == "uncertain foreground")
			put := make(chan error, 1)
			go func() { put <- db.Put([]byte("key"), []byte("new")) }()
			select {
			case err := <-put:
				if errors.Is(err, ErrCommitUncertain) != (mode == "uncertain foreground") ||
					err != nil && mode != "uncertain foreground" {
					close(release)
					<-done
					t.Fatalf("foreground Put: %v", err)
				}
			case <-time.After(5 * time.Second):
				close(release)
				<-done
				t.Fatal("catalog staging held the writer gate")
			}
			if mode == "cancel" {
				cancel()
			}
			close(release)
			err := <-done
			if mode == "append" {
				if err != nil || !published.Load() {
					t.Fatalf("clean publication: %v", err)
				}
			} else {
				want := error(context.Canceled)
				if mode == "uncertain foreground" {
					want = ErrUnhealthy
				}
				if !errors.Is(err, want) || published.Load() {
					t.Fatalf("unexpected cleaner publication: %v", err)
				}
				for name := range directoryBytes(t, dir) {
					if _, existed := before[name]; !existed {
						t.Fatalf("unpublished staging file retained: %s", name)
					}
				}
			}
			if err := db.Close(); err != nil && !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			reopened, err := OpenWithHooks(dir, Options{}, nil, parkedMaintenance())
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			// The injected after-rename failure completed the rename on this filesystem.
			wantValue(t, reopened, "key", "new")
			if reopened.store.Current.LastUserSequence != 2 {
				t.Fatalf("reopened sequence %d", reopened.store.Current.LastUserSequence)
			}
		})
	}
}

func TestRepackPublishesWholeGroupAndPreservesConcurrentTail(t *testing.T) {
	for _, tc := range []struct {
		name, barrier, fault string
		published            bool
	}{
		{"success", "clean.output.sync:after", "", true},
		{"rotation during catalog preparation", "clean.catalog.dirsync:after", "", true},
		{"before rename", "clean.output.sync:after", "clean.current.rename:before", false},
		{"after rename", "clean.output.sync:after", "clean.current.rename:after", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var staged atomic.Bool
			copied, resume := make(chan struct{}), make(chan struct{})
			ioHooks := &storage.IO{Hook: func(point string) error {
				if point == tc.barrier && staged.CompareAndSwap(false, true) {
					close(copied)
					<-resume
				}
				if point == tc.fault {
					return syscall.EIO
				}
				return nil
			}}
			// Check the repacked catalog across Close with the final append still active.
			db := newTestDBWithHooks(t, Options{IndexMode: IndexMemory}, ioHooks, &Hooks{CleanerStart: make(chan struct{})})
			mustPut(t, db, "prefix", "earlier")
			prefix := sealForCleaning(t, db)
			var group []format.CatalogEntry
			for _, key := range []string{"first", "updated", "deleted", "last"} {
				mustPut(t, db, key, "original")
				group = append(group, sealForCleaning(t, db))
			}
			done := make(chan error, 1)
			go func() { done <- db.cleanSegment(t.Context(), group...) }()
			select {
			case <-copied:
			case err := <-done:
				t.Fatalf("copy ended before publication barrier: %v", err)
			}
			// Every source has been copied. Commit a new sealed tail and an
			// active append before allowing the worker to install the group.
			var tail format.CatalogEntry
			func() {
				defer close(resume)
				mustPut(t, db, "updated", "new")
				if err := db.Delete([]byte("deleted")); err != nil {
					t.Fatal(err)
				}
				tail = sealForCleaning(t, db)
				mustPut(t, db, "active", "later")
			}()
			err := <-done
			if tc.fault != "" {
				if !errors.Is(err, ErrCommitUncertain) || !errors.Is(db.health(), ErrUnhealthy) {
					t.Fatalf("unexpected publication outcome: %v, health: %v", err, db.health())
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := testLookup(t, db, "deleted"); ok || testLoc(t, db, "updated").File.ID != tail.ID {
					t.Fatal("repacking overwrote newer index locations")
				}
			}
			dir := db.store.Dir
			if err := db.Close(); err != nil && !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			image, err := OpenWithHooks(dir, Options{}, nil, &Hooks{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			files := image.store.Catalog.Entries
			if len(files) < 3 || files[0] != prefix || files[len(files)-1] != tail || image.store.Current.LastUserSequence != 8 {
				t.Fatalf("lost prefix or acknowledged tail: %+v, sequence %d", files, image.store.Current.LastUserSequence)
			}
			middle := files[1 : len(files)-1]
			if tc.published {
				if len(middle) != 1 || middle[0].Kind != format.CleanedLogKind || middle[0].FirstSequence != group[0].FirstSequence || middle[0].LastSequence != group[len(group)-1].LastSequence {
					t.Fatalf("incomplete group replacement: %+v", middle)
				}
			} else if !slices.Equal(middle, group) {
				t.Fatalf("original group changed before rename: %+v", middle)
			}
			wantValue(t, image, "prefix", "earlier")
			wantValue(t, image, "first", "original")
			wantValue(t, image, "last", "original")
			wantValue(t, image, "updated", "new")
			wantValue(t, image, "active", "later")
			if _, err := image.Get([]byte("deleted")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("repacked deleted key: %v", err)
			}
		})
	}
}

func TestCleanSegmentRepairsGroupAndRestartsTombstoneAccounting(t *testing.T) {
	db := newTestDBWithHooks(t, Options{}, nil, parkedMaintenance())
	mustPut(t, db, "obsolete", "old")
	if err := db.Delete([]byte("obsolete")); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "kept", "first")
	first := sealForCleaning(t, db)
	mustPut(t, db, "next", "second")
	second := damageProtectedTail(t, db)
	tombstones := db.store.Files[first.ID].DeleteBytes.Load()
	if err := db.cleanSegment(t.Context(), first, second); err != nil {
		t.Fatal(err)
	}
	files := db.store.Catalog.Entries
	if len(files) != 1 || files[0].Kind != format.CleanedLogKind || files[0].FirstSequence != first.FirstSequence ||
		files[0].LastSequence != second.LastSequence || db.store.Files[files[0].ID].DeleteBytes.Load() != tombstones {
		t.Fatalf("replacement after repair: %+v", files)
	}
	wantValue(t, db, "kept", "first")
	wantValue(t, db, "next", "second")
	if _, err := db.Get([]byte("obsolete")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key after cleaning: %v", err)
	}
}

func TestCleanRepairsSnapshotKeyCollisionBeforePayloadCheck(t *testing.T) {
	var verifies atomic.Int32
	fs := &storage.IO{Hook: func(point string) error {
		if point == "data-repair.verify:before" {
			verifies.Add(1)
		}
		return nil
	}}
	db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
	mustPut(t, db, "a", "first")
	mustPut(t, db, "b", "second")
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	a, b := testLoc(t, db, "a"), testLoc(t, db, "b")
	source := db.store.Catalog.Entries[0]
	if source.Kind != format.SnapshotKind || a.File.ID != source.ID || b.File != a.File || a.RecordOffset >= b.RecordOffset {
		t.Fatal("expected two ordered records in one snapshot")
	}
	fd, err := os.OpenFile(b.File.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fd.WriteAt([]byte("a"), b.RecordOffset+format.RecordHeaderBytes)
	if err := errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	if err := db.cleanSegment(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	if got := db.store.Catalog.Entries; len(got) != 1 || got[0].ID == source.ID || got[0].Kind != format.SnapshotKind || verifies.Load() != 1 {
		t.Fatalf("replacement after repair: %+v, repairs %d", got, verifies.Load())
	}
	wantValue(t, db, "a", "first")
	wantValue(t, db, "b", "second")
}

func TestCompactionKeepsSelectedPairOnReplacementParityFailure(t *testing.T) {
	for _, operation := range []string{"compact", "clean"} {
		for _, phase := range []string{"write:after", "rename:after", "dirsync:before"} {
			t.Run(operation+"/"+phase, func(t *testing.T) {
				var armed atomic.Bool
				fs := &storage.IO{Hook: func(p string) error {
					if p == operation+".parity."+phase && armed.Swap(false) {
						return syscall.ENOSPC
					}
					return nil
				}}
				db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
				mustPut(t, db, "keep", "value")
				if err := db.ProtectParity(t.Context()); err != nil {
					t.Fatal(err)
				}
				before := db.store.Current
				entry := db.store.Catalog.Entries[0]
				oldParity := selectedParity(t, db, entry)
				run := func() error {
					if operation == "clean" {
						return db.cleanSegment(t.Context(), entry)
					}
					_, err := db.Compact(t.Context())
					return err
				}
				armed.Store(true)
				var op *OpError
				if err := run(); !errors.As(err, &op) || op.Outcome != NotApplied || !errors.Is(err, ErrNoSpace) {
					t.Fatalf("failed parity publication: %v", err)
				}
				files, err := filepath.Glob(filepath.Join(db.store.Dir, "PARITY-*"))
				if err != nil || len(files) != 1 || files[0] != oldParity || db.store.Current != before || db.health() != nil {
					t.Fatalf("selected DATA or PARITY changed: %v (%v), frontier %+v", files, err, db.store.Current)
				}
				wantValue(t, db, "keep", "value")
				if err := run(); err != nil {
					t.Fatal(err)
				}
				replacement := db.store.Catalog.Entries[0]
				if replacement.ID == entry.ID {
					t.Fatal("retry did not publish a replacement")
				}
				sidecar, err := os.Open(selectedParity(t, db, replacement))
				if err != nil {
					t.Fatal(err)
				}
				if err := errors.Join(format.ValidateParitySidecar(t.Context(), sidecar, before.UUID, replacement), sidecar.Close()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
