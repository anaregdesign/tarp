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

func sealForCleaning(t *testing.T, db *DB) format.File {
	t.Helper()
	db.writer.Lock()
	defer db.writer.Unlock()
	if err := db.sealActive(); err != nil {
		t.Fatal(err)
	}
	return db.store.Catalog.Files[len(db.store.Catalog.Files)-1]
}

func TestSegmentCleaningRebasesOnConcurrentTailAndHonorsCapture(t *testing.T) {
	ctx := context.Background()
	paused, resume := make(chan struct{}), make(chan struct{})
	fs := &storage.IO{CleanerStart: make(chan struct{}), Hook: func(p string) error {
		if p == "clean.scan:after" {
			close(paused)
			<-resume
		}
		return nil
	}}
	db := newTestDB(t, Options{}, fs)
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
	clean := db.store.Catalog.Files[0]
	if clean.Kind != format.Cleaned || clean.First != source.First || clean.Last != source.Last ||
		db.store.Catalog.PrunedThrough != 9 || db.store.Current.Sequence != 9 || db.store.Catalog.Files[1] != tail {
		t.Fatalf("lost range or tail: %+v current=%+v", db.store.Catalog, db.store.Current)
	}
	wantValue(t, db, "keep", "stable")
	wantValue(t, db, "del", "reinserted")
	wantValue(t, db, "update", "new")
	if db.index["keep"].File.ID != clean.ID || db.index["update"].File.ID == clean.ID ||
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
	if err := Restore(ctx, backupDir, restoreDir, Options{}); err != nil {
		t.Fatal(err)
	}
	restored, err := OpenWithIO(restoreDir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.store.Current.Sequence != 6 {
		t.Fatalf("backup lost its cut: %d", restored.store.Current.Sequence)
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
	reopened, err := OpenWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
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
	if reopened.store.Current.Sequence != 10 {
		t.Fatal("maintenance advanced user sequence")
	}
}

func TestUncertainCleanerStopsQueuedWriterBeforeUnlock(t *testing.T) {
	for _, point := range []string{"clean.current.rename:before", "clean.current.rename:after", "clean.current.dirsync:before", "clean.current.dirsync:after"} {
		t.Run(point, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			fs := &storage.IO{CleanerStart: make(chan struct{}), Hook: func(p string) error {
				if p == point {
					close(entered)
					<-release
					return syscall.EIO
				}
				return nil
			}}
			db := newTestDB(t, Options{}, fs)
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
			recovered, err := OpenWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
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
			if recovered.store.Current.Sequence != 4 {
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
			fs := &storage.IO{CleanerStart: make(chan struct{}), Hook: func(p string) error {
				if p == point && armed.CompareAndSwap(true, false) {
					return syscall.EDQUOT
				}
				return nil
			}}
			db := newTestDB(t, Options{}, fs)
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

func TestCleanerValidatesDiscardedBytesWithoutOpeningUnrelatedSegments(t *testing.T) {
	db := newTestDB(t, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
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
	if e := db.store.Catalog.Files[1]; e.ID != 0 || e.Kind != format.Cleaned || e.First != source.First || e.Last != source.Last {
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
	if err := db.cleanSegment(context.Background(), corrupt); !errors.Is(err, ErrCorrupt) || !errors.Is(db.health(), ErrUnhealthy) {
		t.Fatalf("discarded corruption went unnoticed: %v", err)
	}
}

func TestCleaningSnapshotAndSparseFileRepeatedlyPreservesDeletes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
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
	if err := db.cleanSegment(ctx, db.store.Catalog.Files[0]); err != nil {
		t.Fatal(err)
	}
	snapshot := db.store.Catalog.Files[0]
	if snapshot.Kind != format.Snapshot || db.index["stable"].File.ID != snapshot.ID {
		t.Fatal("snapshot was not replaced by a snapshot")
	}
	if err := db.cleanSegment(ctx, log); err != nil {
		t.Fatal(err)
	}
	sparse := db.store.Catalog.Files[1]
	if err := db.Delete([]byte("later")); err != nil {
		t.Fatal(err)
	}
	if err := db.cleanSegment(ctx, sparse); err != nil {
		t.Fatal(err)
	}
	sparse = db.store.Catalog.Files[1]
	if sparse.First != log.First || sparse.Last != log.Last || sparse.Kind != format.Cleaned ||
		sparse.ID == 0 || db.store.Files[sparse.ID].DeleteBytes.Load() != sparse.Bytes-format.SegmentHeaderBytes {
		t.Fatalf("second cleaning lost required delete or covered interval %+v", sparse)
	}
	mustPut(t, db, "stable", "later")
	if err := db.cleanSegment(ctx, snapshot); err != nil {
		t.Fatal(err)
	}
	if db.store.Catalog.Files[0] != sparse {
		t.Fatalf("fully discarded snapshot was not removed: %+v", db.store.Catalog.Files)
	}
	if _, err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	dir := db.store.Dir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := OpenWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
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
	if got.store.Current.Sequence != 9 {
		t.Fatal("incorrect committed sequence", got.store.Current.Sequence)
	}
}

// A failed foreground commit may have changed CURRENT while the unselected
// cleaner manifest was syncing. Discard only the cleaner's private output.
func TestCleanCatalogPreparationPreservesForegroundOutcome(t *testing.T) {
	for _, mode := range []string{"append", "cancel", "uncertain foreground"} {
		t.Run(mode, func(t *testing.T) {
			ready, release := make(chan struct{}), make(chan struct{})
			var armed, published atomic.Bool
			fs := &storage.IO{CleanerStart: make(chan struct{}), Hook: func(point string) error {
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
			db := newTestDB(t, Options{}, fs)
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
			reopened, err := OpenWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			// The injected after-rename failure completed the rename on this filesystem.
			wantValue(t, reopened, "key", "new")
			if reopened.store.Current.Sequence != 2 {
				t.Fatalf("reopened sequence %d", reopened.store.Current.Sequence)
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
			db := newTestDB(t, Options{}, &storage.IO{CleanerStart: make(chan struct{}), Hook: func(point string) error {
				if point == tc.barrier && staged.CompareAndSwap(false, true) {
					close(copied)
					<-resume
				}
				if point == tc.fault {
					return syscall.EIO
				}
				return nil
			}})
			mustPut(t, db, "prefix", "earlier")
			prefix := sealForCleaning(t, db)
			var group []format.File
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
			var tail format.File
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
				if _, ok := db.index["deleted"]; ok || db.index["updated"].File.ID != tail.ID {
					t.Fatal("repacking overwrote newer index locations")
				}
			}
			dir := db.store.Dir
			if err := db.Close(); err != nil && !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			image, err := OpenWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			files := image.store.Catalog.Files
			if len(files) < 3 || files[0] != prefix || files[len(files)-1] != tail || image.store.Current.Sequence != 8 {
				t.Fatalf("lost prefix or acknowledged tail: %+v, sequence %d", files, image.store.Current.Sequence)
			}
			middle := files[1 : len(files)-1]
			if tc.published {
				if len(middle) != 1 || middle[0].Kind != format.Cleaned || middle[0].First != group[0].First || middle[0].Last != group[len(group)-1].Last {
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
