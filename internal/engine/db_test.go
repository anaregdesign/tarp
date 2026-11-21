package engine

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func testOptions() Options {
	return Options{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 128, TargetSegmentBytes: 256, ReadFileHandles: 2}
}

// parkedMaintenance isolates manual storage operations and their fault hooks
// from the asynchronous cleaner and parity builder.
func parkedMaintenance() *Hooks {
	gate := make(chan struct{})
	return &Hooks{CleanerStart: gate, ParityStart: gate}
}

func newTestDB(t *testing.T, opts Options, fs *storage.IO) *DB {
	t.Helper()
	return newTestDBWithHooks(t, opts, fs, nil)
}

func newTestDBWithHooks(t *testing.T, opts Options, fs *storage.IO, hooks *Hooks) *DB {
	t.Helper()
	db, err := OpenWithHooks(filepath.Join(t.TempDir(), "db"), opts, fs, hooks)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil && db.failed.Load() == nil {
			t.Errorf("close: %v", err)
		}
	})
	return db
}

func mustPut(t *testing.T, db *DB, key, value string) {
	t.Helper()
	if err := db.Put([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func wantValue(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get([]byte(key))
	if err != nil || string(got) != want {
		t.Fatalf("get %q: %q, %v; want %q", key, got, err, want)
	}
}

func directoryBytes(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]string)
	for _, e := range entries {
		if e.Name() == "LOCK" {
			continue
		}
		if e.IsDir() && e.Name() == ".tarp-work" {
			// Maintenance creates this directory lazily. A completed or
			// discarded operation must leave no named private files in it.
			files, err := os.ReadDir(filepath.Join(dir, e.Name()))
			if err != nil || len(files) != 0 {
				t.Fatalf("unexpected private workspace contents: %v, %v", files, err)
			}
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		result[e.Name()] = string(b)
	}
	return result
}

func TestPersistenceOwnershipAndRotation(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	key, value := []byte{0, 0xff}, []byte("first")
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}
	key[0], value[0] = 9, 'X'
	got, err := db.Get([]byte{0, 0xff})
	if err != nil || string(got) != "first" {
		t.Fatalf("ownership: %q %v", got, err)
	}
	got[0] = 'Y'
	for i := 0; i < 12; i++ {
		mustPut(t, db, fmt.Sprintf("k%d", i), strings.Repeat("x", 20))
	}
	mustPut(t, db, "empty", "")
	if err := db.Delete([]byte("k0")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("k0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing delete: %v", err)
	}
	if _, err := Open(db.store.Dir, testOptions()); !errors.Is(err, ErrLocked) {
		t.Fatalf("duplicate owner: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	binary, err := reopened.Get([]byte{0, 0xff})
	if err != nil || string(binary) != "first" {
		t.Fatalf("recovered ownership: %q %v", binary, err)
	}
	wantValue(t, reopened, "empty", "")
	if _, err := reopened.Get([]byte("k0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key recovered: %v", err)
	}
	for i := 1; i < 12; i++ {
		wantValue(t, reopened, fmt.Sprintf("k%d", i), strings.Repeat("x", 20))
	}
}

func TestQueuedPutDefersOwnedInput(t *testing.T) {
	var copied atomic.Int64
	db := newTestDBWithHooks(t, testOptions(), nil, &Hooks{PutCopied: func(k, v int) {
		copied.Add(int64(k + v))
	}})
	db.writer.Lock()
	key, value := []byte("key"), []byte("value")
	done := make(chan error, 1)
	go func() { done <- db.Put(key, value) }()
	deadline := time.Now().Add(5 * time.Second)
	for db.cleaner.writers.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	waiters, queued := db.cleaner.writers.Load(), copied.Load()
	db.writer.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if waiters != 1 || queued != 0 || copied.Load() != int64(len(key)+len(value)) {
		t.Fatalf("waiters=%d, bytes copied while queued=%d, after Put=%d", waiters, queued, copied.Load())
	}
	key[0], value[0] = 'X', 'X'
	wantValue(t, db, "key", "value")
}

func TestOversizedPutIsPreMutation(t *testing.T) {
	for _, opts := range []Options{
		{ValueCacheBudgetBytes: 140, MaxRecordBytes: 128, ReadFileHandles: 1},
		{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 60, ReadFileHandles: 1},
	} {
		db := newTestDB(t, opts, nil)
		mustPut(t, db, "a", "old")
		before := directoryBytes(t, db.store.Dir)
		sequence := db.store.Current.Sequence
		for _, k := range []string{"a", "new"} {
			if err := db.Put([]byte(k), bytes.Repeat([]byte{1}, 100)); !errors.Is(err, ErrTooLarge) {
				t.Fatalf("size: %v", err)
			}
		}
		if !reflect.DeepEqual(before, directoryBytes(t, db.store.Dir)) || db.store.Current.Sequence != sequence {
			t.Fatal("oversized Put mutated source files")
		}
		wantValue(t, db, "a", "old")
		for _, call := range []func() error{
			func() error { return db.Put(nil, nil) },
			func() error { return db.Delete(nil) },
			func() error { _, err := db.Get(nil); return err },
		} {
			if err := call(); !errors.Is(err, ErrEmptyKey) {
				t.Fatalf("empty key: %v", err)
			}
		}
	}
}

func TestMissCannotRefillStaleVersion(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprint(deleted), func(t *testing.T) {
			var armed atomic.Bool
			started, release := make(chan struct{}), make(chan struct{})
			fs := &storage.IO{Hook: func(point string) error {
				if point == "read:after" && armed.CompareAndSwap(true, false) {
					close(started)
					<-release
				}
				return nil
			}}
			opts := testOptions()
			opts.ValueCacheBudgetBytes = 140
			db := newTestDB(t, opts, fs)
			mustPut(t, db, "k", "old")
			mustPut(t, db, "x", "evict")
			armed.Store(true)
			result := make(chan struct {
				value []byte
				err   error
			}, 1)
			go func() {
				v, err := db.Get([]byte("k"))
				result <- struct {
					value []byte
					err   error
				}{v, err}
			}()
			<-started
			if deleted {
				if err := db.Delete([]byte("k")); err != nil {
					t.Fatal(err)
				}
			} else {
				mustPut(t, db, "k", "new")
			}
			close(release)
			got := <-result
			if deleted && !errors.Is(got.err, ErrNotFound) || !deleted && (got.err != nil || string(got.value) != "new") {
				t.Fatalf("racing miss: %q %v", got.value, got.err)
			}
			s := db.Stats()
			if s.ValueCache.Hits != 0 || s.ValueCache.Misses != 1 || s.MissingKeyLookups != 0 {
				t.Fatalf("retry should count only the initial miss: %+v", s)
			}
		})
	}
}

func TestExactCacheIntegration(t *testing.T) {
	var reads atomic.Int64
	opts := testOptions()
	opts.ValueCacheBudgetBytes = 2 * (128 + 1 + 1)
	db := newTestDB(t, opts, &storage.IO{Hook: func(p string) error {
		if p == "read:before" {
			reads.Add(1)
		}
		return nil
	}})
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	wantValue(t, db, "a", "A")
	if _, err := db.Get([]byte("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	before := directoryBytes(t, db.store.Dir)
	if err := db.Put([]byte("b"), bytes.Repeat([]byte{1}, 200)); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, directoryBytes(t, db.store.Dir)) {
		t.Fatal("oversized update mutated files")
	}
	mustPut(t, db, "c", "C")
	wantValue(t, db, "a", "A")
	if reads.Load() != 0 {
		t.Fatal("hit performed disk read, or size rejection changed LRU")
	}
	wantValue(t, db, "b", "B")
	if reads.Load() != 1 {
		t.Fatal("wrong LRU victim")
	}
	s := db.Stats()
	if s.ValueCache.Hits != 2 || s.ValueCache.Misses != 1 || s.MissingKeyLookups != 1 || s.ValueCache.Evictions != 2 {
		t.Fatalf("unexpected lookup/eviction counts: %+v", s)
	}
}

func TestCommitFaultOutcomesAndFailStop(t *testing.T) {
	for _, tc := range []struct {
		point   string
		outcome Outcome
		applied bool // The hook tells us which frontier this process can reopen.
	}{
		{"data.write:before", NotApplied, false}, {"data.write:after", NotApplied, false},
		{"data.sync:before", NotApplied, false}, {"data.sync:after", NotApplied, false},
		{"current.write:before", NotApplied, false}, {"current.write:after", NotApplied, false},
		{"current.sync:before", NotApplied, false}, {"current.sync:after", NotApplied, false},
		{"current.rename:before", Unknown, false}, {"current.rename:after", Unknown, true},
		{"current.dirsync:before", Unknown, true}, {"current.dirsync:after", Unknown, true},
	} {
		for _, operation := range []string{"put", "delete"} {
			t.Run(operation+"/"+tc.point, func(t *testing.T) {
				var armed atomic.Bool
				db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
					if p == tc.point && armed.CompareAndSwap(true, false) {
						return syscall.ENOSPC
					}
					return nil
				}})
				mustPut(t, db, "k", "old")
				armed.Store(true)
				var err error
				if operation == "put" {
					err = db.Put([]byte("k"), []byte("new"))
				} else {
					err = db.Delete([]byte("k"))
				}
				var op *OpError
				phase, _, _ := strings.Cut(tc.point, ":")
				if !errors.As(err, &op) || op.Operation != operation || op.Phase != phase || op.Outcome != tc.outcome ||
					!errors.Is(err, ErrNoSpace) || !errors.Is(err, syscall.ENOSPC) || errors.Is(err, ErrCommitUncertain) != (tc.outcome == Unknown) {
					t.Fatalf("got %v, want %s at %s with outcome %s and ENOSPC", err, operation, phase, tc.outcome)
				}
				if _, err := db.Get([]byte("k")); !errors.Is(err, ErrUnhealthy) || !errors.Is(err, syscall.ENOSPC) {
					t.Fatalf("cached read after failure: %v", err)
				}
				if err := db.Close(); !errors.Is(err, syscall.ENOSPC) {
					t.Fatalf("close lost cause: %v", err)
				}
				reopened, err := Open(db.store.Dir, testOptions())
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := reopened.Close(); err != nil {
						t.Error(err)
					}
				}()
				switch {
				case !tc.applied:
					wantValue(t, reopened, "k", "old")
				case operation == "put":
					wantValue(t, reopened, "k", "new")
				default:
					if _, err := reopened.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
						t.Fatalf("Get after committed Delete: %v", err)
					}
				}
			})
		}
	}
}

func TestPartialWriteAndCommittedCorruption(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "k", "old")
	once := false
	db.store.IO = &storage.IO{WriteAt: func(f *os.File, b []byte, off int64) (int, error) {
		if !once {
			once = true
			n, err := f.WriteAt(b[:len(b)/2], off)
			return n, errors.Join(err, io.ErrUnexpectedEOF)
		}
		return f.WriteAt(b, off)
	}}
	if err := db.Put([]byte("k"), []byte("new")); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	db2, err := Open(db.store.Dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, db2, "k", "old")
	active := testLoc(t, db2, "k").File.Path
	end := db2.store.Current.ActiveEnd
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(active, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(func() error { _, e := f.WriteAt([]byte{0xff}, end-1); return e }(), f.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(db.store.Dir, testOptions()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("silently skipped corrupt final committed record: %v", err)
	}
}

func TestReopenCompatibilityBeforeTailCleanup(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "k", strings.Repeat("v", 50))
	path, active := db.store.Dir, testLoc(t, db, "k").File.Path
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(active, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("uncommitted")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	before := directoryBytes(t, path)
	opts := testOptions()
	opts.ValueCacheBudgetBytes = 150
	if _, err := Open(path, opts); !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatalf("smaller cache: %v", err)
	}
	if !reflect.DeepEqual(before, directoryBytes(t, path)) {
		t.Fatal("incompatible Open changed files")
	}
	reopened, err := Open(path, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	mustPut(t, reopened, "x", "")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	opts = testOptions()
	opts.MaxRecordBytes = 50
	reopened, err = Open(path, opts)
	if err != nil {
		t.Fatalf("obsolete oversized record should remain decodable: %v", err)
	}
	wantValue(t, reopened, "x", "")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWaitsAndRepeats(t *testing.T) {
	var armed atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
		if p == "data.sync:before" && armed.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return nil
	}})
	armed.Store(true)
	put := make(chan error, 1)
	go func() { put <- db.Put([]byte("k"), nil) }()
	<-started
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		db.life.Lock()
		closing := db.closing
		db.life.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close did not begin")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	select {
	case err := <-closed:
		t.Fatalf("closed before admitted Put finished: %v", err)
	default:
	}
	close(release)
	if err := <-put; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Go(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestUnsupportedAndMissingFrontier(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "k", "v")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	current := filepath.Join(db.store.Dir, "CURRENT")
	b, err := os.ReadFile(current)
	if err != nil {
		t.Fatal(err)
	}
	copy(b, "FOREIGN!")
	if err := os.WriteFile(current, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(db.store.Dir, testOptions()); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("unsupported: %v", err)
	}
	if err := os.Remove(current); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(db.store.Dir, testOptions()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing frontier: %v", err)
	}
}

func TestConcurrentReadFailureBeforeFrontierLeavesPutUnapplied(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target int64
		point  string
	}{
		{"append", 256, "data.sync:after"},
		{"rotation", 192, "segment.sync:after"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synced, release := make(chan struct{}), make(chan struct{})
			armed := false
			fs := &storage.IO{Hook: func(p string) error {
				if armed && p == tc.point {
					close(synced)
					<-release
				}
				if p == "read:before" {
					return syscall.EIO
				}
				return nil
			}}
			engineHooks := &Hooks{CleanerStart: make(chan struct{})}
			opts := Options{ValueCacheBudgetBytes: 140, MaxRecordBytes: 64, TargetSegmentBytes: tc.target}
			db := newTestDBWithHooks(t, opts, fs, engineHooks)
			mustPut(t, db, "cold", "old")
			mustPut(t, db, "key", "old") // Evicts "cold", forcing Get to read the segment.
			before := db.store.Current
			armed = true
			done := make(chan error, 1)
			go func() { done <- db.Put([]byte("key"), []byte("new")) }()
			select {
			case <-synced:
			case err := <-done:
				t.Fatalf("Put returned before %s was reached: %v", tc.point, err)
			}
			_, readErr := db.Get([]byte("cold"))
			close(release)
			writeErr := <-done
			if !errors.Is(readErr, syscall.EIO) {
				t.Fatalf("cold Get returned %v, want EIO", readErr)
			}
			var op *OpError
			if !errors.As(writeErr, &op) || op.Operation != "put" || op.Phase != "commit.health" || op.Outcome != NotApplied ||
				!errors.Is(writeErr, ErrUnhealthy) || !errors.Is(writeErr, syscall.EIO) || errors.Is(writeErr, ErrCommitUncertain) {
				t.Fatalf("Put after failed Get: %v", writeErr)
			}
			if err := db.Close(); !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			reopened, err := OpenWithHooks(db.store.Dir, opts, nil, &Hooks{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if after := reopened.store.Current; after != before {
				t.Fatalf("unapplied Put changed frontier: %+v -> %+v", before, after)
			}
			wantValue(t, reopened, "key", "old")
			wantValue(t, reopened, "cold", "old")
		})
	}
}

func TestCloseRetainsBothReadAndCloseFailures(t *testing.T) {
	opts := Options{ValueCacheBudgetBytes: 140}
	db := newTestDB(t, opts, &storage.IO{Hook: func(p string) error {
		switch p {
		case "read:before":
			return syscall.EIO
		case "close.active:before":
			return syscall.ENOSPC
		}
		return nil
	}})
	mustPut(t, db, "cold", "old")
	mustPut(t, db, "key", "old")
	if _, err := db.Get([]byte("cold")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("cold Get: %v", err)
	}
	const callers = 4
	start, results := make(chan struct{}), make(chan error, callers)
	for range callers {
		go func() {
			<-start
			results <- db.Close()
		}()
	}
	close(start)
	first := <-results
	if !errors.Is(first, syscall.EIO) || !errors.Is(first, syscall.ENOSPC) || !errors.Is(first, ErrNoSpace) {
		t.Fatalf("Close lost cause: %v", first)
	}
	for i := 1; i < callers; i++ {
		if got := <-results; got != first {
			t.Fatalf("Close returned a different result: %v", got)
		}
	}
	if _, err := db.Get([]byte("key")); !errors.Is(err, ErrClosed) || errors.Is(err, ErrUnhealthy) {
		t.Fatalf("Get after Close: %v", err)
	}
	reopened, err := Open(db.store.Dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	wantValue(t, reopened, "key", "old")
	wantValue(t, reopened, "cold", "old")
}

func TestOpenValidatesSelectedFilesBeforeCleanup(t *testing.T) {
	opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: 128}
	engineHooks := &Hooks{CleanerStart: make(chan struct{})}
	db := newTestDBWithHooks(t, opts, nil, engineHooks)
	mustPut(t, db, "sealed", "first")
	mustPut(t, db, "active", "second")
	frontier := db.store.Current
	sealed := filepath.Base(testLoc(t, db, "sealed").File.Path)
	active := storage.DataName(frontier.UUID, frontier.ActiveID)
	if sealed == active {
		t.Fatal("fixture did not seal the first segment")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	original := directoryBytes(t, db.store.Dir)
	orphan := storage.DataName(frontier.UUID, frontier.ManifestID+100)
	tail := format.EncodeRecord(format.Put, frontier.Sequence+1, []byte("future"), []byte("uncommitted"))

	for _, damage := range []string{"missing sealed file", "short sealed file", "extended sealed file", "sealed checksum", "unknown file"} {
		t.Run(damage, func(t *testing.T) {
			files := maps.Clone(original)
			files[active] += string(tail)
			files[orphan] = "unselected staging"
			switch damage {
			case "missing sealed file":
				delete(files, sealed)
			case "short sealed file":
				files[sealed] = files[sealed][:len(files[sealed])-1]
			case "extended sealed file":
				files[sealed] += "x"
			case "sealed checksum":
				b := []byte(files[sealed])
				b[len(b)-1] ^= 1
				files[sealed] = string(b)
			case "unknown file":
				files["unexpected"] = "preserve"
			}
			dir := t.TempDir()
			writeFixtureFile(t, filepath.Join(dir, "LOCK"), nil)
			for name, b := range files {
				writeFixtureFile(t, filepath.Join(dir, name), []byte(b))
			}
			rejected, err := OpenWithHooks(dir, opts, nil, engineHooks)
			if rejected != nil {
				rejected.Close()
				t.Fatal("opened corrupt fixture")
			}
			if !errors.Is(err, ErrCorrupt) || !maps.Equal(directoryBytes(t, dir), files) {
				t.Fatalf("Open must preserve all files on corruption: %v", err)
			}

			// Repair only the defect. The next Open should now perform both
			// cleanup operations without admitting the valid but uncommitted record.
			if damage == "unknown file" {
				if err := os.Remove(filepath.Join(dir, "unexpected")); err != nil {
					t.Fatal(err)
				}
			} else {
				writeFixtureFile(t, filepath.Join(dir, sealed), []byte(original[sealed]))
			}
			reopened, err := OpenWithHooks(dir, opts, nil, engineHooks)
			if err != nil {
				t.Fatalf("retry after repair: %v", err)
			}
			t.Cleanup(func() {
				if err := reopened.Close(); err != nil {
					t.Error(err)
				}
			})
			wantValue(t, reopened, "sealed", "first")
			wantValue(t, reopened, "active", "second")
			if _, err := reopened.Get([]byte("future")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("uncommitted key after recovery: %v", err)
			}
			after := directoryBytes(t, dir)
			if _, exists := after[orphan]; exists || after[active] != original[active] || reopened.store.Current != frontier {
				t.Fatal("successful recovery did not remove only the uncommitted suffix and orphaned files")
			}
		})
	}
}

func TestWriteAndSyncErrorFamilies(t *testing.T) {
	for _, cause := range []error{syscall.EIO, syscall.ENOSPC, syscall.EDQUOT} {
		for _, point := range []string{"data.write:before", "data.sync:before"} {
			t.Run(cause.Error()+"/"+point, func(t *testing.T) {
				var armed atomic.Bool
				db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
					if p == point && armed.CompareAndSwap(true, false) {
						return cause
					}
					return nil
				}})
				mustPut(t, db, "old", "acknowledged")
				armed.Store(true)
				err := db.Put([]byte("next"), []byte("value"))
				if !errors.Is(err, cause) || errors.Is(err, ErrNoSpace) != (cause != syscall.EIO) {
					t.Fatalf("error classification: %v", err)
				}
				if _, err := db.Get([]byte("old")); !errors.Is(err, ErrUnhealthy) {
					t.Fatal(err)
				}
				if err := db.Close(); !errors.Is(err, cause) {
					t.Fatal(err)
				}
				reopened, err := Open(db.store.Dir, testOptions())
				if err != nil {
					t.Fatal(err)
				}
				wantValue(t, reopened, "old", "acknowledged")
				if _, err := reopened.Get([]byte("next")); !errors.Is(err, ErrNotFound) {
					t.Fatal(err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestInitializationFailureNeverInventsAHistory(t *testing.T) {
	for _, point := range []string{"init.parent.sync:after", "lock.sync:after", "segment.write:before", "segment.sync:after", "catalog.write:after", "catalog.sync:after", "catalog.dirsync:after", "current.write:after", "current.sync:after", "current.rename:after", "current.dirsync:after"} {
		t.Run(point, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			_, err := OpenWithIO(dir, testOptions(), &storage.IO{Hook: func(p string) error {
				if p == point {
					return syscall.EIO
				}
				return nil
			}})
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("initialization phase not exercised: %v", err)
			}
			db, err := Open(dir, testOptions())
			if err != nil {
				if !errors.Is(err, ErrCorrupt) {
					t.Fatal(err)
				}
				return
			}
			if db.keys.Count() != 0 {
				t.Fatal("invented initial state")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExistingEmptyDirectoryStillRequiresParentSync(t *testing.T) {
	var synced bool
	db, err := OpenWithIO(t.TempDir(), testOptions(), &storage.IO{Hook: func(point string) error {
		if point == "init.parent.sync:after" {
			synced = true
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !synced {
		t.Fatal("successful initialization omitted durability of the existing empty directory entry")
	}
}

func TestGenerationExhaustionCannotWrapDuringRotation(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		t.Run(fmt.Sprint(rotate), func(t *testing.T) {
			db := newTestDBWithHooks(t, testOptions(), nil, &Hooks{CleanerStart: make(chan struct{})})
			mustPut(t, db, "k", "old")
			if rotate {
				mustPut(t, db, "pad", string(make([]byte, 60)))
			}
			activeID := db.store.Current.ActiveID
			db.store.Current.Generation = math.MaxUint64 - 1
			err := db.Put([]byte("k"), []byte("new"))
			if err != nil {
				t.Fatalf("rotation=%t: %v", rotate, err)
			}
			current, readErr := os.ReadFile(filepath.Join(db.store.Dir, "CURRENT"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			frontier, decodeErr := format.DecodeFrontier(current)
			if decodeErr != nil || frontier.Generation != math.MaxUint64 || (frontier.ActiveID != activeID) != rotate {
				t.Fatalf("invalid last publication: %+v %v", frontier, decodeErr)
			}
			if err := db.Put([]byte("k"), []byte("never")); err == nil {
				t.Fatal("exhausted generation accepted")
			}
			wantValue(t, db, "k", "new")
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(db.store.Dir, testOptions())
			if err != nil {
				t.Fatal(err)
			}
			wantValue(t, reopened, "k", "new")
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenFinishesNewDatabasePublicationWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	storageIO := &storage.IO{Hook: func(point string) error {
		if point == "current.rename:after" {
			cancel()
		}
		return nil
	}}
	dir := filepath.Join(t.TempDir(), "new")
	db, err := openDB(ctx, dir, testOptions(), false, storageIO, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("did not cancel at publication")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	retried, err := OpenContext(t.Context(), dir, testOptions(), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := retried.Close(); err != nil {
		t.Fatal(err)
	}
}

// The large value needs more chunk checks than the open preflight. Cancel
// during its replay deterministically, before the uncommitted tail is repaired.
type cancelDuringOpen struct {
	context.Context
	cancel    context.CancelFunc
	remaining int
}

func (c *cancelDuringOpen) Err() error {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestOpenCancellationDuringReplayAllowsRetry(t *testing.T) {
	opts := Options{}
	db := newTestDB(t, opts, nil)
	value := bytes.Repeat([]byte("v"), 512<<10)
	if err := db.Put([]byte("key"), value); err != nil {
		t.Fatal(err)
	}
	dir, active := db.store.Dir, testLoc(t, db, "key").File.Path
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fd, err := os.OpenFile(active, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fd.WriteString("uncommitted")
	if err := errors.Join(writeErr, fd.Close()); err != nil {
		t.Fatal(err)
	}
	before := directoryBytes(t, dir)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	replay := &cancelDuringOpen{Context: ctx, cancel: cancel, remaining: 12}
	if failed, err := OpenContext(replay, dir, opts, true); failed != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled recovery: %v, %v", failed, err)
	}
	if !maps.Equal(directoryBytes(t, dir), before) {
		t.Fatal("repaired files before replay completed")
	}
	retried, err := OpenContext(t.Context(), dir, opts, true)
	if err != nil {
		t.Fatal(err)
	}
	defer retried.Close()
	if got, err := retried.Get([]byte("key")); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("retry lost committed data: %d bytes, %v", len(got), err)
	}
}

func TestOpenCancellationDuringCleanupSyncsUnlinksAndAllowsRetry(t *testing.T) {
	opts := testOptions()
	db := newTestDB(t, opts, nil)
	mustPut(t, db, "k", "v")
	dir, frontier := db.store.Dir, db.store.Current
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{100, 101} {
		name := storage.DataName(frontier.UUID, id)
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var unlinked, synced int
	storageIO := &storage.IO{Hook: func(point string) error {
		switch point {
		case "cleanup.unlink:after":
			unlinked++
			cancel()
		case "cleanup.dirsync:after":
			synced++
		}
		return nil
	}}
	if failed, err := openDB(ctx, dir, opts, true, storageIO, nil); failed != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup: %v, %v", failed, err)
	}
	if unlinked != 1 || synced != 1 {
		t.Fatalf("cleanup completed %d unlinks, %d directory syncs", unlinked, synced)
	}
	retried, err := OpenContext(t.Context(), dir, opts, true)
	if err != nil {
		t.Fatal(err)
	}
	defer retried.Close()
	wantValue(t, retried, "k", "v")
}

func TestRecoveryBoundaryFailureReleasesOwnershipAndPreservesAcknowledgment(t *testing.T) {
	for _, point := range []string{"recover.truncate:before", "recover.truncate:after", "recover.sync:before", "recover.sync:after"} {
		for _, cause := range []error{syscall.EIO, syscall.ENOSPC, syscall.EDQUOT} {
			t.Run(point+"/"+cause.Error(), func(t *testing.T) {
				db := newTestDB(t, testOptions(), nil)
				mustPut(t, db, "k", "acknowledged")
				dir, path, end := db.store.Dir, testLoc(t, db, "k").File.Path, db.store.Current.ActiveEnd
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				fd, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				tail := []byte("incomplete-tail")
				n, writeErr := fd.Write(tail)
				if err := errors.Join(writeErr, fd.Sync(), fd.Close()); err != nil || n != len(tail) {
					t.Fatalf("seed tail: %d %v", n, err)
				}
				reached := 0
				failed, err := OpenWithIO(dir, testOptions(), &storage.IO{Hook: func(p string) error {
					if p == point {
						reached++
						return cause
					}
					return nil
				}})
				if failed != nil || reached != 1 || !errors.Is(err, cause) {
					t.Fatalf("failed recovery did not preserve its cause/phase: db=%v reached=%d err=%v", failed, reached, err)
				}
				stat, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				expected := end
				if point == "recover.truncate:before" {
					expected += int64(len(tail))
				}
				if stat.Size() != expected {
					t.Fatalf("truncation at %s: size=%d want=%d", point, stat.Size(), expected)
				}
				reopened, err := Open(dir, testOptions())
				if err != nil {
					t.Fatalf("failed Open retained ownership or prevented safe retry: %v", err)
				}
				wantValue(t, reopened, "k", "acknowledged")
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
				stat, err = os.Stat(path)
				if err != nil || stat.Size() != end {
					t.Fatalf("retry did not finish safe suffix cleanup: %v", err)
				}
			})
		}
	}
}

func TestPartialPayloadWritePreservesAcknowledgedValue(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "k", "acknowledged")
	reached := false
	db.store.IO = &storage.IO{WriteAt: func(fd *os.File, data []byte, offset int64) (int, error) {
		if !reached {
			reached = true
			count := format.RecordHeaderBytes + len("k") + 3
			if count >= len(data) {
				t.Fatal("fixture must tear within the value payload")
			}
			n, err := fd.WriteAt(data[:count], offset)
			return n, errors.Join(err, fd.Sync(), syscall.EIO)
		}
		return fd.WriteAt(data, offset)
	}}
	if err := db.Put([]byte("k"), []byte("partially-written-payload")); !reached || !errors.Is(err, syscall.EIO) {
		t.Fatalf("partial-payload failure not reached: %v", err)
	}
	if err := db.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, reopened, "k", "acknowledged")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

// This opaque backup was produced by commit 1eb703808a595d15df603553bb2f873dedb39396.
// The new engine treats the unsupported outer signatures as opaque as well.
//
//go:embed testdata/previous-release/*
var previousReleaseFS embed.FS

func TestPreviousReleaseRejectionPreservesInput(t *testing.T) {
	// The full suite also runs as a standalone binary inside qualification guests.
	fixture, err := fs.Sub(previousReleaseFS, "testdata/previous-release")
	if err != nil {
		t.Fatal(err)
	}
	previous := filepath.Join(t.TempDir(), "backup")
	if err := os.CopyFS(previous, fixture); err != nil {
		t.Fatal(err)
	}
	for _, lock := range []bool{false, true} {
		dir := filepath.Join(t.TempDir(), "old-db")
		if err := os.CopyFS(dir, os.DirFS(previous)); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, "BACKUP")); err != nil {
			t.Fatal(err)
		}
		if lock {
			if err := os.WriteFile(filepath.Join(dir, "LOCK"), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		files, err := filepath.Glob(filepath.Join(dir, "DATA-*"))
		if err != nil || len(files) != 1 {
			t.Fatalf("opaque fixture: %v, %v", files, err)
		}
		f, err := os.OpenFile(files[0], os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("uncommitted suffix")
		if err := errors.Join(err, f.Close()); err != nil {
			t.Fatal(err)
		}
		before := directoryBytes(t, dir)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		// A second rejection also checks release of ownership on failure.
		for i := 0; i < 2; i++ {
			if _, err := Open(dir, Options{}); !errors.Is(err, ErrUnsupportedFormat) {
				t.Fatalf("lock=%t open=%d: %v", lock, i, err)
			}
		}
		after, err := os.ReadDir(dir)
		if err != nil || len(after) != len(entries) || !reflect.DeepEqual(directoryBytes(t, dir), before) {
			t.Fatalf("opening a foreign directory changed it: %v", err)
		}
	}
	before := directoryBytes(t, previous)
	target := filepath.Join(t.TempDir(), "restored")
	if err := Restore(context.Background(), RestoreRequest{
		Source:      previous,
		Destination: target,
	}); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("old backup: %v", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) || !reflect.DeepEqual(directoryBytes(t, previous), before) {
		t.Fatalf("restore changed its source or published a directory: %v", err)
	}
}

func TestMissSurvivesRelocationAndRetirement(t *testing.T) {
	var armed atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	opts := testOptions()
	opts.ValueCacheBudgetBytes = 140
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
	if err := Restore(context.Background(), RestoreRequest{
		Source:      backup,
		Destination: target,
		Limits:      smaller.RestoreLimits(),
	}); err != nil {
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
			fs := &storage.IO{Hook: func(p string) error {
				if armed && p == tc.point {
					return syscall.EIO
				}
				return nil
			}}
			engineHooks := parkedMaintenance()
			opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: 128}
			db := newTestDBWithHooks(t, opts, fs, engineHooks)
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
			image, err := OpenWithHooks(dir, opts, nil, parkedMaintenance())
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
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]bool{
				"CURRENT": true,
				"LOCK":    true,
				storage.ManifestName(frontier.UUID, frontier.ManifestID): true,
				storage.DataName(frontier.UUID, frontier.ActiveID):       true,
			}
			for _, file := range image.store.Catalog.Files {
				if file.HasPhysicalFile() {
					want[storage.DataName(frontier.UUID, file.ID)] = true
				}
			}
			got := make(map[string]bool, len(entries))
			for _, entry := range entries {
				got[entry.Name()] = true
			}
			if !maps.Equal(got, want) {
				t.Fatalf("objects after recovery: got %v, want %v", got, want)
			}
		})
	}
}

func TestConcurrentReadFailureAfterDurablePublication(t *testing.T) {
	var pause, failRead atomic.Bool
	published, release := make(chan struct{}), make(chan struct{})
	fs := &storage.IO{Hook: func(p string) error {
		if p == "current.dirsync:after" && pause.CompareAndSwap(true, false) {
			close(published)
			<-release
		}
		if p == "read:before" && failRead.Load() {
			return syscall.EIO
		}
		return nil
	}}
	opts := testOptions()
	opts.ValueCacheBudgetBytes = 140
	db := newTestDB(t, opts, fs)
	mustPut(t, db, "k", "old")
	mustPut(t, db, "x", "old")
	pause.Store(true)
	done := make(chan error, 1)
	go func() { done <- db.Put([]byte("x"), []byte("new")) }()
	<-published
	failRead.Store(true)
	if _, err := db.Get([]byte("k")); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	close(release)
	err := <-done
	var op *OpError
	if !errors.As(err, &op) || op.Outcome != Committed || !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("durable writer reported rollback: %v", err)
	}
	if err := db.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, reopened, "x", "new")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseFailureStillReleasesOwnership(t *testing.T) {
	db, err := OpenWithIO(filepath.Join(t.TempDir(), "db"), testOptions(), &storage.IO{Hook: func(p string) error {
		if p == "close.active:before" {
			return syscall.EIO
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "k", "v")
	first := db.Close()
	if !errors.Is(first, syscall.EIO) || db.Close() != first {
		t.Fatalf("close result not retained: %v", first)
	}
	db2, err := Open(db.store.Dir, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

type observedCall struct {
	start, end            uint64
	key, kind, value, got string
	missing               bool
}

func linearizable(calls []observedCall) bool {
	var search func(uint64, string, bool) bool
	search = func(done uint64, value string, present bool) bool {
		if done == uint64(1)<<len(calls)-1 {
			return true
		}
		for i, op := range calls {
			bit := uint64(1) << i
			if done&bit != 0 {
				continue
			}
			eligible := true
			for j, before := range calls {
				if before.end < op.start && done&(uint64(1)<<j) == 0 {
					eligible = false
					break
				}
			}
			if !eligible {
				continue
			}
			next, exists := value, present
			switch op.kind {
			case "put":
				if op.missing {
					continue
				}
				next, exists = op.value, true
			case "delete":
				if op.missing == present {
					continue
				}
				next, exists = "", false
			case "get":
				if op.missing == present || present && op.got != value {
					continue
				}
			default:
				continue
			}
			if search(done|bit, next, exists) {
				return true
			}
		}
		return false
	}
	return search(0, "", false)
}

func TestSeededHistoriesArePerKeyLinearizable(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		db := newTestDB(t, testOptions(), nil)
		var clock atomic.Uint64
		history := make([]observedCall, 12)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for worker := 0; worker < 3; worker++ {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(seed, uint64(worker)))
				<-start
				for n := 0; n < 4; n++ {
					i := worker*4 + n
					op := observedCall{key: fmt.Sprint(rng.IntN(2)), value: fmt.Sprintf("%d:%d", worker, n)}
					op.start = clock.Add(1)
					var err error
					switch rng.IntN(3) {
					case 0:
						op.kind = "put"
						err = db.Put([]byte(op.key), []byte(op.value))
					case 1:
						op.kind = "delete"
						err = db.Delete([]byte(op.key))
					case 2:
						op.kind = "get"
						var got []byte
						got, err = db.Get([]byte(op.key))
						op.got = string(got)
					}
					op.end = clock.Add(1)
					op.missing = errors.Is(err, ErrNotFound)
					if err != nil && (!op.missing || op.kind == "put") {
						t.Error(err)
					}
					history[i] = op
				}
			})
		}
		close(start)
		wg.Wait()
		for _, key := range []string{"0", "1"} {
			var calls []observedCall
			for _, op := range history {
				if op.key == key {
					calls = append(calls, op)
				}
			}
			if !linearizable(calls) {
				t.Fatalf("seed %d key %s non-linearizable: %+v", seed, key, calls)
			}
		}
	}
}

func TestLinearizabilityOracleRejectsImpossiblePutResult(t *testing.T) {
	calls := []observedCall{{kind: "put", value: "v", missing: true, start: 1, end: 2}}
	if linearizable(calls) {
		t.Fatal("an impossible Put ErrNotFound response was accepted")
	}
}

func TestMissingDeletePrecedesExhaustedCountersAndPreservesBatch(t *testing.T) {
	for _, mode := range []IndexMode{IndexMemory, IndexBTree} {
		db := newTestDBWithHooks(t, Options{IndexMode: mode}, nil, &Hooks{CleanerStart: make(chan struct{})})
		mustPut(t, db, "before", "preserved")
		frontier, stats := db.store.Current, db.Stats()
		db.store.Current.Sequence = math.MaxUint64
		db.store.Current.Generation = math.MaxUint64
		exhausted := db.store.Current
		if err := db.Delete([]byte("missing")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("mode %d missing Delete with exhausted counters: %v", mode, err)
		}
		keys := [][]byte{[]byte("before"), []byte("before"), []byte("missing")}
		err := db.BatchDelete(keys)
		var keyError *BatchKeyError
		if !errors.Is(err, ErrNotFound) || !errors.As(err, &keyError) || keyError.Index != 2 {
			t.Fatalf("mode %d missing BatchDelete with exhausted counters: %v", mode, err)
		}
		if got := db.Stats(); db.store.Current != exhausted || got.WriteCommits != stats.WriteCommits || got.LiveKeys != stats.LiveKeys || got.LiveRecordBytes != stats.LiveRecordBytes || got.TombstoneBytes != stats.TombstoneBytes {
			t.Fatalf("mode %d changed metadata after rejected delete, stats %+v", mode, got)
		}
		db.store.Current = frontier
		wantValue(t, db, "before", "preserved")
		if err := db.Delete([]byte("before")); err != nil {
			t.Fatalf("mode %d retry: %v", mode, err)
		}
	}
}

func TestOperationOutcomePreservesItsOwnFaultBesideUncertainReadRepair(t *testing.T) {
	repair := &storage.UncertainReadRepair{Err: &storage.Fault{Phase: "repair.dirsync", Outcome: Unknown, Err: syscall.EIO}}
	write := &storage.Fault{Phase: "commit.health", Outcome: NotApplied, Err: repair}
	for _, err := range []error{write, errors.Join(repair, write)} {
		got := operationError("put", "append", err)
		if got.Phase != write.Phase || got.Outcome != NotApplied || !errors.Is(got, ErrCommitUncertain) {
			t.Fatalf("lost outer publication or nested uncertainty: %v", got)
		}
	}
}

func TestGetRechecksUnavailableLocationAfterConcurrentMutation(t *testing.T) {
	for _, mode := range []IndexMode{IndexMemory, IndexBTree} {
		for _, mutation := range []string{"put", "delete"} {
			t.Run(fmt.Sprintf("%v/%s", mode, mutation), func(t *testing.T) {
				entered, resume := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				block := sync.OnceFunc(func() { close(entered); <-resume })
				fs := &storage.IO{Hook: func(point string) error {
					if point == "repair.verify:before" {
						block()
					}
					return nil
				}}
				dir := t.TempDir()
				db, err := OpenWithHooks(dir, Options{IndexMode: mode}, fs, parkedMaintenance())
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				defer release()
				damageProtectedReadFixture(t, db)
				sidecars, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
				if err != nil || len(sidecars) != 1 {
					t.Fatalf("expected one sidecar: %v, %v", sidecars, err)
				}
				if err := os.Remove(sidecars[0]); err != nil {
					t.Fatal(err)
				}
				type readResult struct {
					value []byte
					err   error
				}
				done := make(chan readResult, 1)
				go func() {
					value, err := db.Get([]byte("target"))
					done <- readResult{value, err}
				}()
				select {
				case <-entered:
				case got := <-done:
					t.Fatalf("Get finished before repair: %q, %v", got.value, got.err)
				case <-time.After(5 * time.Second):
					t.Fatal("Get did not start repair")
				}
				switch mutation {
				case "put":
					mustPut(t, db, "target", "new")
				case "delete":
					if err := db.Delete([]byte("target")); err != nil {
						t.Fatal(err)
					}
				}
				release()
				got := <-done
				if mutation == "put" && (string(got.value) != "new" || got.err != nil) ||
					mutation == "delete" && (got.value != nil || !errors.Is(got.err, ErrNotFound)) {
					t.Fatalf("Get after %s: %q, %v", mutation, got.value, got.err)
				}
				mustPut(t, db, "unrelated", "available")
			})
		}
	}
}

func TestLocalizedReadErrorPreservesCauseAndRepairSpaceLimit(t *testing.T) {
	for _, quota := range []error{syscall.ENOSPC, syscall.EDQUOT} {
		raw := &storage.UnavailableDataError{FileID: 11, Err: errors.Join(format.ErrCorrupt, quota)}
		err := operationError("get", "read", raw)
		if !storage.IsDataUnavailable(err) || !errors.Is(err, ErrDataUnavailable) ||
			!errors.Is(err, ErrCorrupt) || !errors.Is(err, ErrNoSpace) || !errors.Is(err, quota) {
			t.Fatalf("lost localized DATA or capacity error: %v", err)
		}
		if storage.IsDataUnavailable(errors.Join(err, syscall.EIO)) {
			t.Fatal("a separate I/O failure was classified as localized")
		}
	}
}
