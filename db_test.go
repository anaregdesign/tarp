package tarp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

func testOptions() Options {
	return Options{CacheBytes: 1024, MaxRecordBytes: 128, MaxDataFileBytes: 256, ReadFileHandles: 2}
}

func newTestDB(t *testing.T, opts Options, fs *storage.IO) *DB {
	t.Helper()
	db, err := openWithIO(filepath.Join(t.TempDir(), "db"), opts, fs)
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
	for _, entry := range db.store.Files {
		if st, err := os.Stat(entry.Path); err != nil || st.Size() > testOptions().MaxDataFileBytes {
			t.Fatalf("file bound: %v %v", st, err)
		}
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

func TestOversizedPutIsPreMutation(t *testing.T) {
	for _, opts := range []Options{
		{CacheBytes: 140, MaxRecordBytes: 128, MaxDataFileBytes: 256, ReadFileHandles: 1},
		{CacheBytes: 1024, MaxRecordBytes: 60, MaxDataFileBytes: 256, ReadFileHandles: 1},
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
			opts.CacheBytes = 140
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
		})
	}
}

func TestExactCacheIntegration(t *testing.T) {
	var reads atomic.Int64
	opts := testOptions()
	opts.CacheBytes = 2 * (128 + 1 + 1)
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
}

func TestCommitFaultOutcomesAndFailStop(t *testing.T) {
	for _, point := range []string{"data.write:before", "data.write:after", "data.sync:before", "data.sync:after", "current.write:after", "current.sync:after", "current.rename:before", "current.rename:after", "current.dirsync:before", "current.dirsync:after"} {
		t.Run(point, func(t *testing.T) {
			var armed atomic.Bool
			fs := &storage.IO{Hook: func(p string) error {
				if p == point && armed.Load() {
					return syscall.ENOSPC
				}
				return nil
			}}
			db := newTestDB(t, testOptions(), fs)
			mustPut(t, db, "k", "old")
			armed.Store(true)
			err := db.Put([]byte("k"), []byte("new"))
			var op *OpError
			if !errors.As(err, &op) || !errors.Is(err, ErrNoSpace) {
				t.Fatalf("error: %v", err)
			}
			uncertain := strings.HasPrefix(point, "current.rename") || strings.HasPrefix(point, "current.dirsync")
			if errors.Is(err, ErrCommitUncertain) != uncertain || op.Outcome == NotApplied && uncertain {
				t.Fatalf("wrong outcome: %+v", op)
			}
			if _, err := db.Get([]byte("k")); !errors.Is(err, ErrUnhealthy) {
				t.Fatalf("cached read escaped fail-stop: %v", err)
			}
			armed.Store(false)
			if err := db.Close(); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("close lost cause: %v", err)
			}
			reopened, err := Open(db.store.Dir, testOptions())
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			if point == "current.rename:after" || strings.HasPrefix(point, "current.dirsync") {
				want = "new"
			}
			wantValue(t, reopened, "k", want)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
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
	active := db2.index["k"].File.Path
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
	path, active := db.store.Dir, db.index["k"].File.Path
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
	opts.CacheBytes = 150
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
