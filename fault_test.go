package tarp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

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
			_, err := openWithIO(dir, testOptions(), &storage.IO{Hook: func(p string) error {
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
			if len(db.index) != 0 {
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
	db, err := openWithIO(t.TempDir(), testOptions(), &storage.IO{Hook: func(point string) error {
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

func TestExactFileBoundaryAndSmallerPhysicalBound(t *testing.T) {
	opts := Options{CacheBytes: 129, MaxRecordBytes: 49, MaxDataFileBytes: 113, ReadFileHandles: 1}
	db := newTestDB(t, opts, nil)
	mustPut(t, db, "a", "")
	for _, f := range db.store.Files {
		if f.Size.Load() != opts.MaxDataFileBytes {
			t.Fatalf("exact record boundary: %d", f.Size.Load())
		}
	}
	mustPut(t, db, "b", "")
	if err := db.Put([]byte("xx"), nil); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	larger := testOptions()
	larger.MaxDataFileBytes = 200
	larger.MaxRecordBytes = 100
	db2, err := Open(db.store.Dir, larger)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db2, "c", "")
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(db.store.Dir, opts); !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatalf("physical file shrink: %v", err)
	}
}

func TestConcurrentPublishersNeverReplace(t *testing.T) {
	a := newTestDB(t, testOptions(), nil)
	b := newTestDB(t, testOptions(), nil)
	mustPut(t, a, "k", "a")
	mustPut(t, b, "k", "b")
	target := filepath.Join(t.TempDir(), "backup")
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, db := range []*DB{a, b} {
		wg.Go(func() {
			<-start
			results <- db.Backup(context.Background(), target)
		})
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, os.ErrExist) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("publish successes: %d", success)
	}
	if err := Restore(context.Background(), target, filepath.Join(t.TempDir(), "restore"), testOptions()); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedCompactionDoesNotRetainOldFileRegistry(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
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

func TestGenerationExhaustionCannotWrapDuringRotation(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		t.Run(fmt.Sprint(rotate), func(t *testing.T) {
			db := newTestDB(t, testOptions(), nil)
			mustPut(t, db, "k", "old")
			if rotate {
				mustPut(t, db, "pad", string(make([]byte, 60)))
			}
			db.store.Current.Generation = math.MaxUint64 - 1
			err := db.Put([]byte("k"), []byte("new"))
			if rotate && err == nil || !rotate && err != nil {
				t.Fatalf("rotation=%t: %v", rotate, err)
			}
			current, readErr := os.ReadFile(filepath.Join(db.store.Dir, "CURRENT"))
			if readErr != nil {
				t.Fatal(readErr)
			}
			frontier, decodeErr := format.DecodeFrontier(current)
			if decodeErr != nil || frontier.Generation != math.MaxUint64 {
				t.Fatalf("invalid last publication: %+v %v", frontier, decodeErr)
			}
			if err := db.Put([]byte("k"), []byte("never")); err == nil {
				t.Fatal("exhausted generation accepted")
			}
			expected := "new"
			if rotate {
				expected = "old"
			}
			wantValue(t, db, "k", expected)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(db.store.Dir, testOptions())
			if err != nil {
				t.Fatal(err)
			}
			wantValue(t, reopened, "k", expected)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCompactionValidatesCapturedSegmentHeaders(t *testing.T) {
	for _, damage := range []string{"checksum", "uuid", "id", "kind"} {
		t.Run(damage, func(t *testing.T) {
			db := newTestDB(t, testOptions(), nil)
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
			if _, err := db.Compact(context.Background()); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("header damage skipped: %v", err)
			}
			if _, err := db.Get([]byte("k")); !errors.Is(err, ErrUnhealthy) {
				t.Fatalf("corrupt source hidden by cache: %v", err)
			}
			if _, err := os.Stat(file.Path); err != nil {
				t.Fatalf("corrupt selected evidence removed: %v", err)
			}
		})
	}
}

func TestCleanupAfterFaultDoesNotRetainUnlinkedMetadata(t *testing.T) {
	var armed atomic.Bool
	db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
		if p == "cleanup.unlink:after" && armed.CompareAndSwap(true, false) {
			return syscall.EIO
		}
		return nil
	}})
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

func TestRecoveryBoundaryFailureReleasesOwnershipAndPreservesAcknowledgment(t *testing.T) {
	for _, point := range []string{"recover.truncate:before", "recover.truncate:after", "recover.sync:before", "recover.sync:after"} {
		for _, cause := range []error{syscall.EIO, syscall.ENOSPC, syscall.EDQUOT} {
			t.Run(point+"/"+cause.Error(), func(t *testing.T) {
				db := newTestDB(t, testOptions(), nil)
				mustPut(t, db, "k", "acknowledged")
				dir, path, end := db.store.Dir, db.index["k"].File.Path, db.store.Current.ActiveEnd
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
				failed, err := openWithIO(dir, testOptions(), &storage.IO{Hook: func(p string) error {
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
