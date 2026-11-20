package engine

import (
	"errors"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func testLookup(t *testing.T, db *DB, key string) (storage.Location, bool) {
	t.Helper()
	loc, ok, err := db.lookupLocation(key)
	if err != nil {
		t.Fatal(err)
	}
	return loc, ok
}

func testLoc(t *testing.T, db *DB, key string) storage.Location {
	t.Helper()
	loc, ok := testLookup(t, db, key)
	if !ok {
		t.Fatalf("missing test location for %q", key)
	}
	return loc
}

func TestFailedIndexPreparationDiscardsWholeBatchBeforeAppend(t *testing.T) {
	for _, mode := range []IndexMode{IndexMemory, IndexBTree} {
		var inject bool
		seen := 0
		hooks := &storage.IO{Hook: func(point string) error {
			if inject && point == "index.prepare:before" {
				seen++
				if seen == 2 {
					return syscall.ENOSPC
				}
			}
			return nil
		}}
		opts := Options{IndexMode: mode, TargetSegmentBytes: 512, MaxRecordBytes: 256}
		db := newTestDBWithHooks(t, opts, hooks, &Hooks{CleanerStart: make(chan struct{})})
		mustPut(t, db, "existing", "before")
		frontier := db.store.Current
		inject = true
		err := db.BatchPut([]Entry{{Key: []byte("existing"), Value: []byte("after")}, {Key: []byte("new"), Value: []byte("v")}})
		var op *OpError
		if !errors.As(err, &op) || op.Outcome != NotApplied || !errors.Is(err, ErrNoSpace) || db.store.Current != frontier {
			t.Fatalf("mode %d: failed preparation: %+v, %v", mode, op, err)
		}
		wantValue(t, db, "existing", "before")
		if _, err := db.Get([]byte("new")); !errors.Is(err, ErrNotFound) {
			t.Fatal(err)
		}
		inject = false
		dir := db.store.Dir
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		wantValue(t, reopened, "existing", "before")
		if err := reopened.Put([]byte("new"), []byte("retry")); err != nil {
			t.Fatal(err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestIndexedReadsRejectMissingDataFileAndReleasePins(t *testing.T) {
	for _, backend := range []struct {
		name string
		mode IndexMode
	}{{"memory", IndexMemory}, {"btree", IndexBTree}} {
		for _, op := range []string{"get", "batch-get", "scan"} {
			t.Run(backend.name+"/"+op, func(t *testing.T) {
				opts := Options{IndexMode: backend.mode, MaxRecordBytes: 64, TargetSegmentBytes: 128}
				db := newTestDBWithHooks(t, opts, nil, &Hooks{CleanerStart: make(chan struct{})})
				mustPut(t, db, "a", "v")
				mustPut(t, db, "b", "v")
				first, missing := testLoc(t, db, "a").File, testLoc(t, db, "b").File
				if first == missing {
					t.Fatal("test requires distinct DATA files")
				}
				Observe(db).ClearCache()
				delete(db.files, missing.ID)
				before := first.References()
				var err error
				switch op {
				case "get":
					_, err = db.Get([]byte("b"))
				case "batch-get":
					_, err = db.BatchGet([][]byte{[]byte("a"), []byte("b")})
				case "scan":
					for _, e := range db.Scan(t.Context()) {
						if e != nil {
							err = e
							break
						}
					}
				}
				if !errors.Is(err, ErrCorrupt) || !errors.Is(db.health(), ErrUnhealthy) || first.References() != before {
					t.Fatalf("operation error %v, health %v, pins %d (before %d)", err, db.health(), first.References(), before)
				}
			})
		}
	}
}
