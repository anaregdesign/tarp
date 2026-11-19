package engine

import (
	"errors"
	"math"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestReadCountersSaturate(t *testing.T) {
	db := newTestDB(t, Options{CacheBytes: 130}, nil)
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	db.state.Lock()
	db.stats.Cache.Hits = math.MaxUint64 - 1
	db.stats.Cache.Misses = math.MaxUint64 - 1
	db.stats.MissingKeys = math.MaxUint64 - 1
	db.stats.BatchGetDuplicateInputs = math.MaxUint64 - 1
	db.state.Unlock()
	for range 2 {
		if _, err := db.BatchGet([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("a"), []byte("b")}); err != nil {
			t.Fatal(err)
		}
	}
	s := db.Stats()
	if s.Cache.Hits != math.MaxUint64 || s.Cache.Misses != math.MaxUint64 ||
		s.MissingKeys != math.MaxUint64 || s.BatchGetDuplicateInputs != math.MaxUint64 {
		t.Fatalf("read counters wrapped: %+v", s)
	}
}

func TestBatchGetPreservesOrderOwnershipAndCache(t *testing.T) {
	var reads atomic.Int64
	opts := testOptions()
	opts.CacheBytes = 260 // Two one-byte keys and values fit.
	db := newTestDB(t, opts, &storage.IO{Hook: func(p string) error {
		if p == "read:before" {
			reads.Add(1)
		}
		return nil
	}})
	mustPut(t, db, "d", "D")
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "")
	wantValue(t, db, "a", "A") // Keep a hot.
	keys := [][]byte{[]byte("x"), []byte("a"), []byte("a"), []byte("b"), []byte("d"), []byte("d")}
	got, err := db.BatchGet(keys)
	if err != nil || len(got) != 6 {
		t.Fatalf("BatchGet = %+v, %v", got, err)
	}
	if got[0].Found || !got[1].Found || string(got[1].Value) != "A" ||
		!got[2].Found || string(got[2].Value) != "A" || !got[3].Found || len(got[3].Value) != 0 ||
		!got[4].Found || string(got[4].Value) != "D" || !got[5].Found || string(got[5].Value) != "D" {
		t.Fatal(got)
	}
	s := db.Stats()
	if s.Cache.Hits != 3 || s.Cache.Misses != 1 || s.MissingKeys != 1 || s.BatchGetDuplicateInputs != 2 {
		t.Fatalf("unexpected counts for one Get and four distinct BatchGet keys: %+v", s)
	}
	got[1].Value[0] = 'B'
	got[4].Value[0] = 'E'
	if string(got[2].Value) != "A" || string(got[5].Value) != "D" {
		t.Fatal("duplicate values share a backing array")
	}
	mustPut(t, db, "c", "C")
	wantValue(t, db, "a", "A")
	if reads.Load() != 1 {
		t.Fatalf("got %d storage reads, want one cold read for d and a cache hit for a", reads.Load())
	}
	wantValue(t, db, "d", "D")
	if reads.Load() != 2 {
		t.Fatalf("BatchGet changed cache membership, read count %d", reads.Load())
	}
}

func TestBatchGetCountsDistinctKeysInEachCall(t *testing.T) {
	db := newTestDB(t, Options{CacheBytes: 130}, nil)
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B") // Only b fits in the LRU.
	const distinct = 64
	keys := make([][]byte, MaxBatchEntries)
	for i := range keys {
		keys[i] = []byte{byte('a' + i%distinct)}
	}
	for call := uint64(1); call <= 2; call++ {
		if _, err := db.BatchGet(keys); err != nil {
			t.Fatal(err)
		}
		s := db.Stats()
		if s.Cache.Hits != call || s.Cache.Misses != call || s.MissingKeys != call*(distinct-2) ||
			s.BatchGetDuplicateInputs != call*(MaxBatchEntries-distinct) {
			t.Fatalf("after BatchGet call %d: %+v", call, s)
		}
	}
}

func TestBatchGetRejectsInvalidInputBeforeReading(t *testing.T) {
	db := newTestDB(t, Options{}, &storage.IO{Hook: func(p string) error {
		if p == "read:before" {
			t.Fatal("unexpected record read")
		}
		return nil
	}})
	mustPut(t, db, "a", "A")
	if got, err := db.BatchGet([][]byte{[]byte("a"), []byte("a"), nil}); got != nil || !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key: %+v, %v", got, err)
	}
	large := make([][]byte, MaxBatchEntries+1)
	for i := range large {
		large[i] = []byte("a")
	}
	if got, err := db.BatchGet(large); got != nil || !errors.Is(err, ErrTooLarge) {
		t.Fatalf("too many keys: %+v, %v", got, err)
	}
	if got, err := db.BatchGet(nil); err != nil || len(got) != 0 {
		t.Fatalf("empty batch: %+v, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BatchGet(nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed empty batch: %v", err)
	}
	s := db.Stats()
	if s.Cache.Hits != 0 || s.Cache.Misses != 0 || s.MissingKeys != 0 || s.BatchGetDuplicateInputs != 0 {
		t.Fatalf("rejected or empty batches counted lookups: %+v", s)
	}
}

func TestBatchGetSeesWholeBatchMutations(t *testing.T) {
	db := newTestDB(t, Options{}, nil)
	keys := [][]byte{[]byte("left"), []byte("right")}
	stop, ready, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		close(ready)
		for {
			select {
			case <-stop:
				return
			default:
			}
			pair, err := db.BatchGet(keys)
			if err != nil {
				t.Error(err)
				return
			}
			if pair[0].Found != pair[1].Found || string(pair[0].Value) != string(pair[1].Value) {
				t.Errorf("observed partial batch: %+v", pair)
				return
			}
			runtime.Gosched()
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	<-ready
	for generation := range byte(8) {
		value := []byte{generation}
		if err := db.BatchPut([]Entry{{keys[0], value}, {keys[1], value}}); err != nil {
			t.Fatal(err)
		}
		if generation%2 == 1 {
			if err := db.BatchDelete(keys); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestBatchGetReadsPinnedPointInTimeAcrossCompact(t *testing.T) {
	started, resume := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	defer release()
	var paused atomic.Bool
	opts := testOptions()
	opts.CacheBytes = 260
	db := newTestDB(t, opts, &storage.IO{
		CleanerStart: make(chan struct{}),
		Hook: func(p string) error {
			if p == "read:before" && paused.CompareAndSwap(false, true) {
				close(started)
				<-resume
			}
			return nil
		},
	})
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	mustPut(t, db, "z", "Z") // Only a is cold in the captured view.
	old := db.index["a"].File
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("c"), []byte("c")}
	done := make(chan struct{})
	var got []BatchResult
	var err error
	go func() {
		defer close(done)
		got, err = db.BatchGet(keys)
	}()
	<-started
	if s := db.Stats(); s.Cache.Hits != 1 || s.Cache.Misses != 1 || s.MissingKeys != 1 || s.BatchGetDuplicateInputs != 2 {
		t.Fatalf("Stats while the first record read is paused: %+v", s)
	}
	mustPut(t, db, "a", "updated")
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "c", "later")
	if result, err := db.Compact(t.Context()); err != nil || result.PinnedBytes == 0 {
		t.Fatalf("compact with batch read in flight: %+v, %v", result, err)
	}
	release()
	<-done
	want := []BatchResult{{[]byte("A"), true}, {[]byte("B"), true}, {[]byte("A"), true}, {}, {}}
	if err != nil || !reflect.DeepEqual(got, want) || old.References() != 0 {
		t.Fatalf("read after checkpoint: %+v, %v, refs %d", got, err, old.References())
	}
}

func TestBatchGetReadFailureDiscardsResultsAndPins(t *testing.T) {
	var reads atomic.Int64
	db := newTestDB(t, Options{CacheBytes: 130}, &storage.IO{Hook: func(p string) error {
		if p == "read:after" && reads.Add(1) == 2 {
			return format.ErrCorrupt
		}
		return nil
	}})
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	mustPut(t, db, "c", "C")
	mustPut(t, db, "d", "D")
	file := db.index["a"].File
	refs := file.References()
	keys := [][]byte{[]byte("d"), []byte("a"), []byte("b"), []byte("c"), []byte("a"), []byte("x"), []byte("x")}
	if got, err := db.BatchGet(keys); got != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt read: %+v, %v", got, err)
	}
	if file.References() != refs {
		t.Fatalf("retained pins: %d, want %d", file.References(), refs)
	}
	if _, err := db.BatchGet(keys); !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("next read: %v", err)
	}
	s := db.Stats()
	if s.Cache.Hits != 1 || s.Cache.Misses != 3 || s.MissingKeys != 1 || s.BatchGetDuplicateInputs != 2 {
		t.Fatalf("failed I/O should retain probe counts: %+v", s)
	}
}
