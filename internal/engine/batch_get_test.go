package engine

import (
	"errors"
	"math"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestReadCountersSaturate(t *testing.T) {
	db := newTestDB(t, Options{ValueCacheBudgetBytes: 130}, nil)
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	db.state.Lock()
	db.stats.ValueCache.Hits = math.MaxUint64 - 1
	db.stats.ValueCache.Misses = math.MaxUint64 - 1
	db.stats.MissingKeyLookups = math.MaxUint64 - 1
	db.stats.BatchGetDuplicateInputs = math.MaxUint64 - 1
	db.state.Unlock()
	for range 2 {
		if _, err := db.BatchGet([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("a"), []byte("b")}); err != nil {
			t.Fatal(err)
		}
	}
	s := db.Stats()
	if s.ValueCache.Hits != math.MaxUint64 || s.ValueCache.Misses != math.MaxUint64 ||
		s.MissingKeyLookups != math.MaxUint64 || s.BatchGetDuplicateInputs != math.MaxUint64 {
		t.Fatalf("read counters wrapped: %+v", s)
	}
}

func TestBatchGetPreservesOrderOwnershipAndCache(t *testing.T) {
	var reads atomic.Int64
	opts := testOptions()
	opts.ValueCacheBudgetBytes = 260 // Two one-byte keys and values fit.
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
	if s.ValueCache.Hits != 3 || s.ValueCache.Misses != 1 || s.MissingKeyLookups != 1 || s.BatchGetDuplicateInputs != 2 {
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
	db := newTestDB(t, Options{ValueCacheBudgetBytes: 130}, nil)
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
		if s.ValueCache.Hits != call || s.ValueCache.Misses != call || s.MissingKeyLookups != call*(distinct-2) ||
			s.BatchGetDuplicateInputs != call*(MaxBatchEntries-distinct) {
			t.Fatalf("after BatchGet call %d: %+v", call, s)
		}
	}
}

func TestBatchGetDistinguishesKeysWithMatchingSuffixes(t *testing.T) {
	db := newTestDB(t, Options{ValueCacheBudgetBytes: 132}, nil)
	mustPut(t, db, "axx", "A")
	mustPut(t, db, "bxx", "B") // Only bxx fits in the LRU.
	for _, first := range []string{"xxx", "bxx"} {
		// The first two uncached keys can differ at the suffix, while many
		// later distinct keys share it and collide on the first probe.
		keys := [][]byte{[]byte(first), []byte("xxy")}
		for range 2 {
			for prefix := byte(0); prefix < 128; prefix++ {
				keys = append(keys, []byte{prefix, 'x', 'x'})
			}
		}
		before := db.Stats()
		results, err := db.BatchGet(keys)
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			found := string(key) == "axx" || string(key) == "bxx"
			if results[i].Found != found || (found && (len(results[i].Value) != 1 || results[i].Value[0] != key[0]-'a'+'A')) {
				t.Fatalf("slot %d, key %q: %+v", i, key, results[i])
			}
		}
		after := db.Stats()
		if after.ValueCache.Hits-before.ValueCache.Hits != 1 || after.ValueCache.Misses-before.ValueCache.Misses != 1 ||
			after.MissingKeyLookups-before.MissingKeyLookups != 127 || after.BatchGetDuplicateInputs-before.BatchGetDuplicateInputs != 129 {
			t.Fatalf("first key %q: before %+v, after %+v", first, before, after)
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
	if s.ValueCache.Hits != 0 || s.ValueCache.Misses != 0 || s.MissingKeyLookups != 0 || s.BatchGetDuplicateInputs != 0 {
		t.Fatalf("rejected or empty batches counted lookups: %+v", s)
	}
}

func TestBatchGetLimitsSnapshotValueBytesBeforeReading(t *testing.T) {
	var reads atomic.Int64
	opts := Options{ValueCacheBudgetBytes: 132, MaxBatchGetValueBytes: 7}
	db := newTestDB(t, opts, &storage.IO{Hook: func(point string) error {
		if point == "read:before" {
			reads.Add(1)
		}
		return nil
	}})
	mustPut(t, db, "c", "123")
	mustPut(t, db, "e", "")
	mustPut(t, db, "h", "45") // Only h fits in the cache now.
	c, e, h, missing := []byte("c"), []byte("e"), []byte("h"), []byte("absent")
	file := testLoc(t, db, "c").File
	refs, stats := file.References(), db.Stats()
	for _, tc := range []struct {
		keys   [][]byte
		index  int
		actual uint64
	}{
		{[][]byte{c, e, missing, h, c}, 4, 8},
		{[][]byte{h, h, h, h}, 3, 8},
		{[][]byte{c, c, c}, 2, 9},
	} {
		got, err := db.BatchGet(tc.keys)
		var limit *BatchLimitError
		if got != nil || !errors.Is(err, ErrTooLarge) || !errors.As(err, &limit) ||
			limit.Kind != BatchGetValueBytesLimit || limit.Actual != tc.actual || limit.Limit != 7 || limit.Index != tc.index {
			t.Fatalf("keys %q: got %+v, err %+v", tc.keys, got, err)
		}
	}
	if file.References() != refs || db.Stats() != stats || reads.Load() != 0 {
		t.Fatalf("rejected batches changed refs, stats or reads: %d, %+v, %d", file.References(), db.Stats(), reads.Load())
	}
	got, err := db.BatchGet([][]byte{c, e, missing, h, h})
	if err != nil || len(got) != 5 || string(got[0].Value) != "123" || !got[1].Found || len(got[1].Value) != 0 ||
		got[2].Found || string(got[3].Value) != "45" || string(got[4].Value) != "45" || reads.Load() != 2 {
		t.Fatalf("exact fit: %+v, %v, %d reads", got, err, reads.Load())
	}
}

func TestBatchGetSizeBoundCannotWrap(t *testing.T) {
	// Four maximum values would wrap to four bytes in uint64. The second
	// result already exceeds the limit. No payload allocation is necessary.
	const size = 1<<62 + 1
	var limit *BatchLimitError
	err := checkBatchBytes(4, math.MaxInt64, size, func(int) int64 { return size })
	if !errors.As(err, &limit) || limit.Index != 1 || limit.Actual != 2*uint64(size) || limit.Limit != math.MaxInt64 {
		t.Fatalf("large result: %+v", err)
	}
}

func TestBatchGetSizeBoundTracksPublishedAndRecoveredValues(t *testing.T) {
	opts := Options{MaxBatchGetValueBytes: 4}
	db := newTestDB(t, opts, nil)
	a, b := []byte("a"), []byte("b")
	checkTooLarge := func(db *DB, keys [][]byte, index int, actual uint64) {
		t.Helper()
		got, err := db.BatchGet(keys)
		var limit *BatchLimitError
		if got != nil || !errors.As(err, &limit) || limit.Kind != BatchGetValueBytesLimit ||
			limit.Index != index || limit.Actual != actual || limit.Limit != 4 {
			t.Fatalf("BatchGet(%q) = %+v, %v; want %d bytes at index %d", keys, got, err, actual, index)
		}
	}

	mustPut(t, db, "a", "x")
	mustPut(t, db, "a", "xyz")
	checkTooLarge(db, [][]byte{a, a}, 1, 6)
	if err := db.BatchPut([]Entry{{a, []byte("xy")}, {b, []byte("12345")}}); err != nil {
		t.Fatal(err)
	}
	checkTooLarge(db, [][]byte{b}, 0, 5)
	if err := db.Delete(b); err != nil {
		t.Fatal(err)
	}
	// The deleted maximum still contributes to the bound. Exact sizes fit.
	got, err := db.BatchGet([][]byte{a, b, a})
	if err != nil || !reflect.DeepEqual(got, []BatchResult{{[]byte("xy"), true}, {}, {[]byte("xy"), true}}) {
		t.Fatalf("BatchGet after delete = %+v, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.maxSeenValueBytes != 2 {
		t.Fatalf("recovered value bound = %d, want 2", reopened.maxSeenValueBytes)
	}
	checkTooLarge(reopened, [][]byte{a, a, a}, 2, 6)
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
	synctest.Test(t, func(t *testing.T) {
		resume := make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		defer release()
		var started atomic.Int32
		opts := testOptions()
		opts.ValueCacheBudgetBytes = 260
		opts.BatchGetReadConcurrency = 2
		opts.MaxBatchGetValueBytes = 5 // The original keys fit; the concurrent replacements do not.
		ioHooks := &storage.IO{Hook: func(p string) error {
			if p == "read:before" && started.Add(1) <= 2 {
				<-resume
			}
			return nil
		}}
		db := newTestDBWithHooks(t, opts, ioHooks, parkedMaintenance())
		mustPut(t, db, "a", "A")
		mustPut(t, db, "b", "B")
		mustPut(t, db, "z", "Z")
		mustPut(t, db, "w", "W") // a and b are cold while z stays cached.
		oldA, oldB := testLoc(t, db, "a").File, testLoc(t, db, "b").File
		keys := [][]byte{[]byte("b"), []byte("a"), []byte("z"), []byte("b"), []byte("c"), []byte("a"), []byte("c")}
		done := startBatchGet(db, keys)
		synctest.Wait()
		if got := started.Load(); got != 2 {
			t.Fatalf("in-flight reads = %d, want 2", got)
		}
		if s := db.Stats(); s.ValueCache.Hits != 1 || s.ValueCache.Misses != 2 || s.MissingKeyLookups != 1 || s.BatchGetDuplicateInputs != 3 {
			t.Fatalf("Stats while both record reads are paused: %+v", s)
		}
		mustPut(t, db, "a", "updated")
		if err := db.Delete([]byte("b")); err != nil {
			t.Fatal(err)
		}
		mustPut(t, db, "c", "later")
		if result, err := db.Compact(t.Context()); err != nil || result.PinnedRetiredDataBytes == 0 {
			t.Fatalf("compact with batch reads in flight: %+v, %v", result, err)
		}
		release()
		outcome := <-done
		want := []BatchResult{{[]byte("B"), true}, {[]byte("A"), true}, {[]byte("Z"), true}, {[]byte("B"), true}, {}, {[]byte("A"), true}, {}}
		if outcome.err != nil || !reflect.DeepEqual(outcome.results, want) || oldA.References() != 0 || oldB.References() != 0 {
			t.Fatalf("read after checkpoint: %+v, %v, refs %d/%d", outcome.results, outcome.err, oldA.References(), oldB.References())
		}
		if got, err := db.BatchGet(keys); got != nil || !errors.Is(err, ErrTooLarge) {
			t.Fatalf("new snapshot: %+v, %v", got, err)
		}
	})
}

func TestBatchGetReadFailureDiscardsResultsAndPins(t *testing.T) {
	var reads atomic.Int64
	db := newTestDB(t, Options{ValueCacheBudgetBytes: 130}, &storage.IO{Hook: func(p string) error {
		if p == "read:after" && reads.Add(1) == 2 {
			return format.ErrCorrupt
		}
		return nil
	}})
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	mustPut(t, db, "c", "C")
	mustPut(t, db, "d", "D")
	file := testLoc(t, db, "a").File
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
	if s.ValueCache.Hits != 1 || s.ValueCache.Misses != 3 || s.MissingKeyLookups != 1 || s.BatchGetDuplicateInputs != 2 {
		t.Fatalf("failed I/O should retain probe counts: %+v", s)
	}
}
