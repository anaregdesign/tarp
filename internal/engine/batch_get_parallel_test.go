package engine

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

type batchGetOutcome struct {
	results []BatchResult
	err     error
}

func startBatchGet(db *DB, keys [][]byte) <-chan batchGetOutcome {
	done := make(chan batchGetOutcome, 1)
	go func() {
		results, err := db.BatchGet(keys)
		done <- batchGetOutcome{results, err}
	}()
	return done
}

func TestBatchGetLimitsConcurrentStorageReads(t *testing.T) {
	for _, tc := range []struct {
		name             string
		limit, keys      int
		calls, wantReads int
	}{
		{"default", 0, 10, 1, 8},
		{"above-default", 16, 18, 1, 16},
		{"serial", 1, 4, 2, 1},
		{"distinct-cold-keys", 8, 2, 1, 2},
		{"shared-across-batches", 2, 4, 3, 2},
		{"shared-across-single-key-batches", 2, 1, 4, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gate := make(chan struct{})
				release := sync.OnceFunc(func() { close(gate) })
				defer release()
				var started atomic.Int32
				opts := Options{CacheBytes: 130, BatchReadConcurrency: tc.limit}
				db := newTestDB(t, opts, &storage.IO{
					CleanerStart: make(chan struct{}),
					Hook: func(point string) error {
						if point == "read:before" {
							started.Add(1)
							<-gate
						}
						return nil
					},
				})
				var keys [][]byte
				var want []BatchResult
				for i := range tc.keys {
					key := []byte{byte('a' + i)}
					mustPut(t, db, string(key), string(key))
					keys = append(keys, key)
					want = append(want, BatchResult{Value: key, Found: true})
				}
				mustPut(t, db, "z", "Z") // All requested live keys except z are cold.
				if tc.keys > 1 {
					keys = append(keys, []byte("z"), keys[0], []byte("?"))
					want = append(want, BatchResult{[]byte("Z"), true}, want[0], BatchResult{})
				}
				var calls []<-chan batchGetOutcome
				for range tc.calls {
					calls = append(calls, startBatchGet(db, keys))
				}
				// All callers and workers are now either waiting for a read slot or
				// blocked in the hook. No storage read has returned its slot yet.
				synctest.Wait()
				if got := int(started.Load()); got != tc.wantReads {
					t.Fatalf("concurrent DATA reads = %d, want %d", got, tc.wantReads)
				}
				// A singleton shares the existing readers' budget. When there is
				// spare capacity, it can start while the other callers are blocked.
				single := startBatchGet(db, keys[:1])
				synctest.Wait()
				wantActive := tc.wantReads
				if tc.limit > tc.wantReads {
					wantActive++
				}
				if got := int(started.Load()); got != wantActive {
					t.Fatalf("concurrent DATA reads after single-key request = %d, want %d", got, wantActive)
				}
				// Neither cache hits nor absent keys require a DATA read slot.
				for _, probe := range []struct {
					key  string
					want BatchResult
				}{
					{"z", BatchResult{[]byte("Z"), true}},
					{"?", BatchResult{}},
				} {
					done := startBatchGet(db, [][]byte{[]byte(probe.key), []byte(probe.key)})
					synctest.Wait()
					select {
					case outcome := <-done:
						if outcome.err != nil || !reflect.DeepEqual(outcome.results, []BatchResult{probe.want, probe.want}) {
							t.Fatalf("BatchGet for %q while DATA reads blocked: %+v", probe.key, outcome)
						}
					default:
						t.Fatalf("BatchGet for %q waited for a DATA read slot", probe.key)
					}
				}
				release()
				if outcome := <-single; outcome.err != nil || !reflect.DeepEqual(outcome.results, want[:1]) {
					t.Fatalf("single-key BatchGet = %+v, want %+v", outcome, want[:1])
				}
				for _, done := range calls {
					outcome := <-done
					if outcome.err != nil || !reflect.DeepEqual(outcome.results, want) {
						t.Fatalf("BatchGet = %+v, %v; want %+v", outcome.results, outcome.err, want)
					}
					if tc.keys > 1 {
						outcome.results[0].Value[0] = '!'
						if string(outcome.results[tc.keys+1].Value) != "a" {
							t.Fatal("duplicate result shares storage with the original")
						}
					}
				}
				if got, expected := int(started.Load()), tc.calls*tc.keys+1; got != expected {
					t.Fatalf("total DATA reads = %d, want %d", got, expected)
				}
			})
		})
	}
}

func TestBatchGetDrainsInFlightReadBeforeReleasingPinsOnError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		var started atomic.Int32
		db := newTestDB(t, Options{CacheBytes: 130, BatchReadConcurrency: 2}, &storage.IO{
			CleanerStart: make(chan struct{}),
			Hook: func(point string) error {
				if point == "read:before" {
					switch started.Add(1) {
					case 1:
						<-gate
					case 2:
						return format.ErrCorrupt
					}
				}
				return nil
			},
		})
		for _, k := range []string{"a", "b", "c", "z"} {
			mustPut(t, db, k, k)
		}
		file := db.index["a"].File
		refs := file.References()
		done := startBatchGet(db, [][]byte{[]byte("a"), []byte("b"), []byte("a"), []byte("c"), []byte("?")})
		synctest.Wait()
		if _, err := db.Get([]byte("z")); !errors.Is(err, ErrUnhealthy) {
			t.Fatalf("Get after worker failure = %v, want ErrUnhealthy", err)
		}
		select {
		case outcome := <-done:
			t.Fatalf("BatchGet returned with a DATA read in flight: %+v", outcome)
		default:
		}
		if got := file.References(); got != refs+3 {
			t.Fatalf("pins while draining = %d, want %d", got, refs+3)
		}
		s := db.Stats()
		if s.Cache.Misses != 3 || s.MissingKeys != 1 || s.BatchGetDuplicateInputs != 1 {
			t.Fatalf("incomplete snapshot during error handling: %+v", s)
		}
		release()
		outcome := <-done
		if outcome.results != nil || !errors.Is(outcome.err, ErrCorrupt) {
			t.Fatalf("BatchGet after drain = %+v, %v", outcome.results, outcome.err)
		}
		if got := file.References(); got != refs || started.Load() != 2 {
			t.Fatalf("after drain: pins = %d, want %d; reads = %d, want 2", got, refs, started.Load())
		}
	})
}

func TestBatchGetSmallReadsAdaptWithinTheSameBatch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		slowAt int
	}{{"first-read", 1}, {"after-fast-prefix", 6}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const limit, unique = 3, 16
				const holdAfter = 11 // Leave enough work for all readers after the slow read.
				training, step := make(chan struct{}), make(chan struct{})
				endTraining := sync.OnceFunc(func() { close(training) })
				release := sync.OnceFunc(func() { close(step) })
				defer endTraining()
				defer release()

				var trained atomic.Bool
				var started atomic.Int32
				db := newTestDB(t, Options{CacheBytes: 130, BatchReadConcurrency: limit}, &storage.IO{
					CleanerStart: make(chan struct{}),
					Hook: func(point string) error {
						if point != "read:before" {
							return nil
						}
						n := int(started.Add(1))
						if !trained.Load() {
							<-training
							return nil
						}
						if n == 1 || n == tc.slowAt || n > holdAfter {
							<-step
						}
						return nil
					},
				})
				keys := [][]byte{[]byte("?"), []byte("z")}
				want := []BatchResult{{}, {Value: []byte("Z"), Found: true}}
				for i := range unique {
					key, value := []byte{byte('a' + i)}, []byte{byte('A' + i)}
					mustPut(t, db, string(key), string(value))
					keys = append(keys, key)
					want = append(want, BatchResult{Value: value, Found: true})
				}
				mustPut(t, db, "z", "Z")
				keys = append(keys, keys[2])
				want = append(want, want[2])

				done := startBatchGet(db, keys)
				synctest.Wait()
				if got := started.Load(); got != limit {
					t.Fatalf("readers on first batch = %d, want %d", got, limit)
				}
				endTraining() // No virtual time passed while these reads were held.
				if outcome := <-done; outcome.err != nil || !reflect.DeepEqual(outcome.results, want) {
					t.Fatalf("training batch: %+v", outcome)
				}
				if got := started.Swap(0); got != unique {
					t.Fatalf("training DATA reads = %d, want %d", got, unique)
				}
				// An absent key and an LRU hit supply no DATA latency samples.
				if got, err := db.BatchGet(keys[:2]); err != nil || !reflect.DeepEqual(got, want[:2]) {
					t.Fatalf("absent/hot batch: %+v, %v", got, err)
				}
				if got := started.Load(); got != 0 {
					t.Fatalf("absent/hot batch issued %d DATA reads", got)
				}
				trained.Store(true)
				before := db.Stats()
				done = startBatchGet(db, keys)
				synctest.Wait()
				if got := started.Load(); got != 1 {
					t.Fatalf("readers after fast training = %d, want 1", got)
				}
				if tc.slowAt != 1 {
					step <- struct{}{}
					synctest.Wait()
					if got := int(started.Load()); got != tc.slowAt {
						t.Fatalf("started while read %d is held = %d", tc.slowAt, got)
					}
				}
				time.Sleep(time.Millisecond) // Only this checked read spans nonzero virtual time.
				step <- struct{}{}
				synctest.Wait()
				if got := started.Load(); got != holdAfter+limit {
					t.Fatalf("readers blocked in tail after slow read = %d, want %d", got-holdAfter, limit)
				}
				s := db.Stats()
				if s.Cache.Hits-before.Cache.Hits != 1 || s.Cache.Misses-before.Cache.Misses != unique ||
					s.MissingKeys-before.MissingKeys != 1 || s.BatchGetDuplicateInputs-before.BatchGetDuplicateInputs != 1 {
					t.Fatalf("snapshot changed during scheduling: before %+v, after %+v", before, s)
				}
				// A second BatchGet must share the permits occupied by the promoted readers.
				single := startBatchGet(db, keys[2:3])
				synctest.Wait()
				if got := started.Load(); got != holdAfter+limit {
					t.Fatalf("concurrent single-key BatchGet exceeded the limit: %d tail reads", got-holdAfter)
				}
				release()
				outcome := <-done
				if outcome.err != nil || !reflect.DeepEqual(outcome.results, want) {
					t.Fatalf("adaptive batch: %+v", outcome)
				}
				outcome.results[2].Value[0] = '!'
				if string(outcome.results[len(keys)-1].Value) != "A" {
					t.Fatal("adaptive batch returned aliased duplicate values")
				}
				if outcome := <-single; outcome.err != nil || !reflect.DeepEqual(outcome.results, want[2:3]) {
					t.Fatalf("single-key batch: %+v", outcome)
				}
				if got := started.Load(); got != unique+1 {
					t.Fatalf("DATA reads after training = %d, want %d", got, unique+1)
				}
			})
		})
	}
}
