package benchmark

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/anaregdesign/tarp"
)

// Each operation returns a whole ordered result set. The immutable fixture
// makes a loop of individual Gets a comparable workload for the snapshot API.
func BenchmarkBatchGet(b *testing.B) {
	const slots = 32
	mixed := make([]int, 0, slots)
	for i := 0; i < slots/2; i += 2 {
		// Revisit each even key after a different live key. With a one-entry
		// LRU the loop misses again; BatchGet can reuse its pinned record.
		mixed = append(mixed, i, -1, i+1, i)
	}
	unique := make([]int, slots)
	for i := range unique {
		unique[i] = i
	}
	for _, tc := range []struct {
		name string
		ids  []int // -1 requests an absent key.
		cold bool
	}{
		{"hot-1", unique[:1], false},
		{"hot-32", unique, false},
		{"miss-os-warm-32", unique, true},
		{"duplicate-missing-os-warm-32", mixed, true},
	} {
		for _, api := range []string{"raw", "typed-string"} {
			for _, method := range []string{"BatchGet", "Get-loop"} {
				b.Run(tc.name+"/"+api+"/"+method, func(b *testing.B) {
					// The last seeded key is never requested. It leaves the
					// one-entry LRU cold for every timed key in the miss arms.
					keys, baseValue, opts := measurementInput(64, slots+1)
					if tc.cold {
						opts.CacheBytes /= int64(len(keys))
					}
					db, err := tarp.Open(filepath.Join(b.TempDir(), "db"), opts)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() {
						if err := db.Close(); err != nil {
							b.Error(err)
						}
					})
					entries := make([]tarp.Entry, len(keys))
					for i, key := range keys {
						value := bytes.Clone(baseValue)
						value[0] = byte(i)
						entries[i] = tarp.Entry{Key: key, Value: value}
					}
					if err := db.BatchPut(entries); err != nil {
						b.Fatal(err)
					}
					request := make([][]byte, len(tc.ids))
					want := make([]tarp.BatchResult, len(tc.ids))
					var found int
					for i, id := range tc.ids {
						if id < 0 {
							request[i] = []byte("absent-batch-key")
							continue
						}
						request[i] = keys[id]
						want[i] = tarp.BatchResult{Value: entries[id].Value, Found: true}
						found++
					}
					b.SetBytes(int64(found * len(baseValue)))
					if api == "raw" {
						measureBatchReads(b, method, db, request, want, db.Get, db.BatchGet, tc.cold, found)
						return
					}
					typed, err := tarp.NewTypedDB(db, tarp.StringCodec{}, tarp.StringCodec{})
					if err != nil {
						b.Fatal(err)
					}
					strings := make([]string, len(request))
					expected := make([]tarp.TypedBatchResult[string], len(request))
					for i, key := range request {
						strings[i] = string(key)
						expected[i] = tarp.TypedBatchResult[string]{Value: string(want[i].Value), Found: want[i].Found}
					}
					measureBatchReads(b, method, db, strings, expected, typed.Get, typed.BatchGet, tc.cold, found)
				})
			}
		}
	}
}

func measureBatchReads[K, V any, R ~struct {
	Value V
	Found bool
}](b *testing.B, method string, db *tarp.DB, keys []K, want []R, get func(K) (V, error), batch func([]K) ([]R, error), cold bool, found int) {
	b.Helper()
	read := batch
	if method == "Get-loop" {
		read = func(keys []K) ([]R, error) {
			results := make([]R, len(keys))
			for i, key := range keys {
				value, err := get(key)
				if err != nil {
					if errors.Is(err, tarp.ErrNotFound) {
						continue
					}
					return nil, err
				}
				results[i] = R{Value: value, Found: true}
			}
			return results, nil
		}
	}
	// Warm the OS page cache/read handles on the same input. On a looping
	// miss arm the last live key differs from the first key of the next pass.
	first, err := read(keys)
	if err != nil || !reflect.DeepEqual(first, want) {
		b.Fatalf("warmup returned incorrect results: %v", err)
	}
	before := db.Stats()
	b.ReportAllocs()
	var results []R
	for b.Loop() {
		results, err = read(keys)
		if err != nil {
			b.Fatal(err)
		}
		runtime.KeepAlive(results)
	}
	if !reflect.DeepEqual(results, want) {
		b.Fatal("incorrect ordered read results")
	}
	after := db.Stats()
	hits, misses := found, 0
	if cold {
		hits, misses = 0, found
	}
	if after.Cache.Hits-before.Cache.Hits != uint64(hits)*uint64(b.N) ||
		after.Cache.Misses-before.Cache.Misses != uint64(misses)*uint64(b.N) ||
		after.MissingKeys-before.MissingKeys != uint64(len(keys)-found)*uint64(b.N) {
		b.Fatalf("unexpected cache classification: before %+v, after %+v", before, after)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(keys)), "ns/slot")
}
