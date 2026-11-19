package benchmark

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/format"
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
	shuffled := slices.Clone(unique)
	rng := rand.New(rand.NewPCG(66, 1))
	rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	shuffledAbsent := make([]int, slots)
	for i, id := range shuffled {
		shuffledAbsent[i] = -2 - id
	}
	repeatedHot := make([]int, slots)
	repeatedAbsent := make([]int, slots)
	for i := range repeatedAbsent {
		repeatedAbsent[i] = -1
	}
	for _, tc := range []struct {
		name string
		ids  []int // Negative IDs request absent keys.
		cold bool
	}{
		{"hot-1", unique[:1], false},
		{"hot-32", unique, false},
		{"hot-shuffled-32", shuffled, false},
		{"repeated-hot-32", repeatedHot, false},
		{"repeated-absent-32", repeatedAbsent, false},
		{"shuffled-distinct-absent-32", shuffledAbsent, false},
		{"miss-os-warm-32", unique, true},
		{"duplicate-missing-os-warm-32", mixed, true},
	} {
		for _, api := range []string{"raw", "typed-string"} {
			for _, method := range []string{"BatchGet", "Get-loop"} {
				b.Run(tc.name+"/"+api+"/"+method, func(b *testing.B) {
					const valueBytes = 64
					db, entries := openBatchReadFixture(b, valueBytes, slots, tc.cold, 0)
					request := make([][]byte, len(tc.ids))
					want := make([]tarp.BatchResult, len(tc.ids))
					var found int
					for i, id := range tc.ids {
						if id < -1 {
							// Continue the fixture's fixed-width hex key space past the seed.
							request[i] = []byte(fmt.Sprintf("%016x", slots-1-id))
							continue
						}
						if id == -1 {
							request[i] = []byte("absent-batch-key")
							continue
						}
						request[i] = entries[id].Key
						want[i] = tarp.BatchResult{Value: entries[id].Value, Found: true}
						found++
					}
					b.SetBytes(int64(found * valueBytes))
					if api == "raw" {
						measureBatchReads(b, method, db, request, want, db.Get, db.BatchGet, tc.cold, tc.ids)
						return
					}
					typed, err := tarp.Bind(db, tarp.StringCodec{}, tarp.StringCodec{})
					if err != nil {
						b.Fatal(err)
					}
					strings := make([]string, len(request))
					expected := make([]tarp.TypedBatchResult[string], len(request))
					for i, key := range request {
						strings[i] = string(key)
						expected[i] = tarp.TypedBatchResult[string]{Value: string(want[i].Value), Found: want[i].Found}
					}
					measureBatchReads(b, method, db, strings, expected, typed.Get, typed.BatchGet, tc.cold, tc.ids)
				})
			}
		}
	}
}

// Compare the per-DB DATA read limit with one caller, a cold tarp LRU and
// warm OS pages. The same shuffled trace is used for every limit and size.
func BenchmarkBatchGetReadConcurrency(b *testing.B) {
	const slots = 32
	ids := rand.New(rand.NewPCG(70, 1)).Perm(slots)
	for _, size := range []int{64, 64 << 10} {
		for _, limit := range []int{1, 2, 4, 8} {
			b.Run(fmt.Sprintf("%dB/limit-%d", size, limit), func(b *testing.B) {
				db, entries := openBatchReadFixture(b, size, slots, true, limit)
				keys := make([][]byte, slots)
				want := make([]tarp.BatchResult, slots)
				for i, id := range ids {
					keys[i] = entries[id].Key
					want[i] = tarp.BatchResult{Found: true, Value: entries[id].Value}
				}
				b.SetBytes(int64(size * slots))
				measureBatchReads(b, "BatchGet", db, keys, want, db.Get, db.BatchGet, true, ids)
			})
		}
	}
}

func openBatchReadFixture(b *testing.B, size, slots int, cold bool, limit int) (*tarp.DB, []tarp.Entry) {
	b.Helper()
	// The last key fills the one-entry cache in the cold cases.
	keys, value, opts := measurementInput(size, slots+1)
	opts.BatchReadConcurrency = limit
	if cold {
		opts.CacheBytes /= int64(len(keys))
	}
	recordBytes := int64(format.RecordHeaderBytes + len(keys[0]) + size)
	opts.MaxRecordBytes = max(opts.MaxRecordBytes, recordBytes)
	opts.TargetSegmentBytes = max(opts.TargetSegmentBytes, format.SegmentHeaderBytes+int64(len(keys))*recordBytes)
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
		v := bytes.Clone(value)
		v[0] = byte(i)
		entries[i] = tarp.Entry{Key: key, Value: v}
	}
	if err := db.BatchPut(entries); err != nil {
		b.Fatal(err)
	}
	return db, entries
}

func measureBatchReads[K, V any, R ~struct {
	Value V
	Found bool
}](b *testing.B, method string, db *tarp.DB, keys []K, want []R, get func(K) (V, error), batch func([]K) ([]R, error), cold bool, ids []int) {
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
	var hits, misses, missing, duplicates uint64
	seen := make(map[int]bool, len(ids))
	for _, id := range ids {
		if method == "BatchGet" && seen[id] {
			duplicates++
			continue
		}
		seen[id] = true
		switch {
		case id < 0:
			missing++
		case cold:
			misses++
		default:
			hits++
		}
	}
	n := uint64(b.N)
	if after.Cache.Hits-before.Cache.Hits != hits*n ||
		after.Cache.Misses-before.Cache.Misses != misses*n ||
		after.MissingKeys-before.MissingKeys != missing*n ||
		after.BatchGetDuplicateInputs-before.BatchGetDuplicateInputs != duplicates*n {
		b.Fatalf("unexpected cache classification: before %+v, after %+v", before, after)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(keys)), "ns/slot")
}
