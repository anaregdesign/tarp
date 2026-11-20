package benchmark

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
)

// The first eight bytes are unique. Prefix compression therefore leaves at
// least 56 of these inline key bytes in each leaf, plus a 40-byte address.
// Bit reversal spreads replacement keys throughout the original key space.
func scaleBTreeKey(id int) [64]byte {
	var key [64]byte
	binary.BigEndian.PutUint64(key[:8], bits.Reverse64(uint64(id)))
	binary.BigEndian.PutUint64(key[56:], uint64(id))
	return key
}

func TestStorageScaleBTree(t *testing.T) {
	round := storageScaleRound(t)
	const liveKeys = 192 << 10
	const seedBatch = 128
	const windows, cycles = 4, 256
	const readProbe = 1024

	// Isolate the foreground tree. Cleaner pressure has a separate scenario.
	opts := engine.Options{IndexMode: engine.IndexBTree, ValueCacheBudgetBytes: 64 << 10}
	core := newTestDBWithHooks(t, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	db := (*tarp.DB)(core)
	probe := engine.Observe(core)
	budget := uint64(probe.Options().BTreeMemoryBudgetBytes)
	if budget != 16<<20 || liveKeys*(56+40) <= budget {
		t.Fatalf("fixture must exceed the default 16 MiB B+tree budget, got %d", budget)
	}
	order := rand.New(rand.NewPCG(112, 1)).Perm(liveKeys)
	start := time.Now()
	for i := 0; i < len(order); i += seedBatch {
		var entries [seedBatch]tarp.Entry
		for j, id := range order[i : i+seedBatch] {
			key := scaleBTreeKey(id)
			entries[j] = tarp.Entry{Key: key[:], Value: key[56:]}
		}
		if err := db.BatchPut(entries[:]); err != nil {
			t.Fatalf("seed after %d keys: %v", i, err)
		}
	}
	elapsed := time.Since(start)
	seed := probe.IndexMetrics()
	if db.Stats().LiveKeys != liveKeys || seed.Evictions == 0 || seed.SpillWrites == 0 || seed.ScratchBytes == 0 {
		t.Fatalf("shuffled seed did not produce a live spilled index: %+v", seed)
	}
	storageScaleRow(t, round, "btree", "seed", map[string]any{
		"live_keys": liveKeys, "key_bytes": 64, "value_bytes": 8, "batch_size": seedBatch,
		"budget_bytes": budget, "elapsed_ns": elapsed.Nanoseconds(), "index_after": seed,
	})

	// These Get keys, the churn victims and the new inserts are disjoint.
	// Clearing only the Value LRU leaves index page residency untouched.
	probe.ClearCache()
	readStart := windows * cycles
	samples := make([]int64, readProbe)
	cache := db.Stats().ValueCache
	start = time.Now()
	for i := range samples {
		key := scaleBTreeKey(order[readStart+i])
		begin := time.Now()
		v, err := db.Get(key[:])
		samples[i] = time.Since(begin).Nanoseconds()
		if err != nil || !bytes.Equal(v, key[56:]) {
			t.Fatalf("read probe %d: %x, %v", i, v, err)
		}
	}
	elapsed = time.Since(start)
	after := probe.IndexMetrics()
	if after.LeafReads <= seed.LeafReads || db.Stats().ValueCache.Misses-cache.Misses != readProbe {
		t.Fatal("read probe did not reload index leaves on Value LRU misses")
	}
	storageScaleRow(t, round, "btree", "value-cold-get", map[string]any{
		"live_keys": liveKeys, "latency": latency(samples, readProbe, elapsed),
		"ops_per_second": readProbe / elapsed.Seconds(), "index_before": seed, "index_after": after,
	})

	for window := range windows {
		before := probe.IndexMetrics()
		cache = db.Stats().ValueCache
		gets, deletes, puts := make([]int64, cycles), make([]int64, cycles), make([]int64, cycles)
		start = time.Now()
		for i := range cycles {
			step := window*cycles + i
			read := scaleBTreeKey(order[readStart+readProbe+step])
			victim := scaleBTreeKey(order[step])
			fresh := scaleBTreeKey(liveKeys + step)
			begin := time.Now()
			v, err := db.Get(read[:])
			gets[i] = time.Since(begin).Nanoseconds()
			if err != nil || !bytes.Equal(v, read[56:]) {
				t.Fatalf("Get at cycle %d: %x, %v", step, v, err)
			}
			begin = time.Now()
			err = db.Delete(victim[:])
			deletes[i] = time.Since(begin).Nanoseconds()
			if err != nil {
				t.Fatalf("Delete at cycle %d: %v", step, err)
			}
			begin = time.Now()
			err = db.Put(fresh[:], fresh[56:])
			puts[i] = time.Since(begin).Nanoseconds()
			if err != nil {
				t.Fatalf("Put at cycle %d: %v", step, err)
			}
		}
		elapsed = time.Since(start)
		after = probe.IndexMetrics()
		stats := db.Stats()
		if stats.LiveKeys != liveKeys || stats.ValueCache.Misses-cache.Misses != cycles ||
			after.PageReads <= before.PageReads || after.Evictions <= before.Evictions ||
			after.CachePeakBytes+after.ReadWorkspacePeakBytes > budget {
			t.Fatalf("window %d: live keys %d, value misses %d, index before %+v after %+v",
				window, stats.LiveKeys, stats.ValueCache.Misses-cache.Misses, before, after)
		}
		storageScaleRow(t, round, "btree", fmt.Sprintf("churn-%d", window+1), map[string]any{
			"live_keys": liveKeys, "cycles": cycles, "elapsed_ns": elapsed.Nanoseconds(),
			"ops_per_second": 3 * cycles / elapsed.Seconds(), "index_before": before, "index_after": after,
			"get": latency(gets, cycles, elapsed), "delete": latency(deletes, cycles, elapsed), "put": latency(puts, cycles, elapsed),
		})
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := tarp.OpenExisting(t.Context(), probe.Dir(), tarp.Options(opts))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	want := make([]bool, liveKeys+windows*cycles)
	for _, id := range order[windows*cycles:] {
		want[id] = true
	}
	for id := liveKeys; id < len(want); id++ {
		want[id] = true
	}
	seen := 0
	for entry, err := range reopened.Scan(t.Context()) {
		if err != nil || len(entry.Key) != 64 || !bytes.Equal(entry.Key[56:], entry.Value) {
			t.Fatalf("reopened entry %x = %x: %v", entry.Key, entry.Value, err)
		}
		id := binary.BigEndian.Uint64(entry.Value)
		if id >= uint64(len(want)) || !want[id] || scaleBTreeKey(int(id)) != [64]byte(entry.Key) {
			t.Fatalf("unexpected reopened key %x", entry.Key)
		}
		want[id] = false
		seen++
	}
	if seen != liveKeys {
		t.Fatalf("reopened %d entries, want %d", seen, liveKeys)
	}
}
