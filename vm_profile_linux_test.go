//go:build tarpprofile

package tarp

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

func vmProfileMeasurements(t *testing.T, size int, write vmMeasureWriter) {
	t.Helper()
	requireDisposableGuest(t)
	keys, value, opts := measurementInput(size, 512)
	opts.CacheBytes, opts.MaxDataFileBytes, opts.ReadFileHandles = 64<<10, 1<<20, 4
	hooks := &measurementHooks{}
	db, err := openWithIO("/mnt/tarp-data/diagnostic", opts, &storage.IO{Hook: hooks.hook})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer profileLocks.Store(false)
	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	write("diagnostic_environment", 0, map[string]any{
		"working_keys": 512, "hot_keys": 8, "options": opts, "go": runtime.Version(),
		"primary_latency_baseline": false, "mutex_build_tag": "tarpprofile",
		"hold_scope":      "underlying Lock returned through Unlock entry; recording overhead excluded but extends actual lock hold; count/sum/max cover all observations",
		"raw_hold_scope":  "at most 1024 evenly spaced samples from the bounded first 65536 retained spans per mutex/window",
		"observer_effect": "all modes include the 5 ms resource sampler, whose writer-lock inventory acquisitions are included",
	})
	for repetition := 1; repetition <= 5; repetition++ {
		for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "slow-backup-compact"} {
			t.Run(fmt.Sprintf("holds-%d-%s", repetition, mode), func(t *testing.T) {
				for _, key := range keys[:8] {
					if _, err := db.Get(key); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Put(keys[0], value); err != nil {
					t.Fatal(err)
				}
				for _, mutex := range []*engineMutex{&db.writer, &db.state, &db.life} {
					mutex.metrics.Lock()
					mutex.samples, mutex.count, mutex.sum, mutex.maximum = nil, 0, 0, 0
					mutex.metrics.Unlock()
				}
				profileLocks.Store(true)
				measureInterferenceFor(t, db, hooks, keys[:8], value, mode, 500*time.Millisecond)
				profileLocks.Store(false)
				for name, mutex := range map[string]*engineMutex{"writer": &db.writer, "state": &db.state, "lifecycle": &db.life} {
					mutex.metrics.Lock()
					var raw []int64
					step := max(1, (len(mutex.samples)+1023)/1024)
					for i := 0; i < len(mutex.samples); i += step {
						raw = append(raw, mutex.samples[i])
					}
					count, total, maximum, retained := mutex.count, mutex.sum, mutex.maximum, len(mutex.samples)
					mutex.metrics.Unlock()
					mean := float64(0)
					if count != 0 {
						mean = float64(total) / float64(count)
					}
					write("lock_hold_"+mode+"_"+name, repetition, map[string]any{
						"observations": count, "total_hold_ns": total, "mean_hold_ns": mean,
						"maximum_hold_ns": maximum, "retained_prefix_count": retained,
						"sample": latency(raw, int64(count), 0), "diagnostic_only": true,
					})
				}
			})
		}
		for _, callers := range []int{1, 8, 32} {
			t.Run(fmt.Sprintf("pending-%d-%d", repetition, callers), func(t *testing.T) {
				vmPendingMemory(t, opts, value, callers, repetition, write)
			})
		}
	}
}

func vmPendingMemory(t *testing.T, opts Options, value []byte, callers, repetition int, write vmMeasureWriter) {
	t.Helper()
	db := newTestDB(t, opts, nil)
	inputs := make([][]byte, callers)
	for i := range inputs {
		inputs[i] = []byte(fmt.Sprintf("%016x", i))
	}
	results := make(chan error, callers)
	var workers sync.WaitGroup
	db.writer.Lock()
	locked := true
	defer func() {
		if locked {
			db.writer.Unlock()
		}
	}()
	runtime.GC()
	before := vmProcessMemory(t)
	for _, key := range inputs {
		workers.Go(func() { results <- db.Put(key, value) })
	}
	deadline := time.Now().Add(15 * time.Second)
	for db.writer.waiters.Load() != int64(callers) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if db.writer.waiters.Load() != int64(callers) {
		t.Fatal("not every pending Put reached the post-copy writer queue")
	}
	runtime.GC()
	after := vmProcessMemory(t)
	write("pending_owned_buffers", repetition, map[string]any{
		"callers": callers, "writer_waiters": db.writer.waiters.Load(),
		"owned_key_value_bytes": callers * (16 + len(value)),
		"before":                before, "all_callers_queued": after, "cache_limit_bytes": opts.CacheBytes,
		"scope": "exact logical copied key/value bytes after admission; process deltas also include goroutine/closure/stack costs, not solely engine heap and not a hard bound",
	})
	db.writer.Unlock()
	locked = false
	workers.Wait()
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
