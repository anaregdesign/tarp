//go:build tarpprofile

package benchmark

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

func vmProfileMeasurements(t *testing.T, size int, write vmMeasureWriter) {
	t.Helper()
	guesttest.RequireDisposableGuest(t)
	keys, value, opts := measurementInput(size, 512)
	opts.CacheBytes, opts.TargetSegmentBytes, opts.ReadFileHandles = 64<<10, 1<<20, 4
	hooks := &measurementHooks{}
	db, err := engine.OpenWithIO("/mnt/tarp-data/diagnostic", opts, &storage.IO{Hook: hooks.hook})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	defer engine.ProfileLocks(false)
	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fixture := "/mnt/tarp-data/diagnostic-fixture"
	measurementPrepareFixture(t, engine.Observe(db).Dir(), fixture, opts, keys, value)
	write("diagnostic_environment", 0, map[string]any{
		"working_keys": 512, "hot_keys": 8, "options": engine.Observe(db).Options(), "go": runtime.Version(),
		"primary_latency_baseline": false, "mutex_build_tag": "tarpprofile",
		"hold_scope":      "underlying Lock returned through Unlock entry; recording overhead excluded but extends actual lock hold; count/sum/max cover all observations",
		"raw_hold_scope":  "at most 1024 evenly spaced samples from the bounded first 65536 retained spans per mutex/window",
		"observer_effect": "all modes include 5 ms resource/copy inventory acquisitions; preparation and restore verification are outside the measured lock window",
		"fixture":         "fresh private closed-fixture copy per mode/window; 512 seeded keys plus 512 updates and 128 deletes/reinserts",
	})
	for repetition := 1; repetition <= 5; repetition++ {
		for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "slow-backup-compact"} {
			t.Run(fmt.Sprintf("holds-%d-%s", repetition, mode), func(t *testing.T) {
				db, hooks := measurementOpenClone(t, fixture, opts)
				defer engine.ProfileLocks(false)
				hooks.windowStart = func() { engine.ProfileLocks(true) }
				hooks.windowEnd = func() { engine.ProfileLocks(false) }
				probe := engine.Observe(db)
				probe.ResetLockProfile()
				measureInterferenceFor(t, db, hooks, keys[:8], value, mode, 500*time.Millisecond)
				for name, stats := range probe.LockProfile() {
					var raw []int64
					step := max(1, (len(stats.Samples)+1023)/1024)
					for i := 0; i < len(stats.Samples); i += step {
						raw = append(raw, stats.Samples[i])
					}
					count, total, maximum, retained := stats.Count, stats.TotalNS, stats.MaximumNS, len(stats.Samples)
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

func vmPendingMemory(t *testing.T, opts engine.Options, value []byte, callers, repetition int, write vmMeasureWriter) {
	t.Helper()
	var copied atomic.Int64
	db := newTestDB(t, opts, &storage.IO{PutCopied: func(keyBytes, valueBytes int) {
		copied.Add(int64(keyBytes + valueBytes))
	}})
	inputs := make([][]byte, callers)
	for i := range inputs {
		inputs[i] = []byte(fmt.Sprintf("%016x", i))
	}
	results := make(chan error, callers)
	var workers sync.WaitGroup
	probe := engine.Observe(db)
	release := probe.HoldWriter()
	locked := true
	defer func() {
		if locked {
			release()
		}
	}()
	runtime.GC()
	before := vmProcessMemory(t)
	for _, key := range inputs {
		workers.Go(func() { results <- db.Put(key, value) })
	}
	deadline := time.Now().Add(15 * time.Second)
	for probe.WaitingWriters() != int64(callers) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if probe.WaitingWriters() != int64(callers) {
		t.Fatal("not every pending Put reached the writer queue")
	}
	runtime.GC()
	after := vmProcessMemory(t)
	waiters, queuedBytes := probe.WaitingWriters(), copied.Load()
	release()
	locked = false
	workers.Wait()
	for i := 0; i < callers; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	totalInput := int64(callers * (len(inputs[0]) + len(value)))
	if queuedBytes != 0 || copied.Load() != totalInput {
		t.Fatalf("Put input copies: %d bytes queued, %d bytes after completion, want 0 and %d", queuedBytes, copied.Load(), totalInput)
	}
	write("pending_owned_buffers", repetition, map[string]any{
		"callers": callers, "foreground_writer_waiters": waiters,
		"owned_key_value_bytes": queuedBytes, "copied_bytes_after_completion": copied.Load(),
		"before": before, "all_callers_queued": after, "cache_limit_bytes": opts.CacheBytes,
		"scope": "observed index/cache input copies while the writer gate was held and after all Puts completed; process memory also includes caller and goroutine costs",
	})
}
