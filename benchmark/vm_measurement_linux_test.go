package benchmark

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/benchmark/qualification"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

type vmMeasureWriter func(string, int, any)

func TestVMMeasurementsGuest(t *testing.T) {
	if os.Getenv("TARP_VM_MEASURE_CONFIRM") != "disposable-vm-only" {
		t.Skip("requires explicit disposable guest measurement opt-in")
	}
	guesttest.RequireDisposableGuest(t)
	var cfg qualification.MeasurementConfig
	data, err := os.ReadFile("/mnt/tarp-input/measurement.json")
	if err != nil || len(data) > 1024 {
		t.Fatalf("measurement configuration: %v", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil || !qualification.MeasurementSize(cfg.ValueBytes) {
		t.Fatalf("unsupported measurement profile: %v", err)
	}
	guesttest.MountTarget(t, true, fmt.Sprintf("measurement-%d-profile-%t", cfg.ValueBytes, cfg.Profile), 42)
	t.Setenv("TMPDIR", "/mnt/tarp-data")
	rows := 0
	write := func(kind string, repetition int, value any) {
		t.Helper()
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		row, err := json.Marshal(qualification.MeasurementRow{
			Kind: kind, ValueBytes: cfg.ValueBytes, Repetition: repetition, Data: body,
		})
		if err != nil || len(row) > 60<<10 {
			t.Fatalf("bounded measurement row: %d bytes %v", len(row), err)
		}
		fmt.Printf("TARP_MEASUREMENT %s\n", row)
		rows++
	}
	verdict := qualification.MeasurementVerdict{
		ValueBytes: cfg.ValueBytes, Profile: cfg.Profile,
		WorkingKeys: qualification.MeasurementKeys, Repetitions: qualification.MeasurementReps,
	}
	if cfg.Profile {
		verdict.WorkingKeys = 512
		vmProfileMeasurements(t, cfg.ValueBytes, write)
	} else {
		vmPrimaryMeasurements(t, cfg.ValueBytes, write)
		verdict.ColdCalls, verdict.ColdOpens, verdict.HistoryMutations = 32*qualification.MeasurementReps, 10, 10240
	}
	verdict.Rows = rows
	result, err := json.Marshal(verdict)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("TARP_MEASURE_PASS %s\n", result)
}

func vmMeasureLatency(write vmMeasureWriter, kind string, repetition int, data sampledLatency) {
	raw := data.SamplesNS
	data.SamplesNS = nil
	write(kind, repetition, data)
	for offset := 0; offset < len(raw); offset += 1024 {
		write(kind+"_raw", repetition, map[string]any{
			"offset": offset, "samples_ns": raw[offset:min(offset+1024, len(raw))],
		})
	}
}

func vmPurgeGuestCaches(t *testing.T) {
	t.Helper()
	guesttest.RequireDisposableGuest(t)
	syscall.Sync()
	if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0200); err != nil {
		t.Fatal(err)
	}
}

func vmStartupData(t *testing.T, db *engine.DB, elapsed time.Duration, cold bool) map[string]any {
	t.Helper()
	row := startupRow(elapsed, vmResources(t, db))
	row["guest_page_cache_purged_before_open"] = cold
	return row
}

func vmParallelHitBatch(t *testing.T, get func([]byte) error, keys [][]byte) map[string]any {
	t.Helper()
	const workers, operations = 4, 10000
	start := make(chan struct{})
	results := make(chan error, workers)
	var ready, done sync.WaitGroup
	ready.Add(workers)
	for worker := 0; worker < workers; worker++ {
		done.Go(func() {
			err := get(keys[worker%len(keys)])
			ready.Done()
			if err != nil {
				results <- err
				return
			}
			<-start
			for i := worker; i < operations; i += workers {
				if err := get(keys[i%len(keys)]); err != nil {
					results <- err
					return
				}
			}
		})
	}
	ready.Wait()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	begin := time.Now()
	close(start)
	done.Wait()
	elapsed := time.Since(begin)
	runtime.ReadMemStats(&after)
	close(results)
	for err := range results {
		t.Fatal(err)
	}
	return map[string]any{
		"workers": workers, "operations": operations, "wall_ns": elapsed.Nanoseconds(),
		"ns_per_op":                    float64(elapsed.Nanoseconds()) / operations,
		"allocated_bytes_per_op":       float64(after.TotalAlloc-before.TotalAlloc) / operations,
		"allocations_per_op":           float64(after.Mallocs-before.Mallocs) / operations,
		"returned_value_copy_included": true,
		"allocation_scope":             "process deltas; workers/channels are prestarted/preallocated and each worker warms its stack before snapshots; release/wait, goroutine completion and concurrent runtime costs remain included",
	}
}

func vmProcessMemory(t *testing.T) map[string]any {
	t.Helper()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	statm, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(statm))
	if len(fields) < 2 {
		t.Fatal("incomplete Linux process memory accounting")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return map[string]any{
		"heap_alloc_bytes": memory.HeapAlloc, "heap_inuse_bytes": memory.HeapInuse,
		"heap_idle_bytes": memory.HeapIdle, "heap_released_bytes": memory.HeapReleased,
		"runtime_sys_bytes": memory.Sys, "stack_inuse_bytes": memory.StackInuse,
		"process_current_rss_bytes": pages * uint64(os.Getpagesize()), "process_peak_rss_bytes": usage.Maxrss * 1024,
		"goroutines": runtime.NumGoroutine(), "gc_count": memory.NumGC, "gc_pause_total_ns": memory.PauseTotalNs,
	}
}

func vmResources(t *testing.T, db *engine.DB) map[string]any {
	t.Helper()
	runtime.GC()
	sample := vmProcessMemory(t)
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	readFDs, writeFDs := 0, 0
	for _, entry := range fds {
		link, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(link, engine.Observe(db).Dir()+"/DATA-") {
			continue
		}
		info, err := os.ReadFile("/proc/self/fdinfo/" + entry.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue // An active cleaner or reader can release its descriptor between probes.
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(info), "\n") {
			parts := strings.Fields(line)
			if len(parts) == 2 && parts[0] == "flags:" {
				flags, err := strconv.ParseUint(parts[1], 8, 64)
				if err != nil {
					t.Fatal(err)
				}
				if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
					readFDs++
				} else {
					writeFDs++
				}
			}
		}
	}
	logical, allocated, err := measurementDiskUsage(engine.Observe(db).Dir())
	if err != nil {
		t.Fatal(err)
	}
	sample["layout"] = observedLayout(db)
	stats := db.Stats()
	sample["live_keys"], sample["cache_logical_bytes"] = stats.LiveKeys, stats.ValueCache.UsedBytes
	sample["index_mode"], sample["index_metrics"] = engine.Observe(db).Options().IndexMode, engine.Observe(db).IndexMetrics()
	sample["cache_limit_bytes"], sample["read_handle_limit"] = engine.Observe(db).Options().ValueCacheBudgetBytes, engine.Observe(db).Options().ReadFileHandles
	sample["process_fds_including_sampler"], sample["read_data_fds"], sample["write_data_fds"] = len(fds), readFDs, writeFDs
	sample["directory_logical_file_bytes"], sample["directory_allocated_bytes"] = logical, allocated
	sample["filesystem_available_bytes"] = guesttest.AvailableBytes(t)
	return sample
}

func vmCompleteIndexRelease(t *testing.T, db *engine.DB) map[string]any {
	t.Helper()
	probe := engine.Observe(db)
	if probe.Options().IndexMode == engine.IndexBTree {
		result := map[string]any{"index_mode": probe.Options().IndexMode, "index_metrics": probe.IndexMetrics(), "live_keys": probe.LiveKeys(), "scope": "B+tree charged pages and scratch file space sampled before Close"}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	index, registry, err := probe.CloseAndRetainMemoryIndex()
	if err != nil {
		t.Fatal(err)
	}
	keys, files := len(index), len(registry)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	runtime.KeepAlive(index)
	runtime.KeepAlive(registry)
	index, registry = nil, nil
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(db)
	delta := int64(before.HeapAlloc) - int64(after.HeapAlloc)
	var bytesPerLiveKey *float64
	if keys > 0 {
		perKey := float64(delta) / float64(keys)
		bytesPerLiveKey = &perKey
	}
	return map[string]any{
		"index_mode": probe.Options().IndexMode, "live_keys": keys, "source_file_metadata_entries": files,
		"before_heap_bytes": before.HeapAlloc, "after_heap_bytes": after.HeapAlloc,
		"complete_index_and_registry_retained_heap_delta_bytes": delta,
		"complete_index_and_registry_bytes_per_live_key":        bytesPerLiveKey,
		"scope": "actual rebuilt key map, owned keys, inline locations and shared file registry released after closing cache/handles; fixed DB/catalog/frontier excluded; raw GC delta, not a process bound",
	}
}

func vmDeviceInterval(t *testing.T, next *int, kind string, call func()) {
	t.Helper()
	id := *next
	(*next)++
	await := func(stage string) {
		path := fmt.Sprintf("/mnt/tarp-input/device-%03d-%s", id, stage)
		end := time.Now().Add(30 * time.Second)
		for time.Now().Before(end) {
			data, err := os.ReadFile(path)
			if err == nil && string(data) == "ready" {
				return
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("external device-counter acknowledgment timed out")
	}
	fmt.Printf("TARP_DEVICE_BEGIN %d %s\n", id, kind)
	await("begin")
	call()
	fmt.Printf("TARP_DEVICE_END %d %s\n", id, kind)
	await("end")
}

func vmPrimaryMeasurements(t *testing.T, size int, write vmMeasureWriter) {
	t.Helper()
	keys, value, opts := measurementInput(size, qualification.MeasurementKeys)
	opts.ValueCacheBudgetBytes, opts.TargetSegmentBytes, opts.ReadFileHandles = 64<<10, 1<<20, 4
	hooks := &measurementHooks{}
	dir := "/mnt/tarp-data/working-set"
	db, err := engine.OpenWithIO(dir, opts, &storage.IO{Hook: hooks.hook})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fixture := "/mnt/tarp-data/interference-fixture"
	measurementPrepareFixture(t, dir, fixture, opts, keys, value)
	db, err = engine.OpenWithIO(dir, opts, &storage.IO{Hook: hooks.hook})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	cpu, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		t.Fatal(err)
	}
	memory, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		t.Fatal(err)
	}
	write("environment", 0, map[string]any{
		"go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0),
		"storage_format": "unversioned", "guest_cpuinfo": string(cpu), "guest_meminfo": string(memory), "options": engine.Observe(db).Options(),
		"working_keys": len(keys), "hot_keys": 8, "key_bytes": 16, "repetitions": 5,
		"read_samples": 8192, "mixed_samples": 4096, "durable_samples": 64,
		"cold_get_samples_per_rep": 32, "cold_open_samples_per_history_state": 5,
		"seed": 42, "dataset": "one prepared 4096-key set, five explicitly warmed windows; maintenance adds one writer key",
		"interference_fixture": "fresh private closed-fixture copy for every mode/window; same 4096 seeded keys plus 1024 updates, 256 deletes/reinserts; full contents witnessed before workers; fixtures removed after each mode",
		"instrumentation":      "plain build; time.Now per sampled operation; 5 ms sampled peaks for all interference modes",
		"sync_policy":          "data Sync + temporary CURRENT Sync + directory Sync; rotation/checkpoint retain all extra barriers",
		"cold_scope":           "guest page cache dropped only in this guarded VM; host caches/physical media remain unpurged",
	})
	hot := keys[:8]
	get := func(key []byte) error {
		got, err := db.Get(key)
		runtime.KeepAlive(got)
		return err
	}
	scanWork := newScanWorkload(keys, value)
	interval := 0
	for repetition := 1; repetition <= qualification.MeasurementReps; repetition++ {
		for _, key := range hot {
			if err := get(key); err != nil {
				t.Fatal(err)
			}
		}
		before := hooks.reads.Load()
		for _, workers := range []int{1, 4} {
			result := measureCalls(t, 8192, workers, func(i int) error { return get(hot[i%len(hot)]) })
			vmMeasureLatency(write, fmt.Sprintf("cache_hit_%d_worker", workers), repetition, result)
		}
		var beforeMemory, afterMemory runtime.MemStats
		runtime.ReadMemStats(&beforeMemory)
		start := time.Now()
		for i := 0; i < 10000; i++ {
			if err := get(hot[i%len(hot)]); err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&afterMemory)
		allocations := testing.AllocsPerRun(1000, func() {
			if err := get(hot[0]); err != nil {
				t.Fatal(err)
			}
		})
		write("cache_hit_batch", repetition, map[string]any{
			"workers": 1, "operations": 10000, "wall_ns": elapsed.Nanoseconds(), "ns_per_op": float64(elapsed.Nanoseconds()) / 10000,
			"allocated_bytes_per_op": float64(afterMemory.TotalAlloc-beforeMemory.TotalAlloc) / 10000,
			"allocations_per_op":     allocations, "allocation_probe_gomaxprocs": 1,
			"storage_reads": hooks.reads.Load() - before, "returned_value_copy_included": true,
		})
		write("cache_hit_batch_4_worker", repetition, vmParallelHitBatch(t, get, hot))
		if hooks.reads.Load() != before {
			t.Fatal("qualified cache hits performed storage reads")
		}
		engine.Observe(db).ClearCache()
		for _, key := range keys {
			if err := get(key); err != nil {
				t.Fatal(err)
			}
		}
		before = hooks.reads.Load()
		var warm sampledLatency
		vmDeviceInterval(t, &interval, "warm", func() {
			warm = measureCalls(t, 8192, 1, func(i int) error { return get(keys[i%len(keys)]) })
		})
		vmMeasureLatency(write, "application_miss_os_warm", repetition, warm)
		if hooks.reads.Load()-before != 8192 {
			t.Fatal("qualified application-miss window included cache hits")
		}
		mixed := measureMixed(t, db, keys, value, true)
		vmMeasureLatency(write, "mixed_multi_writer", repetition, mixed.all)
		vmMeasureLatency(write, "mixed_multi_writer_reads", repetition, mixed.reads)
		vmMeasureLatency(write, "mixed_multi_writer_puts", repetition, mixed.puts)
		write("mixed_multi_writer_schedule", repetition, mixed.schedule)
		for _, rotate := range []bool{false, true} {
			t.Run(fmt.Sprintf("durable-%d-%t", repetition, rotate), func(t *testing.T) {
				writeOpts := measurementWriteOptions(opts, len(keys[0]), size, rotate)
				writer := newTestDB(t, writeOpts, nil)
				if err := writer.Put(keys[0], value); err != nil {
					t.Fatal(err)
				}
				putRaw := make([]int64, 64)
				var putElapsed time.Duration
				for i := range putRaw {
					id := engine.Observe(writer).Frontier().ActiveID
					start := time.Now()
					if err := writer.Put(keys[0], value); err != nil {
						t.Fatal(err)
					}
					putRaw[i] = time.Since(start).Nanoseconds()
					putElapsed += time.Duration(putRaw[i])
					if (engine.Observe(writer).Frontier().ActiveID != id) != rotate {
						t.Fatal("durable Put rotation did not match its declared mode")
					}
				}
				vmMeasureLatency(write, fmt.Sprintf("durable_put_rotate_%t", rotate), repetition, latency(putRaw, 64, putElapsed))
				write(fmt.Sprintf("durable_put_rotation_checks_%t", rotate), repetition, map[string]any{
					"effective_options": engine.Observe(writer).Options(), "prefills": 1, "timed_puts": 64, "verified_samples": 64, "rotation_expected": rotate,
					"scope": "one fully synced prefill outside timing; active IDs checked after every timed Put; wall_ns sums only timed API intervals",
				})
				if rotate {
					raw := make([]int64, 64)
					var elapsed time.Duration
					for i := range raw {
						if err := writer.Put(keys[0], value); err != nil {
							t.Fatal(err)
						}
						id := engine.Observe(writer).Frontier().ActiveID
						start := time.Now()
						if err := writer.Delete(keys[0]); err != nil {
							t.Fatal(err)
						}
						raw[i] = time.Since(start).Nanoseconds()
						elapsed += time.Duration(raw[i])
						if engine.Observe(writer).Frontier().ActiveID == id {
							t.Fatal("declared rotating Delete did not rotate its full segment")
						}
					}
					vmMeasureLatency(write, "durable_delete_rotate_true", repetition, latency(raw, 64, elapsed))
					write("durable_delete_prefill", repetition, map[string]any{
						"prefills": 64, "timed_deletes": 64, "verified_rotations": 64,
						"scope": "fully synced Put fills the active segment outside each timed Delete; wall_ns sums only timed Delete intervals",
					})
				} else {
					for _, key := range keys[:64] {
						if err := writer.Put(key, value); err != nil {
							t.Fatal(err)
						}
					}
					id := engine.Observe(writer).Frontier().ActiveID
					deletes := measureCalls(t, 64, 1, func(i int) error { return writer.Delete(keys[i]) })
					if engine.Observe(writer).Frontier().ActiveID != id {
						t.Fatal("declared nonrotating Delete sample rotated")
					}
					vmMeasureLatency(write, "durable_delete_rotate_false", repetition, deletes)
				}
			})
		}
		write("resources", repetition, vmResources(t, db))
		cold := make([]int64, 32)
		for i := range cold {
			engine.Observe(db).ClearCache()
			vmPurgeGuestCaches(t)
			vmDeviceInterval(t, &interval, "cold", func() {
				start := time.Now()
				if err := get(keys[2048+i*37]); err != nil {
					t.Fatal(err)
				}
				cold[i] = time.Since(start).Nanoseconds()
			})
		}
		vmMeasureLatency(write, "guest_os_cold_virtual_device_get", repetition, latency(cold, int64(len(cold)), 0))
		scanWork.traverse(t, db) // Warm the full working set outside the measured traversal.
		write("scan_full_warm", repetition, scanWork.traverse(t, db))
	}
	for i := 0; i < 8192; i++ {
		if err := db.Put(keys[i%1024], value); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range keys[:1024] {
		if err := db.Delete(key); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range keys[:1024] {
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	write("history", 0, map[string]any{"upserts": 8192, "deletes": 1024, "reinserts": 1024, "hot_history_keys": 1024})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Run the private maintenance fixtures after the workload DB has joined its
	// cleaner so unrelated background I/O cannot contaminate the control windows.
	compactionFixture := prepareCompactionFixture(t, size)
	repack := prepareRepackFixture(t, size)
	for repetition := 1; repetition <= qualification.MeasurementReps; repetition++ {
		for _, mode := range compactionModes {
			write("compaction_"+mode, repetition, compactionFixture.measure(t, mode))
		}
		write("scan_paused_checkpoint", repetition, measurePausedScan(t, repack))
		for _, mode := range repackModesFor(repetition) {
			write("repack_"+mode, repetition, repack.measure(t, mode))
		}
		for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", pinnedBackupMode} {
			t.Run(fmt.Sprintf("interference-%d-%s", repetition, mode), func(t *testing.T) {
				db, hooks := measurementOpenClone(t, fixture, opts)
				data := measureInterferenceFor(t, db, hooks, hot, value, mode, 500*time.Millisecond)
				for _, name := range []string{"read", "durable_put"} {
					vmMeasureLatency(write, "interference_"+mode+"_"+name, repetition, data[name].(sampledLatency))
					delete(data, name)
				}
				write("interference_"+mode, repetition, data)
			})
		}
	}
	for _, state := range []string{"default_history", "manual_checkpoint"} {
		if state == "manual_checkpoint" {
			measurementCheckpointFixture(t, dir, opts)
		}
		for repetition := 1; repetition <= qualification.MeasurementReps; repetition++ {
			for _, cold := range []bool{false, true} {
				t.Run(fmt.Sprintf("startup-%s-%d-cold-%t", state, repetition, cold), func(t *testing.T) {
					// Each trial owns one clone; subtest cleanup closes and removes it.
					scratch := filepath.Join(t.TempDir(), "db")
					measurementCloneClosed(t, dir, scratch)
					if cold {
						vmPurgeGuestCaches(t)
						var current *engine.DB
						var elapsed time.Duration
						vmDeviceInterval(t, &interval, "cold_open", func() {
							current, elapsed = openStartupSample(t, scratch, opts)
						})
						write("startup_cold_"+state, repetition, vmStartupData(t, current, elapsed, true))
						return
					}
					current, elapsed := openStartupSample(t, scratch, opts)
					write("startup_"+state, repetition, vmStartupData(t, current, elapsed, false))
					write("complete_index_"+state, repetition, vmCompleteIndexRelease(t, current))
				})
			}
		}
	}
}
