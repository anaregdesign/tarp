package tarp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/qualification"
	"github.com/anaregdesign/tarp/internal/storage"
)

type vmMeasureWriter func(string, int, any)

func TestVMMeasurementsGuest(t *testing.T) {
	if os.Getenv("TARP_VM_MEASURE_CONFIRM") != "disposable-vm-only" {
		t.Skip("requires explicit disposable guest measurement opt-in")
	}
	requireDisposableGuest(t)
	var cfg qualification.MeasurementConfig
	data, err := os.ReadFile("/mnt/tarp-input/measurement.json")
	if err != nil || len(data) > 1024 {
		t.Fatalf("measurement configuration: %v", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil || !qualification.MeasurementSize(cfg.ValueBytes) {
		t.Fatalf("unsupported measurement profile: %v", err)
	}
	vmMountTarget(t, true, fmt.Sprintf("measurement-%d-profile-%t", cfg.ValueBytes, cfg.Profile), 42)
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

func vmClearAppCache(db *DB) {
	db.state.Lock()
	db.cache = cache.New(db.opts.CacheBytes)
	db.state.Unlock()
}

func vmPurgeGuestCaches(t *testing.T) {
	t.Helper()
	requireDisposableGuest(t)
	syscall.Sync()
	if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3\n"), 0200); err != nil {
		t.Fatal(err)
	}
}

func vmStartupData(t *testing.T, db *DB, elapsed time.Duration, cold bool) map[string]any {
	t.Helper()
	scanned := db.store.Current.ActiveEnd - 64
	for _, file := range db.store.Catalog.Files {
		scanned += file.Bytes - 64
	}
	return map[string]any{
		"elapsed_ns": elapsed.Nanoseconds(), "selected_record_scan_bytes": scanned,
		"guest_page_cache_purged_before_open": cold,
		"note":                                "encoded committed prefix bytes, not a physical I/O counter; cold Opens additionally have independent NBD read intervals; no oracle input",
		"resources":                           vmResources(t, db),
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

func vmResources(t *testing.T, db *DB) map[string]any {
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
		if !strings.HasPrefix(link, db.store.Dir+"/DATA-") {
			continue
		}
		info, err := os.ReadFile("/proc/self/fdinfo/" + entry.Name())
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
	logical, allocated, err := measurementDiskUsage(db.store.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var live, dataBytes, pinned int64
	selected := map[uint64]bool{db.store.Current.ActiveID: true}
	for _, file := range db.store.Catalog.Files {
		selected[file.ID] = true
	}
	for _, loc := range db.index {
		live += loc.Bytes
	}
	for id, file := range db.store.Files {
		dataBytes += file.Size.Load()
		if !selected[id] && file.References() > 0 {
			pinned += file.Size.Load()
		}
	}
	sample["live_keys"], sample["cache_logical_bytes"] = len(db.index), db.cache.Bytes()
	sample["cache_limit_bytes"], sample["read_handle_limit"] = db.opts.CacheBytes, db.opts.ReadFileHandles
	sample["process_fds_including_sampler"], sample["read_data_fds"], sample["write_data_fds"] = len(fds), readFDs, writeFDs
	sample["directory_logical_file_bytes"], sample["directory_allocated_bytes"] = logical, allocated
	sample["live_encoded_record_bytes"], sample["data_file_bytes"] = live, dataBytes
	sample["obsolete_encoded_bytes"] = max(int64(0), dataBytes-int64(len(db.store.Files))*64-live)
	sample["retired_pinned_file_bytes"], sample["filesystem_available_bytes"] = pinned, vmAvailable(t)
	return sample
}

func vmCompleteIndexRelease(t *testing.T, db *DB) map[string]any {
	t.Helper()
	index := db.index // Retain the actual complete rebuilt map, never a clone.
	keys, files := len(index), len(db.store.Files)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	runtime.KeepAlive(index)
	index, db.store.Files = nil, nil
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(db)
	delta := int64(before.HeapAlloc) - int64(after.HeapAlloc)
	return map[string]any{
		"live_keys": keys, "source_file_metadata_entries": files,
		"before_heap_bytes": before.HeapAlloc, "after_heap_bytes": after.HeapAlloc,
		"complete_index_and_registry_retained_heap_delta_bytes": delta,
		"complete_index_and_registry_bytes_per_live_key":        float64(delta) / float64(keys),
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
	opts.CacheBytes, opts.MaxDataFileBytes, opts.ReadFileHandles = 64<<10, 1<<20, 4
	hooks := &measurementHooks{}
	dir := "/mnt/tarp-data/working-set"
	db, err := openWithIO(dir, opts, &storage.IO{Hook: hooks.hook})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
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
		"guest_cpuinfo": string(cpu), "guest_meminfo": string(memory), "options": opts,
		"working_keys": len(keys), "hot_keys": 8, "key_bytes": 16, "repetitions": 5,
		"read_samples": 8192, "mixed_samples": 4096, "durable_samples": 64,
		"cold_get_samples_per_rep": 32, "cold_open_samples_per_history_state": 5,
		"seed": 42, "dataset": "one prepared 4096-key set, five explicitly warmed windows; maintenance adds one writer key",
		"instrumentation": "plain build; time.Now per sampled operation; 5 ms sampled peaks for all interference modes",
		"sync_policy":     "data Sync + temporary CURRENT Sync + directory Sync; rotation/checkpoint retain all extra barriers",
		"cold_scope":      "guest page cache dropped only in this guarded VM; host caches/physical media remain unpurged",
	})
	hot := keys[:8]
	get := func(key []byte) error {
		got, err := db.Get(key)
		runtime.KeepAlive(got)
		return err
	}
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
			"operations": 10000, "wall_ns": elapsed.Nanoseconds(), "ns_per_op": float64(elapsed.Nanoseconds()) / 10000,
			"allocated_bytes_per_op": float64(afterMemory.TotalAlloc-beforeMemory.TotalAlloc) / 10000,
			"allocations_per_op":     allocations, "allocation_probe_gomaxprocs": 1,
			"storage_reads": hooks.reads.Load() - before, "returned_value_copy_included": true,
		})
		if hooks.reads.Load() != before {
			t.Fatal("qualified cache hits performed storage reads")
		}
		vmClearAppCache(db)
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
		mixed := measureCalls(t, 4096, 4, func(i int) error {
			key := keys[(uint64(i)*2654435761)%uint64(len(keys))]
			if i%20 == 0 {
				return db.Put(key, value)
			}
			return get(key)
		})
		var readSamples, writeSamples []int64
		for i, sample := range mixed.SamplesNS {
			if i%20 == 0 {
				writeSamples = append(writeSamples, sample)
			} else {
				readSamples = append(readSamples, sample)
			}
		}
		vmMeasureLatency(write, "mixed_95r_5w", repetition, mixed)
		vmMeasureLatency(write, "mixed_reads", repetition, latency(readSamples, int64(len(readSamples)), time.Duration(mixed.WallNS)))
		vmMeasureLatency(write, "mixed_durable_writes", repetition, latency(writeSamples, int64(len(writeSamples)), time.Duration(mixed.WallNS)))
		for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "slow-backup-compact"} {
			t.Run(fmt.Sprintf("interference-%d-%s", repetition, mode), func(t *testing.T) {
				for _, key := range hot {
					if err := get(key); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Put(hot[0], value); err != nil {
					t.Fatal(err)
				}
				data := measureInterferenceFor(t, db, hooks, hot, value, mode, 500*time.Millisecond)
				for _, name := range []string{"read", "durable_put"} {
					vmMeasureLatency(write, "interference_"+mode+"_"+name, repetition, data[name].(sampledLatency))
					delete(data, name)
				}
				write("interference_"+mode, repetition, data)
			})
		}
		for _, rotate := range []bool{false, true} {
			t.Run(fmt.Sprintf("durable-%d-%t", repetition, rotate), func(t *testing.T) {
				writeOpts := opts
				if rotate {
					writeOpts.MaxRecordBytes = int64(48 + 16 + size)
					writeOpts.MaxDataFileBytes = writeOpts.MaxRecordBytes + 64
				}
				writer := newTestDB(t, writeOpts, nil)
				result := measureCalls(t, 64, 1, func(int) error { return writer.Put(keys[0], value) })
				vmMeasureLatency(write, fmt.Sprintf("durable_put_rotate_%t", rotate), repetition, result)
				if rotate {
					raw := make([]int64, 64)
					var elapsed time.Duration
					for i := range raw {
						if err := writer.Put(keys[0], value); err != nil {
							t.Fatal(err)
						}
						id := writer.store.Current.ActiveID
						start := time.Now()
						if err := writer.Delete(keys[0]); err != nil {
							t.Fatal(err)
						}
						raw[i] = time.Since(start).Nanoseconds()
						elapsed += time.Duration(raw[i])
						if writer.store.Current.ActiveID == id {
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
					vmMeasureLatency(write, "durable_delete_rotate_false", repetition,
						measureCalls(t, 64, 1, func(i int) error { return writer.Delete(keys[i]) }))
				}
			})
		}
		write("resources", repetition, vmResources(t, db))
		cold := make([]int64, 32)
		for i := range cold {
			vmClearAppCache(db)
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
	for _, compacted := range []bool{false, true} {
		if compacted {
			db, err = Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Compact(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		for repetition := 1; repetition <= 5; repetition++ {
			start := time.Now()
			current, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			elapsed := time.Since(start)
			write(fmt.Sprintf("startup_history_compacted_%t", compacted), repetition, vmStartupData(t, current, elapsed, false))
			write(fmt.Sprintf("complete_index_compacted_%t", compacted), repetition, vmCompleteIndexRelease(t, current))
			vmPurgeGuestCaches(t)
			vmDeviceInterval(t, &interval, "cold_open", func() {
				start := time.Now()
				current, err = Open(dir, opts)
				elapsed = time.Since(start)
				if err != nil {
					t.Fatal(err)
				}
			})
			write(fmt.Sprintf("startup_history_cold_compacted_%t", compacted), repetition, vmStartupData(t, current, elapsed, true))
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
