package tarp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func measurementInput(size, count int) ([][]byte, []byte, Options) {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%016x", i))
	}
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i*31 + size)
	}
	return keys, value, Options{
		CacheBytes: int64((128 + 16 + size) * count), MaxRecordBytes: 8 << 10,
		MaxDataFileBytes: 64 << 10, ReadFileHandles: 8,
	}
}

func benchmarkDB(b *testing.B, size int, miss bool) (*DB, [][]byte, []byte) {
	b.Helper()
	keys, value, opts := measurementInput(size, 128)
	if miss {
		opts.CacheBytes /= 16
	}
	db, err := Open(filepath.Join(b.TempDir(), "db"), opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.SetBytes(int64(size))
	return db, keys, value
}

func BenchmarkGet(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			for _, miss := range []bool{false, true} {
				name := "application-hit"
				if miss {
					name = "application-miss-os-warm"
				}
				b.Run(name, func(b *testing.B) {
					db, keys, _ := benchmarkDB(b, size, miss)
					i := 0
					for b.Loop() {
						value, err := db.Get(keys[i%len(keys)])
						if err != nil {
							b.Fatal(err)
						}
						runtime.KeepAlive(value)
						i++
					}
				})
			}
			b.Run("application-hit-parallel", func(b *testing.B) {
				db, keys, _ := benchmarkDB(b, size, false)
				b.SetParallelism(1)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						value, err := db.Get(keys[i%len(keys)])
						if err != nil {
							b.Error(err)
							return
						}
						runtime.KeepAlive(value)
						i++
					}
				})
			})
		})
	}
}

func BenchmarkDurablePut(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		for _, rotate := range []bool{false, true} {
			b.Run(fmt.Sprintf("%d/force-rotation-%t", size, rotate), func(b *testing.B) {
				keys, value, opts := measurementInput(size, 2)
				opts.MaxDataFileBytes = 1 << 20
				if rotate {
					opts.MaxRecordBytes = int64(48 + 16 + size)
					opts.MaxDataFileBytes = 64 + opts.MaxRecordBytes
				}
				db, err := Open(filepath.Join(b.TempDir(), "db"), opts)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := db.Close(); err != nil {
						b.Error(err)
					}
				})
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					if err := db.Put(keys[0], value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type sampledLatency struct {
	Operations int64   `json:"operations"`
	WallNS     int64   `json:"wall_ns"`
	P50NS      int64   `json:"p50_ns"`
	P95NS      int64   `json:"p95_ns"`
	P99NS      int64   `json:"p99_ns"`
	SamplesNS  []int64 `json:"samples_ns"`
}

func latency(samples []int64, operations int64, elapsed time.Duration) sampledLatency {
	sorted := append([]int64(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	q := func(percent int) int64 {
		if len(sorted) == 0 {
			return 0
		}
		return sorted[(len(sorted)*percent+99)/100-1]
	}
	return sampledLatency{Operations: operations, WallNS: elapsed.Nanoseconds(),
		P50NS: q(50), P95NS: q(95), P99NS: q(99), SamplesNS: samples}
}

func measureCalls(t *testing.T, samples, workers int, call func(int) error) sampledLatency {
	t.Helper()
	raw := make([]int64, samples)
	errors := make(chan error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	begin := time.Now()
	for worker := 0; worker < workers; worker++ {
		wg.Go(func() {
			<-start
			for i := worker; i < samples; i += workers {
				now := time.Now()
				err := call(i)
				raw[i] = time.Since(now).Nanoseconds()
				if err != nil {
					errors <- err
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	return latency(raw, int64(samples), time.Since(begin))
}

func diskBytes(paths ...string) (int64, error) {
	total, _, err := measurementDiskUsage(paths...)
	return total, err
}

func measurementDiskUsage(paths ...string) (int64, int64, error) {
	var total, allocated int64
	for _, root := range paths {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if errors.Is(err, os.ErrNotExist) {
				return nil // Concurrent publication/reclamation can remove a sampled name.
			}
			if err != nil {
				return err
			}
			if entry.Type().IsRegular() {
				st, err := entry.Info()
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				if err != nil {
					return err
				}
				total += st.Size()
				raw, ok := st.Sys().(*syscall.Stat_t)
				if !ok {
					return fmt.Errorf("unavailable allocated-file accounting for %s", path)
				}
				allocated += raw.Blocks * 512
			}
			return nil
		})
		if err != nil {
			return 0, 0, err
		}
	}
	return total, allocated, nil
}

func resourceSample(t *testing.T, db *DB) map[string]any {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	copyIndex := make(map[string]storage.Location, len(db.index))
	var logicalIndex int64
	for key, loc := range db.index {
		copyIndex[strings.Clone(key)] = loc
		logicalIndex += int64(len(key)) + int64(unsafe.Sizeof(key)) + int64(unsafe.Sizeof(loc))
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(copyIndex)
	var rss syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rss); err != nil {
		t.Fatal(err)
	}
	peakRSS := int64(rss.Maxrss)
	if runtime.GOOS == "linux" {
		peakRSS *= 1024
	}
	fdPath := "/dev/fd"
	if runtime.GOOS == "linux" {
		fdPath = "/proc/self/fd"
	}
	fds, err := os.ReadDir(fdPath)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := diskBytes(db.store.Dir)
	if err != nil {
		t.Fatal(err)
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(db.store.Dir, &filesystem); err != nil {
		t.Fatal(err)
	}
	var live int64
	for _, loc := range db.index {
		live += loc.Bytes
	}
	count := max(1, len(db.index))
	var dataBytes int64
	for _, file := range db.store.Files {
		dataBytes += file.Size.Load()
	}
	return map[string]any{
		"heap_alloc_bytes": before.HeapAlloc, "runtime_sys_bytes": before.Sys, "process_peak_rss_bytes": peakRSS,
		"cache_logical_bytes": db.cache.Bytes(), "cache_limit_bytes": db.opts.CacheBytes,
		"live_keys": len(db.index), "index_logical_bytes_per_key": float64(logicalIndex) / float64(count),
		"index_clone_retained_heap_delta_per_key": float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(count),
		"open_process_fds_including_sampler":      len(fds), "read_handle_limit": db.opts.ReadFileHandles,
		"physical_directory_bytes": physical, "live_encoded_record_bytes": live,
		"filesystem_type_number": fmt.Sprintf("0x%x", filesystem.Type), "filesystem_block_bytes": filesystem.Bsize,
		"filesystem_flags": fmt.Sprintf("0x%x", filesystem.Flags),
		"data_file_bytes":  dataBytes, "obsolete_record_bytes": max(int64(0), dataBytes-int64(len(db.store.Files))*64-live),
		"pending_calls": 0, "index_clone_note": "isolated map/key clone; shared file metadata excluded; GC delta is noisy, not a process limit",
	}
}

type measurementHooks struct {
	reads   atomic.Int64
	slow    atomic.Bool
	started atomic.Pointer[chan struct{}]
	overlap atomic.Pointer[chan struct{}]
}

func (h *measurementHooks) releaseOverlap() {
	if signal := h.overlap.Swap(nil); signal != nil {
		close(*signal)
	}
}

func (h *measurementHooks) hook(point string) error {
	if point == "read:before" {
		h.reads.Add(1)
	}
	if point == "compact.write:before" {
		h.releaseOverlap()
	}
	if point == "backup.copy:before" {
		gate := h.overlap.Load()
		if signal := h.started.Swap(nil); signal != nil {
			close(*signal)
		}
		if gate != nil {
			<-*gate
		}
		if h.slow.Load() {
			time.Sleep(time.Millisecond)
		}
	}
	return nil
}

func measureInterference(t *testing.T, db *DB, hooks *measurementHooks, keys [][]byte, value []byte, mode string) map[string]any {
	return measureInterferenceFor(t, db, hooks, keys, value, mode, 30*time.Millisecond)
}

func measureInterferenceFor(t *testing.T, db *DB, hooks *measurementHooks, keys [][]byte, value []byte, mode string, minimum time.Duration) map[string]any {
	t.Helper()
	root := t.TempDir()
	target := filepath.Join(root, "backup")
	start, stop := make(chan struct{}), make(chan struct{})
	type workerResult struct {
		latency sampledLatency
		err     error
		kind    string
	}
	results := make(chan workerResult, 2)
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	peakHeap := memory.HeapAlloc
	initialDisk, initialAllocated, err := measurementDiskUsage(db.store.Dir, root)
	if err != nil {
		t.Fatal(err)
	}
	peakDisk := initialDisk
	peakAllocated := initialAllocated
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(db.store.Dir, &filesystem); err != nil {
		t.Fatal(err)
	}
	minimumAvailable := filesystem.Bavail * uint64(filesystem.Bsize)
	var peakPinned int64
	begin := time.Now()
	for worker := 0; worker < 2; worker++ {
		go func() {
			rng := rand.New(rand.NewPCG(42, uint64(worker)))
			raw := make([]int64, 0, 2048)
			var count int64
			kind := "read"
			if worker != 0 {
				kind = "durable_put"
			}
			<-start
			for {
				select {
				case <-stop:
					results <- workerResult{latency: latency(raw, count, time.Since(begin)), kind: kind}
					return
				default:
				}
				now := time.Now()
				var err error
				if worker == 0 {
					var got []byte
					got, err = db.Get(keys[int(count)%len(keys)])
					runtime.KeepAlive(got)
				} else {
					err = db.Put([]byte("writer-key-00000"), value)
				}
				ns := time.Since(now).Nanoseconds()
				count++
				if len(raw) < cap(raw) {
					raw = append(raw, ns)
				} else if slot := rng.Int64N(count); slot < int64(len(raw)) {
					raw[slot] = ns
				}
				if err != nil {
					results <- workerResult{err: err}
					return
				}
			}
		}()
	}
	monitorDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			runtime.ReadMemStats(&memory)
			peakHeap = max(peakHeap, memory.HeapAlloc)
			current, allocated, err := measurementDiskUsage(db.store.Dir, root)
			if err != nil {
				monitorDone <- err
				return
			}
			peakDisk = max(peakDisk, current)
			peakAllocated = max(peakAllocated, allocated)
			if err := syscall.Statfs(db.store.Dir, &filesystem); err != nil {
				monitorDone <- err
				return
			}
			minimumAvailable = min(minimumAvailable, filesystem.Bavail*uint64(filesystem.Bsize))
			db.writer.Lock()
			selected := map[uint64]bool{db.store.Current.ActiveID: true}
			for _, file := range db.store.Catalog.Files {
				selected[file.ID] = true
			}
			var pinned int64
			for id, file := range db.store.Files {
				if !selected[id] && file.References() != 0 {
					pinned += file.Size.Load()
				}
			}
			db.writer.Unlock()
			peakPinned = max(peakPinned, pinned)
			select {
			case <-stop:
				monitorDone <- nil
				return
			case <-ticker.C:
			}
		}
	}()
	readStart := hooks.reads.Load()
	close(start)
	maintenanceStart := time.Now()
	var compact CompactResult
	var backupStart, backupEnd, compactStart, compactEnd time.Time
	compactCall := func() error {
		compactStart = time.Now()
		var err error
		compact, err = db.Compact(context.Background())
		compactEnd = time.Now()
		hooks.releaseOverlap()
		return err
	}
	maintenanceErr := func() error {
		if mode == "backup" || mode == "backup-compact" || mode == "slow-backup-compact" {
			hooks.slow.Store(mode == "slow-backup-compact")
			if mode != "backup" {
				gate := make(chan struct{})
				hooks.overlap.Store(&gate)
			}
			captured := make(chan struct{})
			hooks.started.Store(&captured)
			done := make(chan error, 1)
			go func() {
				backupStart = time.Now()
				err := db.Backup(context.Background(), target)
				backupEnd = time.Now()
				done <- err
			}()
			select {
			case <-captured:
			case err := <-done:
				hooks.releaseOverlap()
				return errors.Join(errors.New("backup returned before copy-start hook"), err)
			}
			if mode != "backup" {
				if err := compactCall(); err != nil {
					return errors.Join(err, <-done)
				}
			}
			return <-done
		}
		if mode == "compact" {
			return compactCall()
		}
		return nil
	}()
	maintenanceNS := time.Since(maintenanceStart).Nanoseconds()
	var backupCopied, compactCopied int64
	if maintenanceErr == nil && !backupStart.IsZero() {
		entries, err := os.ReadDir(target)
		maintenanceErr = err
		for _, entry := range entries {
			if !strings.HasPrefix(entry.Name(), "DATA-") {
				continue
			}
			info, err := entry.Info()
			if err != nil {
				maintenanceErr = errors.Join(maintenanceErr, err)
				break
			}
			backupCopied += info.Size()
		}
	}
	if maintenanceErr == nil && !compactStart.IsZero() {
		db.writer.Lock()
		for _, file := range db.store.Catalog.Files {
			if file.Kind == format.Snapshot {
				compactCopied += file.Bytes
			}
		}
		db.writer.Unlock()
	}
	overlapNS := int64(0)
	if !backupStart.IsZero() && !compactStart.IsZero() {
		left, right := backupStart, backupEnd
		if compactStart.After(left) {
			left = compactStart
		}
		if compactEnd.Before(right) {
			right = compactEnd
		}
		overlapNS = max(int64(0), right.Sub(left).Nanoseconds())
	}
	if !backupStart.IsZero() && backupCopied <= 0 ||
		!compactStart.IsZero() && compactCopied <= 0 ||
		(mode == "backup-compact" || mode == "slow-backup-compact") && overlapNS <= 0 {
		maintenanceErr = errors.Join(maintenanceErr, errors.New("maintenance copy/overlap observation was empty"))
	}
	if remaining := minimum - time.Since(begin); remaining > 0 {
		time.Sleep(remaining)
	}
	close(stop)
	hooks.slow.Store(false)
	output := map[string]any{
		"maintenance_ns": maintenanceNS, "compact": compact,
		"backup_copy_bytes": backupCopied, "backup_elapsed_ns": backupEnd.Sub(backupStart).Nanoseconds(),
		"compact_copy_bytes": compactCopied, "compact_elapsed_ns": compactEnd.Sub(compactStart).Nanoseconds(),
		"observed_maintenance_api_overlap_ns": overlapNS,
		"copy_measurement_scope":              "actual selected backup DATA prefixes / published checkpoint Snapshot file lengths, including segment headers but excluding metadata; elapsed is the complete respective API call; overlap is API intervals, not a physical copy-engine utilization claim",
		"overlap_coordination":                "overlap modes pause the first backup copy until the first checkpoint record write; released on any checkpoint return; this controlled delay is included in API elapsed times",
		"get_file_reads":                      hooks.reads.Load() - readStart,
		"writer_callers":                      1, "caller_value_bytes": len(value), "sample_reservoir_limit_per_worker": 2048,
	}
	for worker := 0; worker < 2; worker++ {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		output[result.kind] = result.latency
	}
	if err := <-monitorDone; err != nil {
		t.Fatal(err)
	}
	output["sampled_peak_heap_bytes"] = peakHeap
	output["sampled_peak_additional_disk_bytes"] = max(int64(0), peakDisk-initialDisk)
	output["sampled_peak_additional_allocated_disk_bytes"] = max(int64(0), peakAllocated-initialAllocated)
	output["sampled_minimum_filesystem_available_bytes"] = minimumAvailable
	output["sampled_peak_retired_pinned_bytes"] = peakPinned
	output["minimum_window_ns"] = minimum.Nanoseconds()
	output["sampler_period_ms"] = 5
	output["sampler_takes_writer_for_file_inventory"] = true
	if hooks.reads.Load() != readStart {
		t.Fatal("declared interference cache hits performed file reads")
	}
	if maintenanceErr != nil {
		t.Fatal(maintenanceErr)
	}
	if mode == "backup" || mode == "backup-compact" || mode == "slow-backup-compact" {
		if err := Restore(context.Background(), target, filepath.Join(root, "restored"), db.opts); err != nil {
			t.Fatal(err)
		}
	}
	return output
}

// TestMeasurementProfiles is opt-in because it performs repeated durable I/O.
// Its JSONL output and mutex profile are development evidence, not qualification.
func TestMeasurementProfiles(t *testing.T) {
	path := os.Getenv("TARP_MEASURE_OUTPUT")
	if path == "" {
		t.Skip("set TARP_MEASURE_OUTPUT to a new JSONL output path")
	}
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := errors.Join(fd.Sync(), fd.Close()); err != nil {
			t.Error(err)
		}
	}()
	encoder := json.NewEncoder(fd)
	write := func(kind string, size, repetition int, data any) {
		t.Helper()
		if err := encoder.Encode(map[string]any{"kind": kind, "value_bytes": size, "repetition": repetition, "data": data}); err != nil {
			t.Fatal(err)
		}
	}
	write("environment", 0, 0, map[string]any{
		"go": runtime.Version(), "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"gomaxprocs": runtime.GOMAXPROCS(0), "logical_cpus": runtime.NumCPU(),
		"notes": os.Getenv("TARP_MEASURE_ENVIRONMENT"), "repetitions": 5, "keys": 128, "key_bytes": 16,
		"read_samples": 2048, "durable_write_samples": 32, "seed": 42,
		"sampling":    "time.Now per operation; bounded Algorithm R reservoirs for overlapping workers; 5 ms sampled memory/disk peaks",
		"sync_policy": "data Sync + temporary CURRENT Sync + directory Sync; rotation adds catalog and segment barriers",
	})
	previous := runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(previous)
	for _, size := range []int{64, 1024, 4096} {
		for repetition := 1; repetition <= 5; repetition++ {
			func() {
				keys, value, opts := measurementInput(size, 128)
				hooks := &measurementHooks{}
				dir := filepath.Join(t.TempDir(), "db")
				io := &storage.IO{Hook: hooks.hook}
				db, err := openWithIO(dir, opts, io)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := db.Close(); err != nil {
						t.Error(err)
					}
				}()
				for _, key := range keys {
					if err := db.Put(key, value); err != nil {
						t.Fatal(err)
					}
				}
				write("options", size, repetition, db.opts)
				get := func(i int) error {
					got, err := db.Get(keys[i%len(keys)])
					runtime.KeepAlive(got)
					return err
				}
				before := hooks.reads.Load()
				write("cache_hit_1_worker", size, repetition, measureCalls(t, 2048, 1, get))
				write("cache_hit_4_workers", size, repetition, measureCalls(t, 2048, 4, get))
				if hooks.reads.Load() != before {
					t.Fatal("declared cache-hit profile performed file reads")
				}
				allocations := testing.AllocsPerRun(100, func() {
					if err := get(0); err != nil {
						t.Fatal(err)
					}
				})
				var memBefore, memAfter runtime.MemStats
				runtime.ReadMemStats(&memBefore)
				for i := 0; i < 100; i++ {
					if err := get(i); err != nil {
						t.Fatal(err)
					}
				}
				runtime.ReadMemStats(&memAfter)
				write("cache_hit_allocation", size, repetition, map[string]any{
					"allocations_per_op": allocations, "allocated_bytes_per_op": float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 100,
				})
				holds := make([]int64, 2048)
				for i := range holds {
					db.state.Lock()
					now := time.Now()
					_, ok := db.cache.Get(keys[i%len(keys)])
					holds[i] = time.Since(now).Nanoseconds()
					db.state.Unlock()
					if !ok {
						t.Fatal("critical-section hit probe missed")
					}
				}
				write("cache_state_hold_probe_not_end_to_end", size, repetition, latency(holds, int64(len(holds)), 0))
				write("resources_hot", size, repetition, resourceSample(t, db))
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				missOpts := opts
				missOpts.CacheBytes /= 16
				db, err = openWithIO(dir, missOpts, io)
				if err != nil {
					t.Fatal(err)
				}
				before = hooks.reads.Load()
				write("application_miss_os_warm", size, repetition, measureCalls(t, 2048, 1, get))
				if hooks.reads.Load()-before != 2048 {
					t.Fatal("declared miss profile admitted cache hits")
				}
				for round := 0; round < 2; round++ {
					for _, key := range keys {
						if err := db.Put(key, value); err != nil {
							t.Fatal(err)
						}
					}
				}
				var live [][]byte
				for i, key := range keys {
					if i%4 == 0 {
						if err := db.Delete(key); err != nil {
							t.Fatal(err)
						}
					} else {
						live = append(live, key)
					}
				}
				for _, compact := range []bool{false, true} {
					if compact {
						if _, err := db.Compact(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					start := time.Now()
					db, err = openWithIO(dir, opts, io)
					if err != nil {
						t.Fatal(err)
					}
					write(fmt.Sprintf("startup_after_compact_%t", compact), size, repetition, map[string]any{
						"elapsed_ns": time.Since(start).Nanoseconds(), "resources": resourceSample(t, db),
					})
				}
				for _, key := range live {
					if _, err := db.Get(key); err != nil {
						t.Fatal(err)
					}
				}
				for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "slow-backup-compact"} {
					if err := db.Put(live[0], value); err != nil {
						t.Fatal(err)
					}
					write("interference_"+mode, size, repetition, measureInterference(t, db, hooks, live, value, mode))
				}
				for _, rotate := range []bool{false, true} {
					_, _, writeOpts := measurementInput(size, 2)
					writeOpts.MaxDataFileBytes = 1 << 20
					if rotate {
						writeOpts.MaxRecordBytes = int64(48 + 16 + size)
						writeOpts.MaxDataFileBytes = writeOpts.MaxRecordBytes + 64
					}
					writer := newTestDB(t, writeOpts, nil)
					write(fmt.Sprintf("durable_put_rotate_%t", rotate), size, repetition,
						measureCalls(t, 32, 1, func(int) error { return writer.Put(keys[0], value) }))
					if !rotate {
						for _, key := range keys[:32] {
							if err := writer.Put(key, value); err != nil {
								t.Fatal(err)
							}
						}
						write("durable_delete", size, repetition, measureCalls(t, 32, 1, func(i int) error { return writer.Delete(keys[i]) }))
					}
					if err := writer.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}()
			t.Logf("completed %d-byte profile repetition %d/5", size, repetition)
		}
	}
	profile, err := os.OpenFile(path+".mutex.pprof", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	err = errors.Join(pprof.Lookup("mutex").WriteTo(profile, 0), profile.Close())
	if err != nil {
		t.Fatal(err)
	}
}
