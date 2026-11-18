package benchmark

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

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func measurementInput(size, count int) ([][]byte, []byte, engine.Options) {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%016x", i))
	}
	value := make([]byte, size)
	for i := range value {
		value[i] = byte(i*31 + size)
	}
	return keys, value, engine.Options{
		CacheBytes: int64((cache.MetadataBytes + 16 + size) * count), MaxRecordBytes: 8 << 10,
		TargetSegmentBytes: 64 << 10, ReadFileHandles: 8,
	}
}

func benchmarkDB(b *testing.B, size int, miss bool) (*tarp.DB, [][]byte, []byte) {
	b.Helper()
	keys, value, opts := measurementInput(size, 128)
	if miss {
		opts.CacheBytes /= 16
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

// Both rotation policies admit the same record. A prefill makes even the first
// timed Put rotate in the one-record case.
func measurementWriteOptions(opts engine.Options, keyBytes, valueBytes int, eachRecord bool) engine.Options {
	opts.MaxRecordBytes = int64(format.RecordHeaderBytes + keyBytes + valueBytes)
	opts.TargetSegmentBytes = 0
	if eachRecord {
		opts.TargetSegmentBytes = format.SegmentHeaderBytes + opts.MaxRecordBytes
	}
	return opts
}

func BenchmarkDurablePut(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		for _, rotate := range []bool{false, true} {
			policy := "default-target"
			if rotate {
				policy = "one-record-target"
			}
			b.Run(fmt.Sprintf("%d/%s", size, policy), func(b *testing.B) {
				keys, value, base := measurementInput(size, 2)
				opts := measurementWriteOptions(base, len(keys[0]), size, rotate)
				var rotations atomic.Uint64
				hooks := &storage.IO{Hook: func(point string) error {
					if point == "segment.sync:after" {
						rotations.Add(1)
					}
					return nil
				}}
				db, err := engine.OpenWithIO(filepath.Join(b.TempDir(), "db"), opts, hooks)
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					if err := db.Close(); err != nil {
						b.Error(err)
					}
				})
				if err := db.Put(keys[0], value); err != nil {
					b.Fatal(err)
				}
				rotations.Store(0)
				b.ReportAllocs()
				b.SetBytes(int64(size))
				for b.Loop() {
					if err := db.Put(keys[0], value); err != nil {
						b.Fatal(err)
					}
				}
				count := rotations.Load()
				if rotate && count != uint64(b.N) {
					b.Fatalf("%d rotations for %d timed writes", count, b.N)
				}
				b.ReportMetric(float64(count)/float64(b.N), "rotations/op")
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

func resourceSample(t *testing.T, db *engine.DB) map[string]any {
	t.Helper()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	probe := engine.Observe(db)
	var copyIndex map[string]storage.Location
	var logicalIndex, cacheBytes int64
	probe.State(func(index map[string]storage.Location, c *cache.Cache) {
		copyIndex = make(map[string]storage.Location, len(index))
		for key, loc := range index {
			copyIndex[strings.Clone(key)] = loc
			logicalIndex += int64(len(key)) + int64(unsafe.Sizeof(key)) + int64(unsafe.Sizeof(loc))
		}
		cacheBytes = c.Bytes()
	})
	liveKeys := len(copyIndex)
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
	logical, allocated, err := measurementDiskUsage(probe.Dir())
	if err != nil {
		t.Fatal(err)
	}
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(probe.Dir(), &filesystem); err != nil {
		t.Fatal(err)
	}
	count := max(1, liveKeys)
	return map[string]any{
		"heap_alloc_bytes": before.HeapAlloc, "runtime_sys_bytes": before.Sys, "process_peak_rss_bytes": peakRSS,
		"cache_logical_bytes": cacheBytes, "cache_limit_bytes": probe.Options().CacheBytes,
		"live_keys": liveKeys, "index_logical_bytes_per_key": float64(logicalIndex) / float64(count),
		"index_clone_retained_heap_delta_per_key": float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(count),
		"open_process_fds_including_sampler":      len(fds), "read_handle_limit": probe.Options().ReadFileHandles,
		"directory_logical_file_bytes": logical, "directory_allocated_bytes": allocated, "layout": observedLayout(db),
		"filesystem_type_number": fmt.Sprintf("0x%x", filesystem.Type), "filesystem_block_bytes": filesystem.Bsize,
		"filesystem_flags": fmt.Sprintf("0x%x", filesystem.Flags),
		"pending_calls":    0, "index_clone_note": "isolated map/key clone; shared file metadata excluded; GC delta is noisy, not a process limit",
	}
}

type measurementHooks struct {
	backupBytes       atomic.Int64
	manualBytes       atomic.Int64
	manualSourceOpens atomic.Int64
	manualRecords     atomic.Int64
	windowStart       func()
	windowEnd         func()
	reads             atomic.Int64
	slow              atomic.Bool
	started           atomic.Pointer[chan struct{}]
	overlap           atomic.Pointer[chan struct{}]
	capture           atomic.Pointer[measurementCapture]
}

// The worker is held from Open through Close for the controlled interference
// comparisons. All foreground calls and the manual checkpoint use the normal engine.
func (h *measurementHooks) interferenceIO() *storage.IO {
	return &storage.IO{CleanerStart: make(chan struct{}), Hook: h.hook, Wrote: h.wrote}
}

func (h *measurementHooks) wrote(point string, n int) {
	switch point {
	case "backup.copy":
		h.backupBytes.Add(int64(n))
	case "compact.segment.write", "compact.write":
		h.manualBytes.Add(int64(n))
	}
}

func (h *measurementHooks) releaseOverlap() {
	if signal := h.overlap.Swap(nil); signal != nil {
		close(*signal)
	}
}

func (h *measurementHooks) hook(point string) error {
	switch point {
	case "read:before":
		h.reads.Add(1)
	case "compact.write:before":
		h.releaseOverlap()
	case "compact.source.open:after":
		h.manualSourceOpens.Add(1)
	case "compact.record:after":
		h.manualRecords.Add(1)
	}
	if point == "backup.copy:before" {
		gate := h.overlap.Load()
		if capture := h.capture.Swap(nil); capture != nil {
			if err := capture.observe(); err != nil {
				return err
			}
		}
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

func measureInterference(t *testing.T, db *engine.DB, hooks *measurementHooks, keys [][]byte, value []byte, mode string) map[string]any {
	return measureInterferenceFor(t, db, hooks, keys, value, mode, 30*time.Millisecond)
}

func measureInterferenceFor(t *testing.T, db *engine.DB, hooks *measurementHooks, keys [][]byte, value []byte, mode string, minimum time.Duration) map[string]any {
	t.Helper()
	probe := engine.Observe(db)
	probe.Store(func(s *storage.Store) {
		if s.IO == nil || s.IO.CleanerStart == nil || s.IO.Wrote == nil {
			t.Fatal("interference control requires its gated worker and per-actor byte observer")
		}
	})
	root := t.TempDir()
	target := filepath.Join(root, "backup")
	if err := db.Put([]byte("writer-key-00000"), value); err != nil {
		t.Fatal(err)
	}
	expectedDigest, expectedKeys := measurementState(t, db)
	for _, key := range keys {
		if _, err := db.Get(key); err != nil {
			t.Fatal(err)
		}
	}
	frontier := engine.Observe(db).Frontier()
	captured := &measurementCapture{parent: root, uuid: frontier.UUID}
	fixtureSequence := frontier.Sequence
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
	initialDisk, initialAllocated, err := measurementDiskUsage(probe.Dir(), root)
	if err != nil {
		t.Fatal(err)
	}
	peakDisk := initialDisk
	peakAllocated := initialAllocated
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(probe.Dir(), &filesystem); err != nil {
		t.Fatal(err)
	}
	minimumAvailable := filesystem.Bavail * uint64(filesystem.Bsize)
	var peakPinned int64
	var begin time.Time
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
			current, allocated, err := measurementDiskUsage(probe.Dir(), root)
			if err != nil {
				monitorDone <- err
				return
			}
			peakDisk = max(peakDisk, current)
			peakAllocated = max(peakAllocated, allocated)
			if err := syscall.Statfs(probe.Dir(), &filesystem); err != nil {
				monitorDone <- err
				return
			}
			minimumAvailable = min(minimumAvailable, filesystem.Bavail*uint64(filesystem.Bsize))
			probe.Store(func(s *storage.Store) {
				selected := map[uint64]bool{s.Current.ActiveID: true}
				for _, file := range s.Catalog.Files {
					selected[file.ID] = true
				}
				var pinned int64
				for id, file := range s.Files {
					if !selected[id] && file.References() != 0 {
						pinned += file.Size.Load()
					}
				}
				peakPinned = max(peakPinned, pinned)
			})
			select {
			case <-stop:
				monitorDone <- nil
				return
			case <-ticker.C:
			}
		}
	}()
	readStart := hooks.reads.Load()
	backupBefore, manualBefore := hooks.backupBytes.Load(), hooks.manualBytes.Load()
	opensBefore, recordsBefore := hooks.manualSourceOpens.Load(), hooks.manualRecords.Load()
	if hooks.windowStart != nil {
		hooks.windowStart()
	}
	begin = time.Now()
	close(start)
	maintenanceStart := time.Now()
	var compact engine.CompactResult
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
			hooks.capture.Store(captured)
			if mode != "backup" {
				gate := make(chan struct{})
				hooks.overlap.Store(&gate)
			}
			copyStarted := make(chan struct{})
			hooks.started.Store(&copyStarted)
			done := make(chan error, 1)
			go func() {
				backupStart = time.Now()
				err := db.Backup(context.Background(), target)
				backupEnd = time.Now()
				done <- err
			}()
			select {
			case <-copyStarted:
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
	maintenanceNS := int64(0)
	if !backupStart.IsZero() || !compactStart.IsZero() {
		maintenanceNS = time.Since(maintenanceStart).Nanoseconds()
	}
	backupCopied := hooks.backupBytes.Load() - backupBefore
	compactCopied := hooks.manualBytes.Load() - manualBefore
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
	if (backupCopied > 0) != !backupStart.IsZero() ||
		(compactCopied > 0) != !compactStart.IsZero() ||
		(mode == "backup-compact" || mode == "slow-backup-compact") && overlapNS <= 0 {
		maintenanceErr = errors.Join(maintenanceErr, errors.New("maintenance copy or overlap does not match the requested operation"))
	}
	if remaining := minimum - time.Since(begin); remaining > 0 {
		time.Sleep(remaining)
	}
	close(stop)
	hooks.slow.Store(false)
	output := map[string]any{
		"maintenance_ns": maintenanceNS, "compact": compact,
		"fixture_sequence_before_workers": fixtureSequence, "fixture_live_keys": expectedKeys,
		"fixture_state_sha256": expectedDigest, "fixture_manifest_sha256": fmt.Sprintf("%x", frontier.ManifestHash),
		"automatic_cleaner": "held from Open through Close for controlled foreground and manual-checkpoint comparisons",
		"backup_copy_bytes": backupCopied, "backup_elapsed_ns": backupEnd.Sub(backupStart).Nanoseconds(),
		"compact_copy_bytes": compactCopied, "compact_elapsed_ns": compactEnd.Sub(compactStart).Nanoseconds(),
		"compact_copy_source_opens":           hooks.manualSourceOpens.Load() - opensBefore,
		"compact_records_copied":              hooks.manualRecords.Load() - recordsBefore,
		"observed_maintenance_api_overlap_ns": overlapNS,
		"copy_measurement_scope":              "bytes accepted by backup DATA writes and manual checkpoint DATA writes, including segment headers; metadata and foreground writes are excluded; elapsed and overlap cover complete API calls",
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
	if hooks.windowEnd != nil {
		hooks.windowEnd()
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
		restoredPath := filepath.Join(root, "restored")
		if err := engine.Restore(context.Background(), target, restoredPath, probe.Options()); err != nil {
			t.Fatal(err)
		}
		restored, err := engine.Open(restoredPath, probe.Options())
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := restored.Close(); err != nil {
				t.Error(err)
			}
		}()
		digest, count := measurementState(t, restored)
		restoredSequence := engine.Observe(restored).Frontier().Sequence
		if !captured.reached || restoredSequence != captured.sequence ||
			digest != expectedDigest || count != expectedKeys {
			t.Fatalf("produced backup did not reopen at its witnessed capture/state: capture=%d restored=%d keys=%d/%d digest=%s/%s",
				captured.sequence, restoredSequence, count, expectedKeys, digest, expectedDigest)
		}
		output["backup_capture_sequence"], output["restored_sequence"] = captured.sequence, restoredSequence
		output["restored_live_keys"], output["restored_state_sha256"], output["backup_reopen_verified"] = count, digest, true
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
		"storage_format": "unversioned", "notes": os.Getenv("TARP_MEASURE_ENVIRONMENT"), "repetitions": 5, "keys": 128, "key_bytes": 16,
		"read_samples": 2048, "durable_write_samples": 32, "seed": 42,
		"sampling":    "time.Now per operation; bounded Algorithm R reservoirs for overlapping workers; 5 ms sampled memory/disk peaks",
		"sync_policy": "data Sync + temporary CURRENT Sync + directory Sync; rotation adds catalog and segment barriers",
	})
	previous := runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(previous)
	for _, size := range []int{64, 1024, 4096} {
		compactionFixture := prepareCompactionFixture(t, size)
		startupFixtures := t.TempDir()
		for repetition := 1; repetition <= 5; repetition++ {
			func() {
				keys, value, opts := measurementInput(size, 128)
				hooks := &measurementHooks{}
				dir := filepath.Join(t.TempDir(), "db")
				io := &storage.IO{Hook: hooks.hook}
				db, err := engine.OpenWithIO(dir, opts, io)
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
				write("options", size, repetition, engine.Observe(db).Options())
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
					engine.Observe(db).State(func(_ map[string]storage.Location, c *cache.Cache) {
						now := time.Now()
						_, ok := c.Get(keys[i%len(keys)])
						holds[i] = time.Since(now).Nanoseconds()
						if !ok {
							t.Fatal("critical-section hit probe missed")
						}
					})
				}
				write("cache_state_hold_probe_not_end_to_end", size, repetition, latency(holds, int64(len(holds)), 0))
				write("resources_hot", size, repetition, resourceSample(t, db))
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				interferenceFixture := filepath.Join(t.TempDir(), "interference-fixture")
				measurementPrepareFixture(t, dir, interferenceFixture, opts, keys, value)
				missOpts := opts
				missOpts.CacheBytes /= 16
				db, err = engine.OpenWithIO(dir, missOpts, io)
				if err != nil {
					t.Fatal(err)
				}
				before = hooks.reads.Load()
				write("application_miss_os_warm", size, repetition, measureCalls(t, 2048, 1, get))
				if hooks.reads.Load()-before != 2048 {
					t.Fatal("declared miss profile admitted cache hits")
				}
				// Build both recovery layouts from one history per value size.
				if repetition == 1 {
					for round := 0; round < 2; round++ {
						for _, key := range keys {
							if err := db.Put(key, value); err != nil {
								t.Fatal(err)
							}
						}
					}
					for i := 0; i < len(keys); i += 4 {
						if err := db.Delete(keys[i]); err != nil {
							t.Fatal(err)
						}
					}
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				for _, state := range []string{"default_history", "manual_checkpoint"} {
					source := filepath.Join(startupFixtures, state)
					if repetition == 1 {
						if state == "manual_checkpoint" {
							measurementCheckpointFixture(t, dir, opts)
						}
						measurementCloneClosed(t, dir, source)
					}
					t.Run(fmt.Sprintf("startup-%s-%d-%d", state, size, repetition), func(t *testing.T) {
						scratch := filepath.Join(t.TempDir(), "db")
						measurementCloneClosed(t, source, scratch)
						current, elapsed := openStartupSample(t, scratch, opts)
						if got, want := engine.Observe(current).LiveKeys(), len(keys)-len(keys)/4; got != want {
							t.Fatalf("reopened history has %d live keys; want %d", got, want)
						}
						write("startup_"+state, size, repetition, startupRow(elapsed, resourceSample(t, current)))
					})
				}
				for _, mode := range compactionModes {
					write("compaction_"+mode, size, repetition, compactionFixture.measure(t, mode))
				}
				for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "slow-backup-compact"} {
					t.Run(fmt.Sprintf("interference-%d-%d-%s", size, repetition, mode), func(t *testing.T) {
						clone, observers := measurementOpenClone(t, interferenceFixture, opts)
						write("interference_"+mode, size, repetition, measureInterference(t, clone, observers, keys[:8], value, mode))
					})
				}
				for _, rotate := range []bool{false, true} {
					_, _, base := measurementInput(size, 2)
					writeOpts := measurementWriteOptions(base, len(keys[0]), size, rotate)
					writer := newTestDB(t, writeOpts, nil)
					if err := writer.Put(keys[0], value); err != nil {
						t.Fatal(err)
					}
					write(fmt.Sprintf("durable_put_options_rotate_%t", rotate), size, repetition, engine.Observe(writer).Options())
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
