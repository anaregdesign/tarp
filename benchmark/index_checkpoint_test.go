//go:build linux

package benchmark

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

const checkpointMeasureKeys = 6144

var checkpointMeasureOptions = engine.Options{
	IndexMode: engine.IndexBTree, BTreeMemoryBudgetBytes: 4 << 20,
	MaxRecordBytes: 4096, ValueCacheBudgetBytes: 1 << 20, TargetSegmentBytes: 1 << 20,
}

func checkpointRow(t *testing.T, kind string, row map[string]any) {
	t.Helper()
	row["kind"] = kind
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("INDEX_CHECKPOINT %s\n", b)
}

func checkpointOpen(dir string, opts engine.Options, fs *storage.IO) (*engine.DB, error) {
	return engine.OpenWithHooks(dir, opts, fs, &engine.Hooks{CleanerStart: make(chan struct{})})
}

func checkpointFill(t *testing.T, db *engine.DB, n, generation int) {
	t.Helper()
	value := bytes.Repeat([]byte{byte(generation)}, 256)
	for start := 0; start < n; start += 128 {
		entries := make([]engine.Entry, min(128, n-start))
		for i := range entries {
			entries[i] = engine.Entry{Key: []byte(fmt.Sprintf("%016x", start+i)), Value: value}
		}
		if err := db.BatchPut(entries); err != nil {
			t.Fatal(err)
		}
	}
}

type indexFixture struct {
	dir, digest string
	buildNS     int64
	indexBytes  int64
}

func prepareIndexFixture(t *testing.T, name string) indexFixture {
	t.Helper()
	f := indexFixture{dir: filepath.Join(t.TempDir(), "fixture")}
	opts := checkpointMeasureOptions
	db, err := checkpointOpen(f.dir, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	rounds := 1
	if name != "mostly_live" {
		rounds = 8
	}
	for r := range rounds {
		checkpointFill(t, db, checkpointMeasureKeys, r)
	}
	runtime.GC()
	res, err := sampleVerifyResources(filepath.Join(f.dir, ".tarp-work"), func() error {
		start := time.Now()
		err := db.CheckpointIndex(t.Context())
		f.buildNS = time.Since(start).Nanoseconds()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closeExact := time.Since(start)
	indexFiles, err := filepath.Glob(filepath.Join(f.dir, "INDEX-*"))
	if err != nil || len(indexFiles) != 1 {
		t.Fatalf("after creation, INDEX files %v: %v", indexFiles, err)
	}
	st, err := os.Stat(indexFiles[0])
	if err != nil {
		t.Fatal(err)
	}
	f.indexBytes = st.Size()
	checkpointRow(t, "creation", map[string]any{
		"fixture": name, "seed_rounds": rounds, "live_keys": checkpointMeasureKeys,
		"covered_records": rounds * checkpointMeasureKeys, "checkpoint_ns": f.buildNS, "close_exact_ns": closeExact.Nanoseconds(),
		"index_bytes": f.indexBytes, "heap_baseline_bytes": res.baselineHeap,
		"peak_heap_bytes": res.peakHeap, "peak_scratch_bytes": res.peakScratch, "resource_samples": res.samples,
	})
	// Memory Close leaves the tail uncheckpointed so every clone starts with
	// this exact sealed prefix plus a later committed batch.
	opts.IndexMode, opts.BTreeMemoryBudgetBytes = engine.IndexMemory, 0
	db, err = checkpointOpen(f.dir, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	checkpointFill(t, db, 128, rounds)
	if name == "post_compaction_miss" {
		if _, err := db.Compact(t.Context()); err != nil {
			t.Fatal(err)
		}
		f.indexBytes = 0
	}
	var count int
	f.digest, count = measurementState(t, db)
	if count != checkpointMeasureKeys {
		t.Fatalf("fixture has %d keys", count)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return f
}

func conditionIndexFiles(t *testing.T, dir, cache string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			t.Fatalf("unexpected fixture object %s", e.Name())
		}
		fd, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if cache == "warm" {
			_, err = io.Copy(io.Discard, fd)
		} else if err = fd.Sync(); err == nil {
			err = adviseContainerBatchDontNeed(fd.Fd())
		}
		if err := errors.Join(err, fd.Close()); err != nil {
			t.Fatal(err)
		}
	}
}

func measureIndexOpen(t *testing.T, f indexFixture, name, cache, arm string, round int) int64 {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	measurementCloneClosed(t, f.dir, dir)
	opts := checkpointMeasureOptions
	if arm == "replay" {
		paths, err := filepath.Glob(filepath.Join(dir, "INDEX-*"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
		}
	}
	if arm == "indexed" {
		opts.OpenValidation = engine.Indexed
	}
	reads := map[string]int64{"covered-header": 0, "covered-payload": 0, "tail-header": 0, "tail-payload": 0, "replay-header": 0, "replay-payload": 0, "index": 0}
	probe := &storage.IO{OpenRead: func(k string, n int) { reads[k] += int64(n) }}
	conditionIndexFiles(t, dir, cache)
	runtime.GC()
	var db *engine.DB
	var openNS int64
	res, err := sampleVerifyResources(filepath.Join(dir, ".tarp-work"), func() error {
		var err error
		start := time.Now()
		db, err = checkpointOpen(dir, opts, probe)
		openNS = time.Since(start).Nanoseconds()
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if count := db.Stats().LiveKeys; count != checkpointMeasureKeys {
		t.Fatalf("Open returned %d live keys", count)
	}
	hit := arm != "replay" && name != "post_compaction_miss"
	if hit {
		if reads["index"] != f.indexBytes || reads["tail-payload"] == 0 || reads["replay-payload"] != 0 ||
			(reads["covered-payload"] == 0) != (arm == "indexed") {
			t.Fatalf("%s expected a hit with a committed suffix: %v", arm, reads)
		}
	} else if reads["replay-payload"] == 0 || reads["index"] != 0 {
		t.Fatalf("%s expected full authoritative replay: %v", arm, reads)
	}
	metrics := engine.Observe(db).IndexMetrics()
	digest, count := measurementState(t, db)
	if digest != f.digest || count != checkpointMeasureKeys {
		t.Fatal("reopened contents differ from source")
	}
	start := time.Now()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	closeNS := time.Since(start).Nanoseconds()
	var dataBytes int64
	for k, n := range reads {
		if k != "index" {
			dataBytes += n
		}
	}
	checkpointRow(t, "open", map[string]any{
		"fixture": name, "arm": arm, "cache": cache, "round": round, "hit": hit, "ns": openNS,
		"close_ns": closeNS, "reads": reads, "data_read_bytes": dataBytes, "index_read_bytes": reads["index"],
		"peak_heap_bytes": res.peakHeap, "heap_baseline_bytes": res.baselineHeap, "peak_scratch_bytes": res.peakScratch,
		"resource_samples": res.samples, "foreground_pager_peak_scratch_bytes": metrics.ScratchPeakBytes,
		"state_sha256": digest,
	})
	return openNS
}

func measureCheckpointWrites(t *testing.T, f indexFixture, name string, overlap bool) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "db")
	measurementCloneClosed(t, f.dir, dir)
	reached, start := make(chan struct{}), make(chan struct{})
	var once sync.Once
	fs := &storage.IO{Hook: func(point string) error {
		if overlap && point == "index.scan:before" {
			once.Do(func() { close(reached); <-start })
		}
		return nil
	}}
	db, err := checkpointOpen(dir, checkpointMeasureOptions, fs)
	if err != nil {
		t.Fatal(err)
	}
	type completion struct {
		end time.Time
		err error
	}
	done := make(chan completion, 1)
	if overlap {
		go func() {
			err := db.CheckpointIndex(t.Context())
			done <- completion{time.Now(), err}
		}()
		select {
		case <-reached:
		case result := <-done:
			t.Fatalf("checkpoint returned before capture: %v", result.err)
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	const writes = 32
	var samples, during []int64
	var started [writes]time.Time
	value := bytes.Repeat([]byte{42}, 256)
	begin := time.Now()
	close(start)
	for i := range writes {
		key := []byte(fmt.Sprintf("%016x", i))
		started[i] = time.Now()
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(started[i]).Nanoseconds())
	}
	duration := time.Since(begin)
	if overlap {
		result := <-done
		if result.err != nil {
			t.Fatal(result.err)
		}
		for i, at := range started {
			if at.Before(result.end) {
				during = append(during, samples[i])
			}
		}
		if len(during) == 0 {
			t.Fatal("no foreground Put began during checkpoint")
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	checkpointRow(t, "overlap", map[string]any{
		"fixture": name, "concurrent_checkpoint": overlap, "all_puts": latency(samples, writes, duration),
		"started_during_checkpoint": len(during), "overlapping_put_ns": during,
	})
}

// TestIndexCheckpointMeasurement runs only in the explicitly enabled Linux
// campaign. All API latency excludes snapshot copying, validation of the result
// and file-cache conditioning. Scratch and heap samples are process-level.
func TestIndexCheckpointMeasurement(t *testing.T) {
	if os.Getenv("TARP_INDEX_CHECKPOINT_MEASURE") != "1" {
		t.Skip("set TARP_INDEX_CHECKPOINT_MEASURE=1 in the measurement campaign")
	}
	checkpointRow(t, "methodology", map[string]any{
		"go": runtime.Version(), "goarch": runtime.GOARCH, "gomaxprocs": runtime.GOMAXPROCS(0), "rounds": 3,
		"fixture":    "6144 distinct 16-byte keys, 256-byte values, batches of 128. Long history applies eight full passes, mostly live one pass. Park automatic cleaning. Each fixture has a 128-record tail. Compact invalidates the third fixture's checkpoint.",
		"cache":      "Private regular files read sequentially for warm, synced with POSIX_FADV_DONTNEED for cold. Advice does not guarantee device or host cache eviction.",
		"timing":     "Independent cloned input per call; rotate arm order. Timed Open includes byte instrumentation and concurrent resource sampling. Verify SHA-256 of the full logical state outside the timer. Each measured BTree Close includes a refresh for the committed tail or miss.",
		"resources":  "Logical bytes returned by DATA and INDEX file reads, including headers read again during hashing. Heap objects and apparent sizes of open index/replay files in .tarp-work sampled every 2ms; samples can miss short peaks. Foreground pager reports a separate internal high-water mark.",
		"overlap":    "32 durable overwrites on independent clones with and without a concurrent checkpoint. Release the scan hook immediately before writes; record which calls began before checkpoint completion.",
		"break_even": "Ceiling of the explicit checkpoint creation cost divided by the positive difference between median replay and median hit Open latency. A null count indicates no saving.",
	})
	for _, name := range []string{"long_overwrite", "mostly_live", "post_compaction_miss"} {
		t.Run(name, func(t *testing.T) {
			f := prepareIndexFixture(t, name)
			if name != "post_compaction_miss" {
				for _, overlap := range []bool{false, true} {
					t.Run(fmt.Sprintf("overlap_%t", overlap), func(t *testing.T) { measureCheckpointWrites(t, f, name, overlap) })
				}
			}
			for _, cache := range []string{"warm", "cold"} {
				arms := []string{"replay", "full", "indexed"}
				times := make(map[string][]int64)
				for round := range 3 {
					for i := range arms {
						arm := arms[(round+i)%len(arms)]
						t.Run(strings.Join([]string{cache, arm, fmt.Sprint(round + 1)}, "_"), func(t *testing.T) {
							times[arm] = append(times[arm], measureIndexOpen(t, f, name, cache, arm, round+1))
						})
					}
				}
				if name == "post_compaction_miss" || t.Failed() {
					continue
				}
				for _, series := range times {
					slices.Sort(series)
				}
				for _, arm := range arms[1:] {
					saved := times["replay"][1] - times[arm][1]
					var payback any
					if saved > 0 {
						payback = (f.buildNS + saved - 1) / saved
					}
					checkpointRow(t, "break_even", map[string]any{
						"fixture": name, "arm": arm, "cache": cache, "saved_ns_per_reopen": saved,
						"checkpoint_ns": f.buildNS, "reopens": payback,
					})
				}
			}
		})
	}
}
