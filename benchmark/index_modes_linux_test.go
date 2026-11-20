package benchmark

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

const indexTrialBudget = 1 << 20
const indexTrialRequests = 256

func indexTrialKey(n int) []byte {
	var key [16]byte
	copy(key[:], "account/")
	binary.BigEndian.PutUint64(key[8:], uint64(n))
	return key[:]
}

// TestIndexModeCampaign runs one explicitly selected arm in a fresh process.
// Ordinary CI does not run timing campaigns. See docs/measurements.md.
func TestIndexModeCampaign(t *testing.T) {
	modeName := os.Getenv("TARP_INDEX_TRIAL_MODE")
	if modeName == "" {
		t.Skip("set TARP_INDEX_TRIAL_MODE to run the index comparison")
	}
	opts := tarp.Options{CacheBytes: 64 << 10, TargetSegmentBytes: 8 << 20}
	switch modeName {
	case "memory":
		opts.IndexMode = tarp.IndexMemory
	case "btree":
		opts.IndexMode = tarp.IndexBTree
		opts.IndexMemoryBytes = indexTrialBudget
	default:
		t.Fatalf("unsupported index mode %q", modeName)
	}
	n, err := strconv.Atoi(os.Getenv("TARP_INDEX_TRIAL_KEYS"))
	if err != nil || n != 2048 && n != 20480 {
		t.Fatal("TARP_INDEX_TRIAL_KEYS must be 2048 or 20480")
	}
	ctx := t.Context()
	root := t.TempDir()
	store := filepath.Join(root, "db")
	value := bytes.Repeat([]byte{0x67}, 64)
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	// Seed using the same backend, write API, DATA layout, key order and
	// slice lifetime in both arms. Each child process gets an independent DB.
	seedOpts := opts
	seedOpts.IndexMode = tarp.IndexMemory
	seedOpts.IndexMemoryBytes = 0
	seed, err := tarp.Open(store, seedOpts)
	check(err)
	for i := 0; i < n; i += 128 {
		chunk := make([]tarp.Entry, 128)
		for j := range chunk {
			chunk[j] = tarp.Entry{Key: indexTrialKey(2 * (i + j)), Value: value}
		}
		check(seed.BatchPut(chunk))
	}
	check(seed.Close())
	seed = nil
	var db *tarp.DB
	defer func() {
		if db != nil {
			check(db.Close())
		}
	}()

	snapshot := func() index.Metrics {
		if db == nil {
			return index.Metrics{}
		}
		return engine.Observe((*engine.DB)(db)).IndexMetrics()
	}
	clearValues := func() { engine.Observe((*engine.DB)(db)).ClearCache() }
	access := func(key []byte) {
		t.Helper()
		v, e := db.Get(key)
		if e != nil || !bytes.Equal(v, value) {
			t.Fatalf("Get %x: %x %v", key, v, e)
		}
	}
	run := func(name string, reps int, long bool, call func(int)) {
		t.Helper()
		runtime.GC()
		pre := snapshot()
		beforeMemory := vmProcessMemory(t)
		storageIO := &containerBatchIO{}
		beforeIO, e := storageIO.ReadBytes()
		check(e)
		durations := make([]int64, reps)
		start := time.Now()
		// Short lookup timings use no asynchronous sampler. Slow maintenance
		// samples live heap estimates; Linux peak RSS covers the entire process.
		stop := make(chan struct{})
		peak := make(chan uint64, 1)
		samplerStopped := false
		defer func() {
			if long && !samplerStopped {
				close(stop)
				<-peak
			}
		}()
		if long {
			go func() {
				s := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
				tick := time.NewTicker(5 * time.Millisecond)
				defer tick.Stop()
				var high uint64
				for {
					metrics.Read(s)
					high = max(high, s[0].Value.Uint64())
					select {
					case <-stop:
						peak <- high
						return
					case <-tick.C:
					}
				}
			}()
		}
		for i := range reps {
			begin := time.Now()
			call(i)
			durations[i] = time.Since(begin).Nanoseconds()
		}
		elapsed := time.Since(start)
		var high uint64
		if long {
			close(stop)
			high = <-peak
			samplerStopped = true
		}
		post := snapshot()
		afterIO, e := storageIO.ReadBytes()
		check(e)
		afterMemory := vmProcessMemory(t)
		if post.CacheBytes > indexTrialBudget {
			t.Fatalf("charged page cache %d exceeds budget", post.CacheBytes)
		}
		if name == "hot_get" && pre.PageReads != post.PageReads {
			t.Fatal("Value hits loaded index pages")
		}
		row := map[string]any{"mode": modeName, "keys": n, "go": runtime.Version(), "gomaxprocs": runtime.GOMAXPROCS(0),
			"workload": name, "latency": latency(durations, int64(reps), elapsed), "process_read_bytes": afterIO - beforeIO,
			"index_before": pre, "index_after": post, "memory_before": beforeMemory, "memory_after": afterMemory,
			"heap_objects_sampled_peak_bytes": high}
		encoded, e := json.Marshal(row)
		check(e)
		fmt.Println("TARP_INDEX_RESULT", string(encoded))
	}
	run("open", 1, true, func(int) { db, err = tarp.OpenExisting(ctx, store, opts); check(err) })
	if db.Stats().LiveKeys != uint64(n) {
		t.Fatal("incorrect seed count")
	}

	// This stride is coprime to both fixture sizes. Present and missing
	// keys probe the same ranges and all cold-Get requests are distinct.
	trace := make([][]byte, indexTrialRequests)
	missing := make([][]byte, indexTrialRequests)
	var grouped, scattered [indexTrialRequests][32][]byte
	for i := range trace {
		id := (i*6211 + 505) % n
		trace[i], missing[i] = indexTrialKey(2*id), indexTrialKey(2*id+1)
		for j := range grouped[i] {
			grouped[i][j] = indexTrialKey(2 * ((id + j) % n))
			scattered[i][j] = indexTrialKey(2 * ((id + 3851*j) % n))
		}
	}
	access(trace[0])
	run("hot_get", len(trace), false, func(int) { access(trace[0]) })
	clearValues()
	run("value_cold_get", len(trace), false, func(i int) { access(trace[i]) })
	run("missing_get", len(trace), false, func(i int) {
		_, e := db.Get(missing[i])
		if !errors.Is(e, tarp.ErrNotFound) {
			t.Fatalf("Get missing: %v", e)
		}
	})
	batchRead := func(keys [][]byte) {
		results, e := db.BatchGet(keys)
		check(e)
		if len(results) != len(keys) {
			t.Fatal("incorrect batch length")
		}
		for _, x := range results {
			if !x.Found || !bytes.Equal(x.Value, value) {
				t.Fatal("incorrect batch result")
			}
		}
	}
	clearValues()
	run("grouped_batch_get_32", len(trace), false, func(i int) { batchRead(grouped[i][:]) })
	clearValues()
	run("scattered_batch_get_32", len(trace), false, func(i int) { batchRead(scattered[i][:]) })
	run("overwrite_put", 96, false, func(i int) { check(db.Put(trace[i], value)) })
	run("overwrite_batch_put_32", 32, false, func(i int) {
		var entries [32]tarp.Entry
		for j, key := range scattered[i] {
			entries[j] = tarp.Entry{Key: key, Value: value}
		}
		check(db.BatchPut(entries[:]))
	})

	run("keys_first", 64, false, func(int) {
		for _, e := range db.Keys(ctx) {
			check(e)
			return
		}
		t.Fatal("empty Keys")
	})
	run("scan_first", 64, false, func(int) {
		for _, e := range db.Scan(ctx) {
			check(e)
			return
		}
		t.Fatal("empty Scan")
	})
	run("keys_full", 1, true, func(int) {
		count := 0
		for _, e := range db.Keys(ctx) {
			check(e)
			count++
		}
		if count != n {
			t.Fatal(count)
		}
	})
	run("scan_full", 1, true, func(int) {
		count := 0
		for entry, e := range db.Scan(ctx) {
			check(e)
			if !bytes.Equal(entry.Value, value) {
				t.Fatal("scan value")
			}
			count++
		}
		if count != n {
			t.Fatal(count)
		}
	})
	next, stopReader := iter.Pull2(db.Scan(ctx))
	defer stopReader()
	if _, e, ok := next(); e != nil || !ok {
		t.Fatalf("open view %v", e)
	}
	run("overwrite_with_paused_scan", 96, false, func(i int) { check(db.Put(trace[i], value)) })
	stopReader()

	backup := filepath.Join(root, "backup")
	restored := filepath.Join(root, "restored")
	run("backup", 1, true, func(int) { check(db.Backup(ctx, backup)) })
	verify := func(report tarp.BackupInfo, e error) {
		check(e)
		if report.LiveKeys != uint64(n) {
			t.Fatalf("verified keys %d", report.LiveKeys)
		}
	}
	run("verify_read_only", 1, true, func(int) { verify(tarp.VerifyBackup(ctx, backup)) })
	run("verify_scratch", 1, true, func(int) { verify(tarp.VerifyBackupWithScratch(ctx, backup, root)) })
	run("restore", 1, true, func(int) { check(tarp.Restore(ctx, tarp.RestoreRequest{Source: backup, Destination: restored})) })
	run("compact", 1, true, func(int) { _, e := db.Compact(ctx); check(e) })
	access(trace[0])

	clearValues()
	run("mixed_80_get_20_overwrite", len(trace), false, func(i int) {
		if i%5 == 0 {
			check(db.Put(trace[i], value))
			return
		}
		access(trace[i])
	})

	// Force exact comparisons beyond one 8 KiB page. Keys differ in a
	// binary suffix after a common prefix that contains NUL and 0xff.
	const overflowCount = 128
	prefix := bytes.Repeat([]byte{0, 0xff, 0x7f, 0x61}, (9<<10)/4)
	var longKeys [overflowCount][]byte
	entries := make([]tarp.Entry, overflowCount)
	for i := range longKeys {
		longKeys[i] = binary.BigEndian.AppendUint64(bytes.Clone(prefix), uint64(i))
		entries[i] = tarp.Entry{Key: longKeys[i], Value: value}
	}
	check(db.BatchPut(entries))
	clearValues()
	run("shared_prefix_overflow_get", len(trace), false, func(i int) { access(longKeys[(i*53)%overflowCount]) })

	// Start the foreground samples only after the online backup has pinned
	// its layout. Let the real worker copy without further coordination and
	// repeat backups until the last foreground write finishes.
	check(db.Close())
	db = nil
	ready, release := make(chan struct{}), make(chan struct{})
	var first sync.Once
	bgCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	core, err := engine.OpenWithIO(store, engine.Options(opts), &storage.IO{Hook: func(point string) error {
		if point == "backup.copy:before" {
			first.Do(func() {
				close(ready)
				select {
				case <-release:
				case <-bgCtx.Done():
				}
			})
		}
		return nil
	}})
	check(err)
	db = (*tarp.DB)(core)
	done := make(chan error, 1)
	go func() {
		dest := filepath.Join(root, "concurrent-backup")
		for bgCtx.Err() == nil {
			if err := db.Backup(bgCtx, dest); err != nil {
				if bgCtx.Err() != nil && errors.Is(err, bgCtx.Err()) {
					break
				}
				done <- err
				return
			}
			if err := os.RemoveAll(dest); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		joined = true
		t.Fatalf("backup ended before copying: %v", err)
	}
	run("overwrite_during_online_backups", 96, false, func(i int) {
		if i == 0 {
			close(release)
		}
		check(db.Put(trace[i], value))
	})
	cancel()
	err = <-done
	joined = true
	check(err)

}
