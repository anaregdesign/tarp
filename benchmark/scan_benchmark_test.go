package benchmark

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"iter"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

type scanDB interface {
	Scan(context.Context) iter.Seq2[tarp.Entry, error]
}

type scanWorkload struct {
	index map[string]int
	value []byte
	seen  []bool
}

func newScanWorkload(keys [][]byte, value []byte) scanWorkload {
	index := make(map[string]int, len(keys))
	for i, key := range keys {
		index[string(key)] = i
	}
	return scanWorkload{index: index, value: value, seen: make([]bool, len(keys))}
}

type scanResult struct {
	Entries        int   `json:"entries"`
	ValueBytesRead int64 `json:"value_bytes_read"`
	FirstEntryNS   int64 `json:"first_entry_ns"`
	ElapsedNS      int64 `json:"elapsed_ns"`
}

// The timed consumer verifies every key and value without retaining returned slices.
func (w scanWorkload) traverse(t testing.TB, db scanDB) scanResult {
	t.Helper()
	clear(w.seen)
	ctx := t.Context()
	var result scanResult
	start := time.Now()
	for entry, err := range db.Scan(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if result.Entries == 0 {
			result.FirstEntryNS = time.Since(start).Nanoseconds()
		}
		i, ok := w.index[string(entry.Key)]
		if !ok || w.seen[i] || !bytes.Equal(entry.Value, w.value) {
			t.Fatalf("scan returned an unknown, duplicate or changed entry: %x", entry.Key)
		}
		w.seen[i] = true
		result.Entries++
		result.ValueBytesRead += int64(len(entry.Value))
	}
	result.ElapsedNS = time.Since(start).Nanoseconds()
	if result.Entries != len(w.index) {
		t.Fatalf("scan returned %d of %d entries", result.Entries, len(w.index))
	}
	return result
}

type pausedScanResult struct {
	SnapshotEntries       int   `json:"snapshot_entries"`
	LiveEntriesAfter      int   `json:"live_entries_after"`
	DurablePuts           int   `json:"durable_puts"`
	DurableDeletes        int   `json:"durable_deletes"`
	OverwriteNS           int64 `json:"overwrite_ns"`
	DeleteNS              int64 `json:"delete_ns"`
	InsertNS              int64 `json:"insert_ns"`
	CheckpointNS          int64 `json:"checkpoint_ns"`
	PinnedDataBytes       int64 `json:"pinned_data_bytes"`
	SealedSourcesRetained int   `json:"sealed_sources_retained"`
	SealedSourcesRetired  int   `json:"sealed_sources_retired"`
	RetirementWaitNS      int64 `json:"retirement_wait_ns"`
}

func measurePausedScan(t testing.TB, f repackFixture) pausedScanResult {
	t.Helper()
	startCleaner := make(chan struct{})
	db, cleanup := f.openClone(t, nil, &engine.Hooks{CleanerStart: startCleaner})
	defer cleanup()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	// Reconstruct the immutable seed independently of Scan. Only digests are retained.
	keys, value, _ := measurementInput(len(f.value), int(f.frontier.LastUserSequence))
	seed := make(map[string][sha256.Size]byte, len(keys))
	for i, key := range keys {
		value[0] = f.value[0]
		if i < len(keys)-1 {
			value[0] ^= byte(i)
		}
		seed[string(key)] = sha256.Sum256(value)
	}
	current := maps.Clone(seed)
	var result pausedScanResult
	checkEntry := func(entry tarp.Entry, err error) {
		if err != nil {
			t.Fatal(err)
		}
		hash, ok := seed[string(entry.Key)]
		if !ok || sha256.Sum256(entry.Value) != hash {
			t.Fatalf("snapshot returned an unknown, duplicate or changed key: %x", entry.Key)
		}
		delete(seed, string(entry.Key))
		result.SnapshotEntries++
	}
	next, stop := iter.Pull2(db.Scan(ctx))
	defer stop()
	first, err, ok := next()
	if !ok {
		t.Fatal("scan unexpectedly empty")
	}
	checkEntry(first, err)

	// Choose two distinct unread keys, regardless of the first yielded file.
	var unread [][]byte
	for _, key := range keys[:3] {
		if !bytes.Equal(key, first.Key) {
			unread = append(unread, key)
		}
	}
	changed := bytes.Clone(f.value)
	changed[0] ^= 0x80
	inserted := []byte(fmt.Sprintf("%016x", len(keys)))
	start := time.Now()
	if err := db.Put(unread[0], changed); err != nil {
		t.Fatal(err)
	}
	result.OverwriteNS = time.Since(start).Nanoseconds()
	result.DurablePuts++
	start = time.Now()
	if err := db.Delete(unread[1]); err != nil {
		t.Fatal(err)
	}
	result.DeleteNS = time.Since(start).Nanoseconds()
	result.DurableDeletes++
	start = time.Now()
	if err := db.Put(inserted, f.value); err != nil {
		t.Fatal(err)
	}
	result.InsertNS = time.Since(start).Nanoseconds()
	result.DurablePuts++
	current[string(unread[0])] = sha256.Sum256(changed)
	delete(current, string(unread[1]))
	current[string(inserted)] = sha256.Sum256(f.value)

	start = time.Now()
	checkpoint, err := db.Compact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result.CheckpointNS = time.Since(start).Nanoseconds()
	result.PinnedDataBytes = checkpoint.PinnedRetiredDataBytes
	dir := engine.Observe(db).Dir()
	var oldPaths []string
	var sealedBytes int64
	for _, source := range f.inputs {
		path := filepath.Join(dir, storage.DataName(f.frontier.UUID, source.ID))
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("sealed source not retained while scan paused: %v", err)
		}
		oldPaths = append(oldPaths, path)
		sealedBytes += source.Bytes
	}
	result.SealedSourcesRetained = len(oldPaths)
	if checkpoint.PinnedRetiredDataBytes < sealedBytes {
		t.Fatalf("checkpoint reports %d pinned bytes, want at least %d", checkpoint.PinnedRetiredDataBytes, sealedBytes)
	}

	close(startCleaner)
	for {
		entry, err, ok := next()
		if !ok {
			break
		}
		checkEntry(entry, err)
	}
	if len(seed) != 0 {
		t.Fatalf("scan omitted %d original entries", len(seed))
	}

	// Some sources can retire during traversal. This is the residual time from
	// traversal completion until all original sealed files are observed absent.
	start = time.Now()
	for _, path := range oldPaths {
		for {
			_, err := os.Stat(path)
			if errors.Is(err, os.ErrNotExist) {
				result.SealedSourcesRetired++
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatalf("retiring %s: %v", path, ctx.Err())
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	result.RetirementWaitNS = time.Since(start).Nanoseconds()
	result.LiveEntriesAfter = engine.Observe(db).LiveKeys()
	if result.LiveEntriesAfter != len(current) {
		t.Fatalf("current DB has %d keys, want %d", result.LiveEntriesAfter, len(current))
	}
	for key, want := range current {
		value, err := db.Get([]byte(key))
		if err != nil || sha256.Sum256(value) != want {
			t.Fatalf("unexpected current value for %q: %v", key, err)
		}
	}
	return result
}

func BenchmarkScan(b *testing.B) {
	for _, size := range []int{64, 1024, 4096} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.Run("full-warm", func(b *testing.B) {
				db, keys, value := benchmarkDB(b, size, false)
				work := newScanWorkload(keys, value)
				work.traverse(b, db)
				b.SetBytes(int64(len(keys) * len(value)))
				var firstNS int64
				for b.Loop() {
					firstNS += work.traverse(b, db).FirstEntryNS
				}
				b.ReportMetric(float64(firstNS)/float64(b.N), "ns/first-entry")
				b.ReportMetric(float64(len(keys)), "entries/scan")
			})
			b.Run("paused-checkpoint", func(b *testing.B) {
				fixture := prepareRepackFixture(b, size)
				var putNS, checkpointNS, retirementNS, pinnedBytes int64
				for b.Loop() {
					result := measurePausedScan(b, fixture)
					putNS += result.OverwriteNS + result.InsertNS
					checkpointNS += result.CheckpointNS
					retirementNS += result.RetirementWaitNS
					pinnedBytes += result.PinnedDataBytes
				}
				n := float64(b.N)
				b.ReportMetric(float64(putNS)/(2*n), "ns/paused-Put")
				b.ReportMetric(float64(checkpointNS)/n, "ns/checkpoint")
				b.ReportMetric(float64(retirementNS)/n, "ns/post-scan-retirement")
				b.ReportMetric(float64(pinnedBytes)/n, "pinned-B/window")
			})
		})
	}
}

func TestScanMeasurements(t *testing.T) {
	const size = 64
	keys, value, opts := measurementInput(size, 16)
	db := newTestDB(t, opts, nil)
	for _, key := range keys {
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	work := newScanWorkload(keys, value)
	work.traverse(t, db)
	full := work.traverse(t, db)
	if full.Entries != len(keys) || full.ValueBytesRead != int64(len(keys)*size) ||
		full.FirstEntryNS <= 0 || full.ElapsedNS < full.FirstEntryNS {
		t.Fatalf("full scan: %+v", full)
	}
	fixture := prepareRepackFixture(t, size)
	r := measurePausedScan(t, fixture)
	if r.SnapshotEntries != 65 || r.LiveEntriesAfter != 65 || r.DurablePuts != 2 || r.DurableDeletes != 1 ||
		r.SealedSourcesRetained != 8 || r.SealedSourcesRetired != 8 || r.PinnedDataBytes <= 0 ||
		r.CheckpointNS <= 0 || r.OverwriteNS <= 0 || r.DeleteNS <= 0 || r.InsertNS <= 0 || r.RetirementWaitNS <= 0 {
		t.Fatalf("paused scan: %+v", r)
	}
}
