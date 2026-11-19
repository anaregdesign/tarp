package benchmark

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Run once per fresh test process with TARP_BATCH_PUT_MEMORY=empty or rotate.
// Collecting a live heap here forces GC. Keep these runs separate from timing.
func TestBatchPutMemory(t *testing.T) {
	mode := os.Getenv("TARP_BATCH_PUT_MEMORY")
	if mode == "" {
		t.Skip("set TARP_BATCH_PUT_MEMORY=empty or rotate")
	}
	if mode != "empty" && mode != "rotate" {
		t.Fatalf("unknown TARP_BATCH_PUT_MEMORY %q", mode)
	}

	const encodedBytes = 4<<20 - format.SegmentHeaderBytes
	const survivorBytes = 32
	keys := make([][]byte, 6)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%016x", i))
	}
	survivor := keys[len(keys)-1]
	smallRecord := format.RecordHeaderBytes + len(survivor) + survivorBytes
	// Three maximum records, two around half maximum, and one small survivor.
	recordSizes := []int{1 << 20, 1 << 20, 1 << 20, 1 << 19,
		encodedBytes - 3*(1<<20) - (1 << 19) - smallRecord, smallRecord}

	var measuring bool
	var writeHeap uint64
	var dataBytes, writes int
	// Park the cleaner so that reclamation cannot race the retained-heap sample.
	fs := &storage.IO{
		CleanerStart: make(chan struct{}),
		Hook: func(point string) error {
			if measuring && point == "data.write:before" && writeHeap == 0 {
				writeHeap = batchLiveHeap()
			}
			return nil
		},
		Wrote: func(point string, n int) {
			if measuring && point == "data.write" {
				writes++
				dataBytes += n
			}
		},
	}
	db, err := engine.OpenWithIO(filepath.Join(t.TempDir(), "db"), engine.Options{}, fs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	wantSegments := uint64(1)
	if mode == "rotate" {
		if err := db.Put(keys[0], []byte("old")); err != nil {
			t.Fatal(err)
		}
		wantSegments++
	}
	before := db.Stats()
	openHeap := batchLiveHeap()
	entries := make([]engine.Entry, len(keys))
	for i, key := range keys {
		n := recordSizes[i] - format.RecordHeaderBytes - len(key)
		entries[i] = engine.Entry{Key: key, Value: bytes.Repeat([]byte{byte(i + 1)}, n)}
	}
	inputHeap := batchLiveHeap()
	measuring = true
	err = db.BatchPut(entries)
	measuring = false
	if err != nil {
		t.Fatal(err)
	}
	publishedHeap := batchLiveHeap()
	// This sample includes both caller-owned inputs and any bytes retained by DB.
	runtime.KeepAlive(entries)
	entries = nil
	releasedHeap := batchLiveHeap()
	after := db.Stats()
	if writeHeap == 0 || dataBytes != encodedBytes || after.SelectedSegments != wantSegments ||
		after.LiveKeys != uint64(len(keys)) || after.WriteCommits != before.WriteCommits+1 {
		t.Fatalf("invalid publication: data bytes %d, writes %d, heap %d, Stats %+v", dataBytes, writes, writeHeap, after)
	}

	if err := db.BatchDelete(keys[:len(keys)-1]); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get(survivor)
	if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{byte(len(keys))}, survivorBytes)) || db.Stats().LiveKeys != 1 {
		t.Fatalf("incorrect survivor: %x, %v, Stats %+v", got, err, db.Stats())
	}
	retainedHeap := batchLiveHeap()
	var rss syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rss); err != nil {
		t.Fatal(err)
	}
	peakRSS := int64(rss.Maxrss)
	if runtime.GOOS == "linux" {
		peakRSS *= 1024
	}
	row := map[string]any{
		"mode": mode, "encoded_batch_bytes": encodedBytes, "data_writes": writes,
		"selected_segments_after_put": after.SelectedSegments, "surviving_value_bytes": survivorBytes,
		"open_heap_bytes": openHeap, "input_ready_heap_bytes": inputHeap,
		"first_data_write_heap_bytes": writeHeap, "published_inputs_live_heap_bytes": publishedHeap,
		"published_inputs_released_heap_bytes": releasedHeap, "survivor_heap_bytes": retainedHeap,
		"survivor_heap_delta_over_open_bytes": int64(retainedHeap) - int64(openHeap),
		"process_peak_rss_bytes":              peakRSS,
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("batch_put_memory %s", encoded)
}

func batchLiveHeap() uint64 {
	runtime.GC()
	var state runtime.MemStats
	runtime.ReadMemStats(&state)
	return state.HeapAlloc
}
