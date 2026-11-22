package benchmark

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

const containerBatchSlots = 32

type containerBatchFixture struct {
	dir      string
	opts     tarp.Options
	keys     [][]byte // Cold keys first, followed by 32 resident control keys.
	tails    [][]byte // Shared expected value bytes following a unique key prefix.
	requests int
}

func prepareContainerBatchFixture(t *testing.T, dir string, valueBytes, requests int) *containerBatchFixture {
	t.Helper()
	if valueBytes < 8 || requests <= 0 {
		t.Fatalf("invalid BatchGet fixture: value bytes %d, requests %d", valueBytes, requests)
	}
	coldKeys := containerBatchSlots * requests
	f := &containerBatchFixture{
		dir:      dir,
		opts:     tarp.Options{ValueCacheBudgetBytes: max(4<<20, int64(containerBatchSlots*(valueBytes+128))), ReadFileHandles: 16},
		keys:     make([][]byte, coldKeys+containerBatchSlots),
		tails:    make([][]byte, 256),
		requests: requests,
	}
	payload := rand.NewChaCha8([32]byte{72})
	for i := range f.tails {
		f.tails[i] = make([]byte, valueBytes-8)
		_, _ = payload.Read(f.tails[i])
	}

	db, err := tarp.Open(dir, f.opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	// Bound seed memory and keep each BatchPut below the default segment target.
	var entries []tarp.Entry
	var pendingBytes int
	flush := func() {
		if err := db.BatchPut(entries); err != nil {
			t.Fatal(err)
		}
		entries = entries[:0]
		pendingBytes = 0
	}
	appendEntry := func(key, value []byte) {
		if len(entries) == 256 || pendingBytes+len(value) > 2<<20 {
			flush()
		}
		entries = append(entries, tarp.Entry{Key: key, Value: value})
		pendingBytes += len(value)
	}
	// Separate small cold records with live filler records so two requested
	// records cannot share an OS page. Readahead can still warm neighboring OS pages.
	padding := make([]byte, os.Getpagesize())
	for i := range f.keys {
		key := binary.BigEndian.AppendUint64(nil, uint64(i))
		f.keys[i] = key
		value := make([]byte, valueBytes)
		copy(value, key)
		copy(value[len(key):], f.tails[i%len(f.tails)])
		appendEntry(key, value)
		if valueBytes < len(padding) && i < coldKeys {
			fillerKey := binary.BigEndian.AppendUint64(nil, 1<<63|uint64(i))
			appendEntry(fillerKey, padding)
		}
	}
	flush()
	return f
}

// The caller owns the returned DB. Opening it and priming the hit control
// precede any OS cache preparation and timed requests.
func (f *containerBatchFixture) open(t *testing.T, limit int) *tarp.DB {
	t.Helper()
	opts := f.opts
	opts.BatchGetReadConcurrency = limit
	db, err := tarp.Open(f.dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for i := f.requests * containerBatchSlots; i < len(f.keys); i++ {
		key := f.keys[i]
		value, err := db.Get(key)
		if err != nil || len(value) != len(key)+len(f.tails[i%len(f.tails)]) ||
			!bytes.Equal(value[:len(key)], key) || !bytes.Equal(value[len(key):], f.tails[i%len(f.tails)]) {
			db.Close()
			t.Fatalf("priming hot key %d: incorrect value or Get error %v", i, err)
		}
	}
	return db
}

type containerBatchStats struct {
	ValueCacheHits    uint64 `json:"value_cache_hits"`
	ValueCacheMisses  uint64 `json:"value_cache_misses"`
	MissingKeyLookups uint64 `json:"missing_key_lookups"`
	DuplicateInputs   uint64 `json:"duplicate_inputs"`
}

type containerBatchRequest struct {
	keys  [containerBatchSlots][]byte
	tails [containerBatchSlots][]byte
	want  containerBatchStats
}

func (f *containerBatchFixture) trace(seed int64, mode string) []containerBatchRequest {
	coldKeys := containerBatchSlots * f.requests
	rng := rand.New(rand.NewPCG(uint64(seed), 72))
	order := rng.Perm(coldKeys)
	requests := make([]containerBatchRequest, f.requests)
	for i := range requests {
		req := &requests[i]
		ids := order[i*containerBatchSlots : (i+1)*containerBatchSlots]
		switch mode {
		case "distinct":
			req.want.ValueCacheMisses = containerBatchSlots
		case "value-cache-hit":
			req.want.ValueCacheHits = containerBatchSlots
			for j, hot := range rng.Perm(containerBatchSlots) {
				ids[j] = coldKeys + hot
			}
		case "duplicate":
			req.want.ValueCacheMisses = 1
			req.want.DuplicateInputs = containerBatchSlots - 1
			for j := range ids {
				ids[j] = ids[0]
			}
		default:
			panic("unknown container BatchGet trace mode: " + mode)
		}
		for j, id := range ids {
			req.keys[j] = f.keys[id]
			req.tails[j] = f.tails[id%len(f.tails)]
		}
	}
	return requests
}

// A repeated disk key across two requests would still pass the per-call
// BatchGet Stats checks, but it would invalidate a first-touch trace.
func TestContainerBatchDistinctTraceVisitsEachColdKeyOnce(t *testing.T) {
	const calls = 8
	f := &containerBatchFixture{
		requests: calls,
		keys:     make([][]byte, (calls+1)*containerBatchSlots),
		tails:    [][]byte{nil},
	}
	for id := range f.keys {
		f.keys[id] = binary.BigEndian.AppendUint64(nil, uint64(id))
	}
	seen := make([]bool, calls*containerBatchSlots)
	control := f.trace(42, "distinct")
	for i, req := range f.trace(42, "distinct") {
		for j, key := range req.keys {
			id := binary.BigEndian.Uint64(key)
			if id >= uint64(len(seen)) || seen[id] {
				t.Fatalf("request %d, position %d: repeated or non-cold key %d", i, j, id)
			}
			if !bytes.Equal(key, control[i].keys[j]) {
				t.Fatalf("seed did not reproduce request %d, position %d", i, j)
			}
			seen[id] = true
		}
	}
}

func (r *containerBatchRequest) check(results []tarp.BatchResult) error {
	if len(results) != len(r.keys) {
		return fmt.Errorf("received %d slots, want %d", len(results), len(r.keys))
	}
	for j, result := range results {
		key, tail := r.keys[j], r.tails[j]
		if !result.Found || len(result.Value) != len(key)+len(tail) ||
			!bytes.Equal(result.Value[:len(key)], key) || !bytes.Equal(result.Value[len(key):], tail) {
			return fmt.Errorf("slot %d: incorrect value for key %x", j, key)
		}
		if r.want.DuplicateInputs > 0 {
			// A later duplicate must retain its original bytes.
			result.Value[0] ^= 0xff
		}
	}
	return nil
}

type containerBatchRun struct {
	WallNS     int64               `json:"wall_ns"`
	SamplesNS  [][]int64           `json:"samples_ns"`
	StatsDelta containerBatchStats `json:"stats_delta"`
}

// Sample time covers only the public BatchGet call. Wall time additionally
// includes result validation between calls and the closed-loop client barrier.
func runContainerBatchRequests(db *tarp.DB, requests []containerBatchRequest, clients int) (containerBatchRun, error) {
	if clients <= 0 || len(requests) < clients || len(requests)%clients != 0 {
		return containerBatchRun{}, fmt.Errorf("cannot divide %d requests evenly among %d clients", len(requests), clients)
	}
	run := containerBatchRun{SamplesNS: make([][]int64, clients)}
	var want containerBatchStats
	for i := range requests {
		want.ValueCacheHits += requests[i].want.ValueCacheHits
		want.ValueCacheMisses += requests[i].want.ValueCacheMisses
		want.DuplicateInputs += requests[i].want.DuplicateInputs
	}
	before := db.Stats()
	start := make(chan struct{})
	errs := make([]error, clients)
	var ready, done sync.WaitGroup
	ready.Add(clients)
	for client := range clients {
		samples := make([]int64, len(requests)/clients)
		run.SamplesNS[client] = samples
		done.Go(func() {
			ready.Done()
			<-start
			for j := range samples {
				index := j*clients + client
				req := &requests[index]
				begin := time.Now()
				values, err := db.BatchGet(req.keys[:])
				samples[j] = time.Since(begin).Nanoseconds()
				if err == nil {
					err = req.check(values)
				}
				if err != nil {
					errs[client] = fmt.Errorf("client %d request %d: %w", client, index, err)
					return
				}
			}
		})
	}
	ready.Wait()
	started := time.Now()
	close(start)
	done.Wait()
	run.WallNS = time.Since(started).Nanoseconds()
	for _, err := range errs {
		if err != nil {
			return run, err
		}
	}
	after := db.Stats()
	run.StatsDelta = containerBatchStats{
		ValueCacheHits:    after.ValueCache.Hits - before.ValueCache.Hits,
		ValueCacheMisses:  after.ValueCache.Misses - before.ValueCache.Misses,
		MissingKeyLookups: after.MissingKeyLookups - before.MissingKeyLookups,
		DuplicateInputs:   after.BatchGetDuplicateInputs - before.BatchGetDuplicateInputs,
	}
	if run.StatsDelta != want {
		return run, fmt.Errorf("BatchGet Stats delta %+v, want %+v", run.StatsDelta, want)
	}
	return run, nil
}
