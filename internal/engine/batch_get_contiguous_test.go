package engine

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestContiguousRecordCountRequiresBoundedRunsInOneFile(t *testing.T) {
	segment := &storage.DataSegment{}
	read := func(f *storage.DataSegment, offset, size int64) batchRead {
		return batchRead{loc: storage.Location{Segment: f, RecordOffset: offset, RecordBytes: size}}
	}
	for _, tc := range []struct {
		reads []batchRead
		want  int
	}{
		{[]batchRead{read(segment, 5, 8), read(segment, 13, 8), read(segment, 21, 8)}, 3},
		{[]batchRead{read(segment, 5, 8), read(segment, 14, 8)}, 1},
		{[]batchRead{read(segment, 13, 8), read(segment, 5, 8)}, 1},
		{[]batchRead{read(segment, 5, 8), read(&storage.DataSegment{}, 13, 8)}, 1},
		{[]batchRead{read(segment, 5, 8), {}}, 1},
		{[]batchRead{read(segment, 0, 8), read(segment, 8, storage.MaxContiguousReadBytes-8)}, 2},
		{[]batchRead{read(segment, 0, 8), read(segment, 8, storage.MaxContiguousReadBytes-7)}, 1},
		{[]batchRead{read(segment, math.MaxInt64, 8), read(segment, math.MinInt64+7, 8)}, 1},
	} {
		if got := contiguousRecordCount(tc.reads); got != tc.want {
			t.Errorf("group size %d, want %d", got, tc.want)
		}
	}
}

func TestBatchGetContiguousChecksShareReadSlotAndReleasePinsOnFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		var started atomic.Int32
		db := newTestDBWithHooks(t, Options{ValueCacheBudgetBytes: 130, BatchGetReadConcurrency: 1}, &storage.IO{Hook: func(point string) error {
			if point == "read:before" {
				n := started.Add(1)
				if n == 1 {
					<-gate
				}
				if n == 2 {
					return format.ErrCorrupt
				}
			}
			return nil
		}}, &Hooks{CleanerStart: make(chan struct{})})
		for _, k := range []string{"a", "b", "c", "z"} {
			mustPut(t, db, k, k)
		}
		segment := testLoc(t, db, "a").Segment
		refs := segment.References()
		first := startBatchGet(db, [][]byte{[]byte("a"), []byte("b"), []byte("a")})
		synctest.Wait()
		second := startBatchGet(db, [][]byte{[]byte("c")})
		synctest.Wait()
		if started.Load() != 1 || segment.References() != refs+3 {
			t.Fatalf("paused reads %d, pins %d", started.Load(), segment.References())
		}
		release()
		if out := <-first; out.results != nil || !errors.Is(out.err, format.ErrCorrupt) {
			t.Fatalf("group: %+v", out)
		}
		if out := <-second; out.results != nil || !errors.Is(out.err, ErrUnhealthy) {
			t.Fatalf("single: %+v", out)
		}
		if segment.References() != refs {
			t.Fatalf("leaked pins: %d", segment.References())
		}
	})
}

func TestBatchGetContiguousRunsReadInParallelWithinSharedLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		release := sync.OnceFunc(func() { close(gate) })
		defer release()
		var ranges atomic.Int32
		var records atomic.Int32
		db := newTestDBWithHooks(t, Options{BatchGetReadConcurrency: 2}, &storage.IO{Hook: func(point string) error {
			switch point {
			case "read.contiguous:before":
				ranges.Add(1)
				<-gate
			case "read:before":
				records.Add(1)
			}
			return nil
		}}, &Hooks{CleanerStart: make(chan struct{})})
		// Eight contiguous records of this size split into four two-record ranges.
		value := strings.Repeat("v", 3000)
		keys := [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f"), []byte("g"), []byte("h")}
		for _, k := range keys {
			mustPut(t, db, string(k), value)
		}
		Observe(db).ClearCache()
		first := startBatchGet(db, keys)
		synctest.Wait()
		other := startBatchGet(db, keys[:1])
		synctest.Wait()
		if ranges.Load() != 2 || records.Load() != 4 {
			t.Fatalf("while blocked: %d ranges, %d records", ranges.Load(), records.Load())
		}
		release()
		for _, done := range []<-chan batchGetOutcome{first, other} {
			out := <-done
			if out.err != nil {
				t.Fatal(out.err)
			}
			for _, r := range out.results {
				if !r.Found || string(r.Value) != value {
					t.Fatal("incorrect read")
				}
			}
		}
		if ranges.Load() != 4 || records.Load() != 9 {
			t.Fatalf("completed %d ranges, %d records", ranges.Load(), records.Load())
		}
	})
}

func TestBatchGetGroups32ContiguousColdRecordsButReads33Individually(t *testing.T) {
	var grouped atomic.Int32
	db := newTestDBWithHooks(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "read.contiguous:before" {
			grouped.Add(1)
		}
		return nil
	}}, &Hooks{CleanerStart: make(chan struct{})})
	keys := make([][]byte, 33)
	entries := make([]Entry, len(keys))
	for i := range keys {
		keys[i] = []byte{byte(i)}
		entries[i] = Entry{Key: keys[i], Value: []byte{0xff, byte(i)}}
	}
	if err := db.BatchPut(entries); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		count  int
		groups int32
	}{{32, 1}, {33, 0}} {
		Observe(db).ClearCache()
		grouped.Store(0)
		got, err := db.BatchGet(keys[:tc.count])
		if err != nil || len(got) != tc.count || grouped.Load() != tc.groups {
			t.Fatalf("%d cold records: got %d results, %d grouped reads, %v", tc.count, len(got), grouped.Load(), err)
		}
		for i, r := range got {
			if !r.Found || !bytes.Equal(r.Value, entries[i].Value) {
				t.Fatalf("%d cold records, slot %d: %+v", tc.count, i, r)
			}
		}
	}
}

func TestBatchGetScatteredPrefixKeepsIndividualReads(t *testing.T) {
	var grouped, individual atomic.Int32
	db := newTestDBWithHooks(t, Options{ValueCacheBudgetBytes: 130}, &storage.IO{Hook: func(point string) error {
		switch point {
		case "read.contiguous:before":
			grouped.Add(1)
		case "read:before":
			individual.Add(1)
		}
		return nil
	}}, &Hooks{CleanerStart: make(chan struct{})})
	for _, key := range []string{"a", "b", "c", "d", "z"} {
		mustPut(t, db, key, key)
	}
	keys := [][]byte{[]byte("a"), []byte("c"), []byte("d")}
	got, err := db.BatchGet(keys)
	if err != nil || len(got) != len(keys) {
		t.Fatalf("BatchGet returned %v, %v", got, err)
	}
	for i, r := range got {
		if !r.Found || string(r.Value) != string(keys[i]) {
			t.Fatalf("slot %d: %+v", i, r)
		}
	}
	if grouped.Load() != 0 || individual.Load() != 3 {
		t.Fatalf("ranges=%d, records=%d", grouped.Load(), individual.Load())
	}
}
