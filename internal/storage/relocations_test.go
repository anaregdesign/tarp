package storage

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestRelocationsStreamCheckedAddressesAcrossBufferBoundaries(t *testing.T) {
	store := &Store{Dir: t.TempDir()}
	moves := store.NewRelocations()
	defer moves.Close()
	key := strings.Repeat("\x00key\xff", 10_000)
	in := &DataSegment{ID: 4}
	out := &DataSegment{ID: 12}
	old := Location{File: in, RecordOffset: 100, ValueBytes: 2, Sequence: 23, RecordBytes: format.RecordHeaderBytes + int64(len(key)) + 2}
	next := old
	next.File = out
	next.RecordOffset = 200
	for range 3 {
		if err := moves.append(key, old, next); err != nil {
			t.Fatal(err)
		}
	}
	if err := moves.seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(moves.fd.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch file remains linked: %v", err)
	}
	verify := func() error {
		calls := 0
		err := moves.Each(func(k string, a, b Location) error {
			if k != key || a != old || b != next {
				t.Fatal("relocation changed")
			}
			calls++
			return nil
		})
		if err == nil && calls != 3 {
			t.Fatalf("visited %d moves", calls)
		}
		return err
	}
	if err := verify(); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err != nil {
		t.Fatal(err)
	}
	if _, err := moves.fd.WriteAt([]byte{42}, relocationHeader+10); err != nil {
		t.Fatal(err)
	}
	var work *IndexWorkError
	if err := verify(); !errors.As(err, &work) {
		t.Fatalf("invalid scratch record returned %v", err)
	}
}

func TestRelocationsBatchLimitsOnReplay(t *testing.T) {
	store := &Store{Dir: t.TempDir()}
	moves := store.NewRelocations()
	defer moves.Close()
	src, dst := &DataSegment{ID: 1}, &DataSegment{ID: 2}
	appendKey := func(key string) {
		t.Helper()
		old := Location{File: src, RecordOffset: format.SegmentHeaderBytes, RecordBytes: format.RecordHeaderBytes + int64(len(key))}
		next := old
		next.File = dst
		if err := moves.append(key, old, next); err != nil {
			t.Fatal(err)
		}
	}
	for range 150 {
		appendKey(strings.Repeat("k", 16))
	}
	appendKey(strings.Repeat("L", moveBatchBytes))
	appendKey("tail")
	if err := moves.seal(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		maxCount int
		want     []int
	}{
		{32, []int{32, 32, 32, 32, 22, 1, 1}},
		{128, []int{moveBatchBytes / (relocationHeader + 16), 150 - moveBatchBytes/(relocationHeader+16), 1, 1}},
	} {
		var sizes []int
		err := moves.EachBatch(tc.maxCount, func(batch []Relocation) error {
			sizes = append(sizes, len(batch))
			return nil
		})
		if err != nil || !slices.Equal(sizes, tc.want) {
			t.Fatalf("limit %d: got %v, %v; want %v", tc.maxCount, sizes, err, tc.want)
		}
	}
}

func TestRelocationsSpillOnlyBeyondCountOrByteLimit(t *testing.T) {
	for _, tc := range []struct {
		name            string
		count, keyBytes int
		spill           bool
	}{{"small", RelocationBatchCount, 1, false}, {"many", RelocationBatchCount + 1, 1, true},
		{"exact bytes", 1, moveBatchBytes - relocationHeader, false}, {"large keys", 3, moveBatchBytes, true}} {
		t.Run(tc.name, func(t *testing.T) {
			store := &Store{Dir: t.TempDir()}
			t.Setenv("TMPDIR", filepath.Join(store.Dir, "missing-temp"))
			moves := store.NewRelocations()
			defer moves.Close()
			base := Location{File: &DataSegment{ID: 1}, RecordOffset: format.SegmentHeaderBytes, RecordBytes: format.RecordHeaderBytes + int64(tc.keyBytes)}
			next := base
			next.File = &DataSegment{ID: 2}
			key := strings.Repeat("k", tc.keyBytes)
			for i := range tc.count {
				base.Sequence, next.Sequence = uint64(i+1), uint64(i+1)
				if err := moves.append(key, base, next); err != nil {
					t.Fatal(err)
				}
			}
			if err := moves.seal(); err != nil {
				t.Fatal(err)
			}
			resident, ok := moves.Resident()
			if ok == tc.spill || ok && len(resident) != tc.count || !ok && resident != nil {
				t.Fatalf("resident batch has %d moves, ok=%t, expected spill=%t", len(resident), ok, tc.spill)
			}
			workspace := filepath.Join(store.Dir, workspaceName)
			if tc.spill {
				if filepath.Dir(moves.fd.Name()) != workspace {
					t.Fatalf("relocation scratch %q, want directory %q", moves.fd.Name(), workspace)
				}
			} else if _, err := os.Lstat(workspace); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("in-memory relocation created a workspace: %v", err)
			}
			seen := 0
			err := moves.EachBatch(RelocationBatchCount, func(batch []Relocation) error {
				if len(batch) > RelocationBatchCount || len(batch) > 1 && len(batch)*(relocationHeader+tc.keyBytes) > moveBatchBytes {
					t.Fatalf("batch length %d exceeds fixed window", len(batch))
				}
				for _, move := range batch {
					seen++
					if move.Key != key || move.Old.File != base.File || move.Next.File != next.File || move.Old.Sequence != uint64(seen) || move.Next.Sequence != uint64(seen) {
						t.Fatalf("move %d: %+v", seen, move)
					}
				}
				return nil
			})
			if err != nil || seen != tc.count {
				t.Fatalf("visited %d of %d, error %v", seen, tc.count, err)
			}
		})
	}
}

func TestRelocationSpillCanRetryAfterWorkspaceFailure(t *testing.T) {
	store := &Store{Dir: t.TempDir()}
	workspace := filepath.Join(store.Dir, workspaceName)
	if err := os.WriteFile(workspace, []byte("occupied"), 0600); err != nil {
		t.Fatal(err)
	}
	moves := store.NewRelocations()
	defer moves.Close()
	old := Location{File: &DataSegment{ID: 1}, RecordOffset: format.SegmentHeaderBytes, RecordBytes: format.RecordHeaderBytes + 1}
	next := old
	next.File = &DataSegment{ID: 2}
	for range RelocationBatchCount {
		if err := moves.append("k", old, next); err != nil {
			t.Fatal(err)
		}
	}
	var work *IndexWorkError
	if err := moves.append("k", old, next); !errors.As(err, &work) {
		t.Fatalf("spill returned %v, want an index workspace error", err)
	}
	if moves.fd != nil || moves.count != RelocationBatchCount {
		t.Fatalf("failed spill changed buffered moves: %d, fd %v", moves.count, moves.fd)
	}
	if got, err := os.ReadFile(workspace); err != nil || string(got) != "occupied" {
		t.Fatalf("workspace collision: %q, %v", got, err)
	}
	if err := os.Remove(workspace); err != nil {
		t.Fatal(err)
	}
	if err := moves.append("k", old, next); err != nil {
		t.Fatal(err)
	}
	if moves.fd == nil || moves.count != RelocationBatchCount+1 {
		t.Fatalf("retry left %d moves, fd %v", moves.count, moves.fd)
	}
}
