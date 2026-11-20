package storage

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestRelocationsStreamCheckedAddressesAcrossBufferBoundaries(t *testing.T) {
	var moves Relocations
	defer moves.Close()
	key := strings.Repeat("\x00key\xff", 10_000)
	in := &File{ID: 4}
	out := &File{ID: 12}
	old := Location{File: in, Offset: 100, ValueBytes: 2, Sequence: 23, Bytes: format.RecordHeaderBytes + int64(len(key)) + 2}
	next := old
	next.File = out
	next.Offset = 200
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

func TestRelocationsSpillOnlyBeyondCountOrByteLimit(t *testing.T) {
	for _, tc := range []struct {
		name            string
		count, keyBytes int
		spill           bool
	}{{"small", moveBatchCount, 1, false}, {"many", moveBatchCount + 1, 1, true},
		{"exact bytes", 1, moveBatchBytes - relocationHeader, false}, {"large keys", 3, moveBatchBytes, true}} {
		t.Run(tc.name, func(t *testing.T) {
			var moves Relocations
			defer moves.Close()
			base := Location{File: &File{ID: 1}, Offset: format.SegmentHeaderBytes, Bytes: format.RecordHeaderBytes + int64(tc.keyBytes)}
			next := base
			next.File = &File{ID: 2}
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
			if (moves.fd != nil) != tc.spill {
				t.Fatalf("spilled = %t, want %t", moves.fd != nil, tc.spill)
			}
			seen := 0
			err := moves.EachBatch(func(batch []Relocation) error {
				if len(batch) > moveBatchCount || len(batch) > 1 && len(batch)*(relocationHeader+tc.keyBytes) > moveBatchBytes {
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
