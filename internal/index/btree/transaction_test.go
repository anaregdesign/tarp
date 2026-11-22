package btree

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
)

func TestOverwriteKeepsPinnedAddressesAcrossSpillAndAbort(t *testing.T) {
	tr := openTest(t)
	keys := make([]string, 2048)
	original := make(map[string]index.Address, len(keys))
	err := tr.BuildSorted(t.Context(), func(put func(string, index.Address) error) error {
		for i := range keys {
			key := fmt.Sprintf("%04d/%s", i, strings.Repeat("v", 96))
			if i == len(keys)-1 {
				key += strings.Repeat("z", pageSize+1)
			}
			keys[i], original[key] = key, addr(uint64(i))
			if err := put(key, original[key]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := tr.View()
	defer snapshot.Close()
	for _, commit := range []bool{false, true} {
		want := maps.Clone(original)
		tx := tr.Begin()
		defer tx.Abort()
		before := tr.Metrics()
		for pass := range 2 {
			for i, key := range keys {
				if i%31 != 0 && i != len(keys)-1 {
					continue
				}
				next := index.Address{FileID: uint64(10 + pass), RecordOffset: int64(i * 128), RecordBytes: 100, ValueBytes: 32, Sequence: uint64(3000 + pass*len(keys) + i)}
				old, found, err := tx.Apply(index.Mutation{Key: key, Address: next})
				if err != nil || !found || old != want[key] {
					t.Fatalf("overwrite %d on pass %d returned %v %t %v, want %v", i, pass, old, found, err, want[key])
				}
				want[key] = next
			}
		}
		after := tr.Metrics()
		if after.SpillWrites == before.SpillWrites || after.LeafReads == before.LeafReads {
			t.Fatal("overwrites must exercise evicted leaf pages")
		}
		verify(t, snapshot, original)
		if commit {
			tx.Commit()
		} else {
			tx.Abort()
			want = original
		}
		current := tr.View()
		verify(t, current, want)
		current.Close()
		verify(t, snapshot, original)
	}
}

func TestOverwriteSpillFailurePreservesPreviousAddressForRetry(t *testing.T) {
	tr := openTest(t)
	tr.p.capacity = 1 // The first COW overwrite must spill the published leaf.
	tx := tr.Begin()
	defer tx.Abort()
	if err := tx.Put("key", addr(1)); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	v := tr.View()
	defer v.Close()

	file := tr.p.file
	tr.p.file = failScratchWrite{pageFile: file}
	tx = tr.Begin()
	old, found, err := tx.Apply(index.Mutation{Key: "key", Address: addr(2)})
	tx.Abort()
	tr.p.file = file
	var lookupErr *index.LookupError
	if !errors.Is(err, syscall.ENOSPC) || errors.As(err, &lookupErr) || !found || old != addr(1) {
		t.Fatalf("failed overwrite returned %v %t %v", old, found, err)
	}
	tx = tr.Begin()
	defer tx.Abort()
	old, found, err = tx.Apply(index.Mutation{Key: "key", Address: addr(2)})
	if err != nil || !found || old != addr(1) {
		t.Fatalf("retry returned %v %t %v", old, found, err)
	}
	tx.Commit()
	verify(t, v, map[string]index.Address{"key": addr(1)})
	if got, ok, err := tr.Lookup("key"); err != nil || !ok || got != addr(2) {
		t.Fatalf("after retry: %v %t %v", got, ok, err)
	}
}
