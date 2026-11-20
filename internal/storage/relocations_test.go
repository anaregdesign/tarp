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
