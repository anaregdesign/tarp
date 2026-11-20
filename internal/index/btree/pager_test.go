package btree

import (
	"errors"
	"io"
	"strings"
	"syscall"
	"testing"
)

type failScratchWrite struct {
	pageFile
	short bool
}

func (f failScratchWrite) WriteAt(b []byte, off int64) (int, error) {
	if f.short {
		return 0, nil
	}
	return 0, syscall.ENOSPC
}
func TestSpillFailureAbortsBeforePublishingRoot(t *testing.T) {
	for _, short := range []bool{false, true} {
		tr := openTest(t)
		x := tr.Begin()
		if err := x.Put("old", addr(1)); err != nil {
			t.Fatal(err)
		}
		x.Commit()
		v := tr.View()
		tr.p.mu.Lock()
		file := tr.p.file
		tr.p.file = failScratchWrite{file, short}
		tr.p.mu.Unlock()
		x = tr.Begin()
		err := x.Put(strings.Repeat("k", int(MinBudget)), addr(2))
		want := error(syscall.ENOSPC)
		if short {
			want = io.ErrShortWrite
		}
		if !errors.Is(err, want) {
			t.Fatalf("spill = %v, want %v", err, want)
		}
		x.Abort()
		tr.p.mu.Lock()
		tr.p.file = file
		tr.p.mu.Unlock()
		a, found, err := v.Lookup("old")
		v.Close()
		if err != nil || !found || a != addr(1) || tr.Count() != 1 {
			t.Fatalf("lost pre-failure root: %+v %t %v", a, found, err)
		}
		x = tr.Begin()
		if err = x.Put("retry", addr(3)); err != nil {
			t.Fatal(err)
		}
		x.Commit()
	}
}
func TestRetirementQueueReusesConsumedSpaceWithOverlappingReaders(t *testing.T) {
	tr := openTest(t)
	q := &tr.retired
	// Keep a suffix of two epochs resident throughout the entire stream.
	for epoch := uint64(1); epoch <= 240; epoch++ {
		var records idLog
		for i := range 90 {
			if err := records.add(tr.p, 1+uint64(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := q.append(tr.p, epoch, &records); err != nil {
			t.Fatal(err)
		}
		if epoch > 2 {
			if err := q.collect(tr.p, epoch-1); err != nil {
				t.Fatal(err)
			}
		}
	}
	if q.end > 80<<10 {
		t.Fatalf("retirement log grew to %d with only two epochs live", q.end)
	}
}
