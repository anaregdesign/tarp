package btree

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
)

func TestOverflowReadsKeepValidatedLeavesCached(t *testing.T) {
	tr := openTest(t)
	x := tr.Begin()
	keys := make([]string, 128)
	for i := range keys {
		keys[i] = strings.Repeat("k", 2*pageSize) + fmt.Sprintf("%03d", i)
		if err := x.Put(keys[i], addr(uint64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	x.Commit()
	v := tr.View()
	defer v.Close()
	check := func() {
		t.Helper()
		for i, key := range keys {
			got, ok, err := v.Lookup(key)
			if err != nil || !ok || got != addr(uint64(i+1)) {
				t.Fatalf("lookup %d = %v, %t, %v", i, got, ok, err)
			}
		}
	}
	check()
	before := tr.Metrics()
	check()
	after := tr.Metrics()
	if after.LeafReads != before.LeafReads || after.BranchReads != before.BranchReads {
		t.Fatalf("overflow streaming evicted validated nodes: leaf reads +%d, branch reads +%d",
			after.LeafReads-before.LeafReads, after.BranchReads-before.BranchReads)
	}
	if after.OverflowReads <= before.OverflowReads || after.CacheBytes > uint64(tr.p.budget/2) {
		t.Fatalf("expected bounded overflow streaming: before %+v, after %+v", before, after)
	}
}

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
		old, found, err := x.Apply(index.Mutation{Key: strings.Repeat("k", int(MinBudget)), Address: addr(2)})
		want := error(syscall.ENOSPC)
		if short {
			want = io.ErrShortWrite
		}
		var lookupErr *index.LookupError
		if !errors.Is(err, want) || errors.As(err, &lookupErr) || found || old != (index.Address{}) {
			t.Errorf("spill after resolving absent key = %v %v %v, want %v", old, found, err, want)
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

func TestMetricsTrackChargeSpillsAndPinnedRootGenerations(t *testing.T) {
	tr := openTest(t)
	x := tr.Begin()
	if err := x.Put("old", addr(1)); err != nil {
		t.Fatal(err)
	}
	x.Commit()
	a, b := tr.View(), tr.View()
	defer a.Close()
	defer b.Close()
	x = tr.Begin()
	if err := x.Put(strings.Repeat("k", int(MinBudget)), addr(2)); err != nil {
		t.Fatal(err)
	}
	x.Commit()
	c := tr.View()
	m := tr.Metrics()
	if m.ActiveViews != 3 || m.PinnedRootGenerations != 2 || m.RetiredPages == 0 {
		t.Fatalf("root generations: %+v", m)
	}
	if m.CacheBytes > uint64(tr.p.budget/2) || m.CachePeakBytes < m.CacheBytes || m.ScratchBytes == 0 || m.ScratchPeakBytes < m.ScratchBytes || m.Evictions == 0 || m.SpillWrites == 0 || m.SpillNanoseconds == 0 {
		t.Fatalf("pager: %+v", m)
	}
	a.Close()
	b.Close()
	c.Close()
	x = tr.Begin()
	x.Abort()
	after := tr.Metrics()
	if after.ActiveViews != 0 || after.PinnedRootGenerations != 0 || after.RetiredPages != 0 || after.ScratchPeakBytes < m.ScratchPeakBytes {
		t.Fatalf("released: %+v", after)
	}
}
