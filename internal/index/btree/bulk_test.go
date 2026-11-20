package btree

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
)

func TestBulkConstructionWithMixedKeysAndCapturedEmptyView(t *testing.T) {
	tr := openTest(t)
	old := tr.View()
	defer old.Close()
	// Long distinct separators force multiple levels even with a small dataset.
	keys := make([]string, 5600)
	for i := range keys {
		keys[i] = fmt.Sprintf("%08x/%s", i, strings.Repeat(string(byte(i)), i%511))
	}
	for i := range 165 {
		keys = append(keys, strings.Repeat("zz", 1000)+fmt.Sprintf("%d", i))
	}
	sort.Strings(keys)
	if err := tr.BuildSorted(t.Context(), func(put func(string, index.Address) error) error {
		for i, k := range keys {
			if err := put(k, addr(uint64(i+1))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if tr.Count() != len(keys) {
		t.Fatalf("count %d", tr.Count())
	}
	v := tr.View()
	defer v.Close()
	i := 0
	if err := v.All(func(k string, a index.Address) bool {
		if i >= len(keys) || k != keys[i] || a != addr(uint64(i+1)) {
			t.Fatal("incorrect bulk enumeration")
		}
		i++
		return true
	}); err != nil || i != len(keys) {
		t.Fatalf("All: %d, %v", i, err)
	}
	for i := 0; i < len(keys); i += 129 {
		a, ok, err := v.Lookup(keys[i])
		if !ok || err != nil || a != addr(uint64(i+1)) {
			t.Fatalf("lookup %d: %v %t %v", i, a, ok, err)
		}
	}
	if err := old.All(func(string, index.Address) bool { t.Fatal("old root changed"); return false }); err != nil {
		t.Fatal(err)
	}
	x := tr.Begin()
	if err := x.Delete(keys[260]); err != nil {
		t.Fatal(err)
	}
	x.Commit()
	n, ok, err := v.Lookup(keys[260])
	if err != nil || !ok || n != addr(261) {
		t.Fatal("bulk snapshot lost after delete")
	}
}
func TestUnorderedBulkAndCancellationNeverPublish(t *testing.T) {
	tr := openTest(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err := tr.BuildSorted(ctx, func(put func(string, index.Address) error) error {
		for i := 0; i < 2025; i++ {
			if err := put(fmt.Sprintf("%08d", i), addr(uint64(i))); err != nil {
				return err
			}
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || tr.Count() != 0 {
		t.Fatalf("canceled build: count %d, %v", tr.Count(), err)
	}
	for _, keys := range [][]string{{"a", "a"}, {"b", "a"}} {
		err := tr.BuildSorted(t.Context(), func(put func(string, index.Address) error) error {
			for _, k := range keys {
				if e := put(k, addr(1)); e != nil {
					return e
				}
			}
			return nil
		})
		if err == nil || tr.Count() != 0 {
			t.Fatalf("bad input published: %v", err)
		}
	}
	if err := tr.BuildSorted(t.Context(), func(put func(string, index.Address) error) error { return put("retry", addr(1)) }); err != nil {
		t.Fatal(err)
	}
}

func TestSortedConstructionPacksInternalLevels(t *testing.T) {
	tr := openTest(t)
	const count = 160000
	if err := tr.BuildSorted(t.Context(), func(put func(string, index.Address) error) error {
		for i := range count {
			if err := put(fmt.Sprintf("%07d", i), addr(uint64(i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	root, err := tr.p.read(tr.root)
	if err != nil {
		t.Fatal(err)
	}
	child, err := tr.p.read(root.link())
	if err != nil || root[4] != branch || child[4] != branch {
		t.Fatalf("expected at least two internal levels: %v", err)
	}
	for _, i := range []int{0, 73421, count - 2, count - 1} {
		a, ok, err := tr.Lookup(fmt.Sprintf("%07d", i))
		if err != nil || !ok || a != addr(uint64(i)) {
			t.Fatalf("lookup %d: %+v, %t, %v", i, a, ok, err)
		}
	}
}
