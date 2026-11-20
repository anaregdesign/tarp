package btree

import (
	"container/list"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
)

func openTest(t *testing.T) *tree {
	t.Helper()
	backend, err := NewIn(t.TempDir(), MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	tr := backend.(*tree)
	t.Cleanup(func() {
		if err := tr.Close(); err != nil {
			t.Error(err)
		}
	})
	return tr
}
func addr(i uint64) index.Address {
	return index.Address{FileID: 7, Offset: int64(i), Bytes: 65, ValueBytes: 1, Sequence: i}
}
func verify(t *testing.T, v index.View, expected map[string]index.Address) {
	t.Helper()
	var keys []string
	err := v.All(func(key string, a index.Address) bool {
		if want, ok := expected[key]; !ok || want != a {
			t.Errorf("unexpected key %q: %v", key, a)
		}
		keys = append(keys, key)
		return true
	})
	if err != nil || len(keys) != len(expected) || !slices.IsSorted(keys) {
		t.Fatalf("All count=%d expected=%d sorted=%t error=%v", len(keys), len(expected), slices.IsSorted(keys), err)
	}
	for k, want := range expected {
		a, ok, e := v.Lookup(k)
		if e != nil || !ok || a != want {
			t.Fatalf("lookup key len=%d: got %v,%t,%v want %v", len(k), a, ok, e, want)
		}
	}
}
func TestSnapshotsSplitMergeAndReusePages(t *testing.T) {
	tr := openTest(t)
	r := rand.New(rand.NewSource(42))
	state := map[string]index.Address{}
	key := func(i int) string { return fmt.Sprintf("prefix/%05d/%s", i, strings.Repeat("r", i%180)) }
	for start := 0; start < 600; start += 10 {
		tx := tr.Begin()
		for i := start; i < start+10; i++ {
			k := key(i)
			a := addr(uint64(i + 1))
			if err := tx.Put(k, a); err != nil {
				t.Fatal(err)
			}
			state[k] = a
		}
		tx.Commit()
	}
	snapshot := tr.View()
	original := map[string]index.Address{}
	for k, v := range state {
		original[k] = v
	}
	for _, i := range r.Perm(600) {
		k := key(i)
		tx := tr.Begin()
		if err := tx.Delete(k); err != nil {
			t.Fatal(err)
		}
		if i%11 == 0 {
			tx.Abort()
			continue
		}
		tx.Commit()
		delete(state, k)
	}
	current := tr.View()
	verify(t, current, state)
	current.Close()
	verify(t, snapshot, original)
	snapshot.Close()
	for k := range state {
		tx := tr.Begin()
		if err := tx.Delete(k); err != nil {
			t.Fatal(err)
		}
		tx.Commit()
	}
	if tr.Count() != 0 {
		t.Fatalf("count %d", tr.Count())
	}
	for i := 0; i < 300; i++ {
		tx := tr.Begin()
		if err := tx.Put("hot", addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
		tx.Commit()
	}
	baseline := tr.Metrics().ScratchBytes
	for i := 0; i < 300; i++ {
		tx := tr.Begin()
		if err := tx.Put("hot", addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
		tx.Commit()
	}
	if top := tr.Metrics().ScratchBytes; top != baseline {
		t.Fatalf("overwrites grew scratch from %d to %d", baseline, top)
	}
	if tr.Metrics().CacheBytes > uint64(MinBudget) {
		t.Fatal("cache exceeded budget")
	}
}
func TestBinaryOverflowKeysAndGroupLookup(t *testing.T) {
	tr := openTest(t)
	state := map[string]index.Address{}
	keys := []string{"a", "a\x00", "a\x00\x00", "a\xff", string([]byte{0xff, 0x00})}
	base := strings.Repeat("z", pageSize*3+71)
	for i := range 160 {
		keys = append(keys, base+string([]byte{byte(i)}))
	}
	tx := tr.Begin()
	for i, k := range keys {
		a := addr(uint64(i))
		if err := tx.Put(k, a); err != nil {
			t.Fatal(err)
		}
		state[k] = a
	}
	tx.Commit()
	v := tr.View().(*view)
	defer v.Close()
	verify(t, v, state)
	input := []string{keys[0], "nothing", keys[0], keys[len(keys)-1], base, base + "xx"}
	calls := make([]int, len(input))
	err := v.LookupMany(input, func(i int, a index.Address, ok bool) {
		want, exists := state[input[i]]
		if a != want || ok != exists {
			t.Errorf("key[%d] = %v %t", i, a, ok)
		}
		calls[i]++
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range calls {
		if n != 1 {
			t.Fatalf("duplicate or missing callback: %v", calls)
		}
	}
}
func TestCorruptPageAndCloseInIteration(t *testing.T) {
	tr := openTest(t)
	tx := tr.Begin()
	if e := tx.Put("key", addr(3)); e != nil {
		t.Fatal(e)
	}
	tx.Commit()
	v := tr.View()
	defer v.Close()
	root := tr.root
	tr.p.mu.Lock()
	if err := tr.p.flush(root, tr.p.cache[root].Value.(cachedPage).p); err != nil {
		t.Fatal(err)
	}
	tr.p.cache = make(map[uint64]*list.Element)
	tr.p.branches.Init()
	tr.p.ordinary.Init()
	_, err := tr.p.file.WriteAt([]byte{0}, int64(root-1)*pageSize+4)
	tr.p.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = v.Lookup("key"); !errors.Is(err, errPage) {
		t.Fatalf("corrupt page: %v", err)
	}

	other := openTest(t)
	tx = other.Begin()
	if e := tx.Put("key", addr(8)); e != nil {
		t.Fatal(e)
	}
	tx.Commit()
	s := other.View()
	defer s.Close()
	err = s.All(func(string, index.Address) bool {
		if err := other.Close(); err != nil {
			t.Error(err)
		}
		return true
	})
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Close in cursor: %v", err)
	}
}
func TestConcurrentSnapshotAndWrites(t *testing.T) {
	tr := openTest(t)
	var readers sync.WaitGroup
	for range 3 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 130 {
				v := tr.View()
				a, ok, err := v.Lookup("a")
				b, also, err2 := v.Lookup("b")
				v.Close()
				if err != nil || err2 != nil || ok != also || a != b {
					t.Errorf("split view: %v %v, %v %v: %v %v", a, ok, b, also, err, err2)
					return
				}
			}
		}()
	}
	for i := range 120 {
		tx := tr.Begin()
		for _, k := range []string{"a", "b"} {
			if e := tx.Put(k, addr(uint64(i))); e != nil {
				t.Fatal(e)
			}
		}
		tx.Commit()
	}
	readers.Wait()
}
