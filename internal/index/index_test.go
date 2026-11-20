package index_test

import (
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func TestBackendTransactionsExposeWholeSnapshots(t *testing.T) {
	for _, test := range []struct {
		name string
		open func() (index.Backend, error)
	}{
		{"memory", func() (index.Backend, error) { return memory.New(), nil }},
		{"btree", func() (index.Backend, error) { return btree.New(btree.MinBudget) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := test.open()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			a := index.Address{FileID: 4, Offset: 64, Bytes: 56, Sequence: 1}
			b := a
			b.Sequence = 2
			b.Offset += a.Bytes
			key := "\x00k\xff"
			tx := db.Begin()
			if err := tx.Put(key, a); err != nil {
				t.Fatal(err)
			}
			if v, present, err := tx.Lookup(key); err != nil || !present || v != a {
				t.Fatalf("staged lookup %v %v %v", v, present, err)
			}
			if _, present, err := db.Lookup(key); err != nil || present {
				t.Fatalf("visible prepared change: %v %v", present, err)
			}
			tx.Commit()
			view := db.View()
			defer view.Close()
			tx = db.Begin()
			if err := tx.Delete(key); err != nil {
				t.Fatal(err)
			}
			if _, ok, err := tx.Lookup(key); err != nil || ok {
				t.Fatalf("staged delete: %v %v", ok, err)
			}
			if err := tx.Put(key, b); err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("other", b); err != nil {
				t.Fatal(err)
			}
			tx.Abort()
			if db.Count() != 1 {
				t.Fatalf("after abort: %d", db.Count())
			}
			tx = db.Begin()
			if err := tx.Put(key, b); err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("other", b); err != nil {
				t.Fatal(err)
			}
			tx.Commit()
			if db.Count() != 2 {
				t.Fatalf("after commit: %d", db.Count())
			}
			var keys []string
			if err := view.All(func(k string, addr index.Address) bool {
				if addr != a {
					t.Fatalf("old entry = %+v", addr)
				}
				keys = append(keys, k)
				return true
			}); err != nil || !slices.Equal(keys, []string{key}) {
				t.Fatalf("enumeration %q, %v", keys, err)
			}
			var found [4]bool
			count := 0
			if err := index.LookupMany(view, []string{key, "other", key, "absent"}, func(i int, addr index.Address, ok bool) {
				found[i] = ok
				count++
				if ok && addr != a {
					t.Fatalf("lookup %d = %+v", i, addr)
				}
			}); err != nil || count != 4 || found != [4]bool{true, false, true, false} {
				t.Fatalf("LookupMany %v %v", found, err)
			}
			view.Close()
			if _, _, err := view.Lookup(key); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("released view: %v", err)
			}
		})
	}
}
