package index_test

import (
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"slices"
	"strings"
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

func TestBackendsAgainstSameRandomizedSnapshotModel(t *testing.T) {
	for _, name := range []string{"memory", "btree"} {
		t.Run(name, func(t *testing.T) {
			var backend index.Backend
			if name == "memory" {
				backend = memory.New()
			} else {
				var err error
				backend, err = btree.NewIn(t.TempDir(), btree.MinBudget)
				if err != nil {
					t.Fatal(err)
				}
			}
			defer backend.Close()
			rng := rand.New(rand.NewSource(8081))
			keys := make([]string, 201)
			for i := range keys {
				keys[i] = fmt.Sprintf("k%c%04d", byte(i), i)
				if i%47 == 0 {
					keys[i] = strings.Repeat("shared\x00", 1300) + keys[i]
				}
			}
			check := func(v index.View, model map[string]index.Address) {
				t.Helper()
				observed := map[string]index.Address{}
				if err := v.All(func(k string, a index.Address) bool {
					if _, seen := observed[k]; seen {
						t.Fatalf("duplicate key %q", k)
					}
					observed[k] = a
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if !maps.Equal(model, observed) {
					t.Fatalf("snapshot mismatch: got %d, want %d", len(observed), len(model))
				}
			}
			model := map[string]index.Address{}
			for epoch := range 80 {
				before := backend.View()
				prior := maps.Clone(model)
				tx := backend.Begin()
				for step := range 17 {
					key := keys[rng.Intn(len(keys))]
					old, found, err := tx.Lookup(key)
					want, live := model[key]
					if err != nil || found != live || old != want {
						t.Fatalf("staged lookup: %v, %v", found, err)
					}
					if live && rng.Intn(3) == 0 {
						if err := tx.Delete(key); err != nil {
							t.Fatal(err)
						}
						delete(model, key)
					} else {
						a := index.Address{FileID: uint64(1 + epoch%4), Offset: int64(step) * 4096, Bytes: 64, ValueBytes: 1, Sequence: uint64(1 + epoch*17 + step)}
						if err := tx.Put(key, a); err != nil {
							t.Fatal(err)
						}
						model[key] = a
					}
				}
				if epoch%5 == 0 {
					tx.Abort()
					model = prior
				} else {
					tx.Commit()
				}
				check(before, prior)
				before.Close()
				current := backend.View()
				check(current, model)
				current.Close()
				if backend.Count() != len(model) {
					t.Fatalf("count %d != %d", backend.Count(), len(model))
				}
			}
		})
	}
}
