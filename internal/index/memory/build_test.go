package memory_test

import (
	"errors"
	"maps"
	"os"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func TestInitialBuildPublishesOnlyAfterMeasuringLiveAddresses(t *testing.T) {
	db := memory.New()
	defer db.Close()
	build := db.(index.InitialBuilder)
	old := db.View()
	defer old.Close()
	for _, fail := range []bool{true, false} {
		b, err := build.BeginBuild()
		if err != nil {
			t.Fatal(err)
		}
		latest := map[string]index.Address{"\x00": {FileID: 3, Sequence: 4}, "a": {FileID: 2, Sequence: 1}}
		for key, addr := range latest {
			if err := b.Put(key, index.Address{Sequence: 2}); err != nil {
				t.Fatal(err)
			}
			if err := b.Put(key, addr); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Put("gone", index.Address{Sequence: 3}); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete("gone"); err != nil {
			t.Fatal(err)
		}
		if _, found, err := b.Lookup("gone"); err != nil || found {
			t.Fatalf("staged delete: found=%v, err=%v", found, err)
		}
		observed := make(map[string]index.Address)
		injected := errors.New("measurement failed")
		err = b.Finish(func(k string, a index.Address) error {
			if _, found, err := db.Lookup(k); err != nil || found {
				t.Fatalf("premature publication: %v, %v", found, err)
			}
			observed[k] = a
			if fail {
				return injected
			}
			return nil
		})
		if fail {
			if !errors.Is(err, injected) || db.Count() != 0 {
				t.Fatalf("failed build changed index: %d, %v", db.Count(), err)
			}
			continue
		}
		if err != nil || !maps.Equal(observed, latest) || db.Count() != len(latest) {
			t.Fatalf("live measurements=%v, count=%d, %v", observed, db.Count(), err)
		}
		for key, addr := range latest {
			if got, found, err := db.Lookup(key); err != nil || !found || got != addr {
				t.Fatalf("lookup %q: %+v, %v, %v", key, got, found, err)
			}
		}
	}
	if err := old.All(func(string, index.Address) bool { t.Fatal("old view changed across initial build"); return false }); err != nil {
		t.Fatal(err)
	}
	if _, err := build.BeginBuild(); err == nil {
		t.Fatal("attempted to replace nonempty runtime index")
	}
}

func TestAbortedInitialBuildReleasesWriter(t *testing.T) {
	db := memory.New()
	build := db.(index.InitialBuilder)
	b, err := build.BeginBuild()
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put("discard", index.Address{Sequence: 2}); err != nil {
		t.Fatal(err)
	}
	b.Abort()
	if db.Count() != 0 {
		t.Fatal("aborted initial image became visible")
	}
	tx := db.Begin()
	if err := tx.Put("runtime write", index.Address{Sequence: 3}); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := build.BeginBuild(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed backend: %v", err)
	}
}
