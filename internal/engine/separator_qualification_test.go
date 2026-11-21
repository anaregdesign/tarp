//go:build tarpcorner145

package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"
	"testing"
)

type separatorPageLayout struct {
	branches map[string][]uint64
	values   map[string][]uint64
	leaves   map[uint64]bool
}

func observeSeparatorPages(t *testing.T, db *DB) separatorPageLayout {
	t.Helper()
	layout := separatorPageLayout{make(map[string][]uint64), make(map[string][]uint64), make(map[uint64]bool)}
	probe, ok := db.keys.(interface {
		VisitQualificationKeys(func(bool, uint64, string, []uint64)) error
	})
	if !ok {
		t.Fatal("default backend does not expose the B+tree page probe")
	}
	err := probe.VisitQualificationKeys(func(branch bool, id uint64, key string, chain []uint64) {
		if branch {
			layout.branches[key] = chain
		} else {
			layout.values[key] = chain
			layout.leaves[id] = true
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return layout
}

func TestLongSeparatorSurvivesDeletedLeafOverflowReuse(t *testing.T) {
	dir := t.TempDir()
	opts := Options{}
	db, err := OpenWithHooks(dir, opts, nil, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	if db.opts.IndexMode != IndexBTree || db.opts.BTreeMemoryBudgetBytes != 16<<20 {
		t.Fatal(db.opts)
	}

	want := make(map[string]string)
	var seq uint64
	prefix := bytes.Repeat([]byte{0x00, 0xff, 0x80}, 3<<10)
	keyFor := func(id uint64) string {
		return string(binary.BigEndian.AppendUint64(slices.Clone(prefix), id))
	}
	put := func(keys []string, round string) {
		t.Helper()
		for len(keys) > 0 {
			n := min(32, len(keys))
			entries := make([]Entry, n)
			for i, k := range keys[:n] {
				v := fmt.Sprintf("%s/%d/%d", round, seq, i)
				entries[i] = Entry{Key: []byte(k), Value: []byte(v)}
			}
			if err := db.BatchPut(entries); err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				want[string(e.Key)] = string(e.Value)
			}
			seq += uint64(n)
			keys = keys[n:]
		}
	}
	check := func(stage string, absent ...string) {
		t.Helper()
		Observe(db).ClearCache()
		for k, v := range want {
			got, err := db.Get([]byte(k))
			if err != nil || string(got) != v {
				t.Fatalf("%s Get key suffix %x: %q, %v; want %q", stage, []byte(k)[len(k)-8:], got, err, v)
			}
		}
		for _, k := range absent {
			if _, err := db.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
				t.Fatalf("%s expected missing key, got %v", stage, err)
			}
		}
		actual := make(map[string]string, len(want))
		for entry, err := range db.Scan(t.Context()) {
			if err != nil {
				t.Fatal(err)
			}
			if _, duplicate := actual[string(entry.Key)]; duplicate {
				t.Fatal("duplicate in Scan")
			}
			actual[string(entry.Key)] = string(entry.Value)
		}
		if !maps.Equal(actual, want) || Observe(db).Frontier().Sequence != seq {
			t.Fatalf("%s state or sequence mismatch: %d/%d entries, sequence %d/%d", stage, len(actual), len(want), Observe(db).Frontier().Sequence, seq)
		}
		t.Logf("%s: all %d live values and sequence %d matched", stage, len(want), seq)
	}
	var seed, infill []string
	// Fill intermediate leaf pages above half occupancy so dropping just the
	// boundary value leaves its separator live in the parent.
	for id := range 512 {
		seed = append(seed, keyFor(uint64(2*id)))
	}
	for id := range 256 {
		infill = append(infill, keyFor(uint64(4*id+1)))
	}
	for _, length := range []int{239, 240, 241, 8183, 8184, 8185} {
		seed = append(seed, string(prefix[:length]))
	}
	put(seed, "seed")
	put(infill, "infill")
	old := observeSeparatorPages(t, db)
	target := ""
	for _, k := range slices.Sorted(maps.Keys(old.branches)) {
		if _, ok := want[k]; ok && len(k) == len(prefix)+8 && len(old.values[k]) == 2 && len(old.branches[k]) == 2 {
			target = k
			break
		}
	}
	if target == "" {
		t.Fatal("no exact branch separator with a distinct two-page leaf key")
	}
	t.Logf("exact separator key: %d bytes, suffix %x; branch overflow %v, leaf overflow %v; initial leaves %d", len(target), []byte(target)[len(prefix):], old.branches[target], old.values[target], len(old.leaves))

	if err := db.Delete([]byte(target)); err != nil {
		t.Fatal(err)
	}
	delete(want, target)
	seq++
	deleted := observeSeparatorPages(t, db)
	if !slices.Equal(deleted.branches[target], old.branches[target]) {
		t.Fatal("the original branch separator must remain after deletion")
	}
	if _, ok := deleted.values[target]; ok {
		t.Fatal("deleted key still present in leaf")
	}
	missing := keyFor(1 << 32)
	check("after delete", target, missing)
	var growth []string
	for id := range 512 {
		growth = append(growth, keyFor(uint64(1024+id)))
	}
	put(growth, "growth")
	expanded := observeSeparatorPages(t, db)
	if len(expanded.leaves) <= len(old.leaves) || !slices.Equal(expanded.branches[target], old.branches[target]) {
		t.Fatal("expected further leaf splits and retained original separator")
	}
	// Prove the original leaf key's freed overflow storage has other owners.
	reused := make(map[uint64]bool)
	for newKey, pages := range expanded.values {
		for _, id := range pages {
			if slices.Contains(old.values[target], id) {
				reused[id] = true
				t.Logf("old leaf overflow page %d now owned by key suffix %x", id, []byte(newKey)[len(newKey)-8:])
			}
		}
	}
	if len(reused) == 0 {
		t.Fatal("did not observe a deleted overflow page reallocated to a different live leaf key")
	}
	t.Logf("after further splits: leaves %d -> %d, original leaf overflow pages observed reused %d/%d", len(old.leaves), len(expanded.leaves), len(reused), len(old.values[target]))
	check("after page reuse", target, missing)
	put([]string{target}, "reinsert")
	check("after reinsert", missing)
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	check("after Compact", missing)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = OpenContext(t.Context(), dir, Options{}, true)
	if err != nil {
		t.Fatal(err)
	}
	check("after default reopen", missing)
}
