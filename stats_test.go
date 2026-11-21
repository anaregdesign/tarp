package tarp_test

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/anaregdesign/tarp"
)

func ExampleDB_Stats() {
	dir, err := os.MkdirTemp("", "tarp-stats-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := db.BatchPut([]tarp.TypedEntry[string, string]{
		{Key: "state", Value: "done"}, {Key: "result", Value: "report.pdf"},
	}); err != nil {
		panic(err)
	}
	s := db.Stats()
	fmt.Println(s.LiveKeys, s.WriteCommits, s.WrittenRecords)
	// Output: 2 1 2
}

func TestStatsSnapshotDoesNotAllocate(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	if allocs := testing.AllocsPerRun(100, func() { _ = db.Stats() }); allocs != 0 {
		t.Fatalf("Stats allocated %v times per snapshot", allocs)
	}
}

func TestOwnerStatsIncludeViewOperationsAndRetainFinalSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	opts := tarp.Options{ValueCacheBudgetBytes: 130} // one single-byte key and value
	raw, err := tarp.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db, err := tarp.Bind(raw, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.BatchPut([]tarp.TypedEntry[string, string]{
		{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "a", Value: "3"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.BatchGet([]string{"a", "b", "b", "absent"}); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchDelete([]string{"b", "b"}); err != nil {
		t.Fatal(err)
	}
	want := tarp.Stats{
		LiveKeys: 1, LiveRecordBytes: 50, SelectedSegments: 1, SelectedDataBytes: 263,
		ObsoletePutBytes: 100, TombstoneBytes: 49, MissingKeyLookups: 1,
		BatchGetDuplicateInputs: 1, WriteCommits: 2, WrittenRecords: 4,
		ValueCache: tarp.ValueCacheStats{UsedBytes: 130, LimitBytes: 130, Hits: 1, Misses: 1, Evictions: 2},
	}
	if s := raw.Stats(); s != want {
		t.Fatalf("raw stats after view operations = %+v, want %+v", s, want)
	}
	// The default B+tree seals the nonempty log for its Close checkpoint.
	afterClose := want
	afterClose.SelectedSegments++
	afterClose.SelectedDataBytes += 64
	// Scrape while Close releases the DB, and once more after shutdown.
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Go(func() {
		close(start)
		for range 1000 {
			if s := raw.Stats(); s != want && s != afterClose {
				t.Errorf("snapshot during Close = %+v", s)
				return
			}
		}
	})
	<-start
	err = raw.Close()
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if s := raw.Stats(); s != afterClose {
		t.Fatalf("final snapshot = %+v, want %+v", s, afterClose)
	}
	next, err := tarp.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	want = afterClose
	want.ValueCache = tarp.ValueCacheStats{LimitBytes: 130}
	want.BatchGetDuplicateInputs = 0
	want.MissingKeyLookups, want.WriteCommits, want.WrittenRecords = 0, 0, 0
	if s := next.Stats(); s != want {
		t.Fatalf("reopened: %+v, want %+v", s, want)
	}
}
