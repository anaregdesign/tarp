package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/anaregdesign/tarp"
)

func ExampleDB_Snapshot() {
	dir, err := os.MkdirTemp("", "tarp-releases-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	key := []byte("release/current")
	if err := db.Put(key, []byte("v1")); err != nil {
		panic(err)
	}
	snapshot, err := db.Snapshot()
	if err != nil {
		panic(err)
	}
	defer snapshot.Close()
	if err := db.Put(key, []byte("v2")); err != nil {
		panic(err)
	}
	previous, err := snapshot.Get(key)
	if err != nil {
		panic(err)
	}
	current, err := db.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("snapshot %s, live %s\n", previous, current)
	// Output: snapshot v1, live v2
}

func TestSnapshotPointInTimeIsolationCompactionAndCacheIsolation(t *testing.T) {
	for _, mode := range []tarp.IndexMode{tarp.IndexBTree, tarp.IndexMemory} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "db")
			opts := tarp.Options{
				IndexMode:          mode,
				TargetSegmentBytes: 32 << 10,
			}
			db, err := tarp.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			closedDB := false
			defer func() {
				if !closedDB {
					_ = db.Close()
				}
			}()

			// Empty snapshot has Sequence() == 0 and zero entries.
			snapEmpty, err := db.Snapshot()
			if err != nil {
				t.Fatalf("empty Snapshot: %v", err)
			}
			if snapEmpty.Sequence() != 0 {
				t.Fatalf("empty Sequence() = %d, want 0", snapEmpty.Sequence())
			}

			// Write initial dataset and reopen cold so some keys are not in ValueCache.
			var wb tarp.WriteBatch
			for i := range 40 {
				wb.Put([]byte(fmt.Sprintf("k-%02d", i)), []byte(fmt.Sprintf("v1-%02d-%s", i, bytes.Repeat([]byte("a"), 512))))
			}
			wb.Put([]byte("empty-val"), []byte{})
			if err := db.Apply(&wb); err != nil {
				t.Fatal(err)
			}
			if err := snapEmpty.Close(); err != nil {
				t.Fatalf("snapEmpty.Close: %v", err)
			}

			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = tarp.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}

			// Warm ONLY k-00 in ValueCache before taking snap1; leave k-01..k-39 cold on disk!
			if _, err := db.Get([]byte("k-00")); err != nil {
				t.Fatal(err)
			}

			snap1, err := db.Snapshot()
			if err != nil {
				t.Fatalf("snap1: %v", err)
			}
			defer snap1.Close()

			if snap1.Sequence() != 41 {
				t.Fatalf("snap1.Sequence() = %d, want 41", snap1.Sequence())
			}

			// While snap1 is at current revision, reading cold k-01 populates ValueCache cleanly.
			v01, err := snap1.Get([]byte("k-01"))
			if err != nil || !bytes.HasPrefix(v01, []byte("v1-01-")) {
				t.Fatalf("snap1.Get(k-01) before writes = %q, %v", v01, err)
			}

			// Mutate DB heavily: overwrite k-00..k-19 (populating ValueCache with v2!),
			// delete k-20..k-39, insert k-40..k-49, and run Compact to retire old segments!
			wb.Reset()
			for i := range 20 {
				wb.Put([]byte(fmt.Sprintf("k-%02d", i)), []byte(fmt.Sprintf("v2-%02d-%s", i, bytes.Repeat([]byte("b"), 512))))
			}
			for i := 20; i < 40; i++ {
				wb.Delete([]byte(fmt.Sprintf("k-%02d", i)))
			}
			for i := 40; i < 50; i++ {
				wb.Put([]byte(fmt.Sprintf("k-%02d", i)), []byte(fmt.Sprintf("v2-%02d", i)))
			}
			if err := db.Apply(&wb); err != nil {
				t.Fatal(err)
			}

			// Run full compaction while snap1 is open: retired DATA segments must remain pinned!
			compRes, err := db.Compact(ctx, tarp.WithKeyOrder())
			if err != nil {
				t.Fatalf("Compact with open snapshot: %v", err)
			}
			if compRes.PinnedRetiredDataBytes <= 0 {
				t.Fatalf("expected PinnedRetiredDataBytes > 0 while snap1 holds old segments, got %+v", compRes)
			}

			// Verify snap1 STILL observes exact v1 values (no ValueCache v2 leakage!) and deleted keys!
			for i := range 40 {
				k := []byte(fmt.Sprintf("k-%02d", i))
				got, err := snap1.Get(k)
				wantPrefix := fmt.Sprintf("v1-%02d-", i)
				if err != nil || !bytes.HasPrefix(got, []byte(wantPrefix)) {
					t.Fatalf("snap1.Get(%q) = %q, %v; want prefix %q", k, got, err, wantPrefix)
				}
			}
			if got, err := snap1.Get([]byte("empty-val")); err != nil || len(got) != 0 {
				t.Fatalf("snap1.Get(empty-val) = %q, %v; want empty", got, err)
			}
			if _, err := snap1.Get([]byte("k-40")); !errors.Is(err, tarp.ErrNotFound) {
				t.Fatalf("snap1.Get(future k-40) = %v, want ErrNotFound", err)
			}

			// Verify reading old v1 values through snap1 did NOT pollute live DB ValueCache!
			liveHitsBefore := db.Stats().ValueCache.Hits
			live00, err := db.Get([]byte("k-00"))
			if err != nil || !bytes.HasPrefix(live00, []byte("v2-00-")) {
				t.Fatalf("live db.Get(k-00) = %q, %v; want v2-00-*", live00, err)
			}
			if db.Stats().ValueCache.Hits != liveHitsBefore+1 {
				t.Fatalf("expected live k-00 to remain cached after snap1 read")
			}

			// Verify snap1.BatchGet (including duplicate keys, deleted keys, empty value, absent keys).
			batchKeys := [][]byte{
				[]byte("k-00"),
				[]byte("k-25"),
				[]byte("k-00"),
				[]byte("empty-val"),
				[]byte("k-45"),
			}
			batchRes, err := snap1.BatchGet(batchKeys)
			if err != nil {
				t.Fatalf("snap1.BatchGet: %v", err)
			}
			if !batchRes[0].Found || !bytes.HasPrefix(batchRes[0].Value, []byte("v1-00-")) ||
				!batchRes[1].Found || !bytes.HasPrefix(batchRes[1].Value, []byte("v1-25-")) ||
				!batchRes[2].Found || !bytes.Equal(batchRes[0].Value, batchRes[2].Value) ||
				!batchRes[3].Found || len(batchRes[3].Value) != 0 ||
				batchRes[4].Found {
				t.Fatalf("unexpected snap1.BatchGet results: %+v", batchRes)
			}
			// Mutate duplicate slice to verify independent ownership.
			batchRes[0].Value[0] = 'Z'
			if batchRes[2].Value[0] == 'Z' {
				t.Fatalf("duplicate BatchGet slots aliased backing slice")
			}

			// Verify snap1.ScanRange and KeysRange (ascending & reverse).
			var ascKeys, descKeys []string
			for e, err := range snap1.ScanRange(ctx, []byte("k-18"), []byte("k-23")) {
				if err != nil {
					t.Fatalf("snap1.ScanRange asc: %v", err)
				}
				ascKeys = append(ascKeys, string(e.Key))
			}
			for k, err := range snap1.KeysRange(ctx, []byte("k-18"), []byte("k-23"), tarp.WithReverse()) {
				if err != nil {
					t.Fatalf("snap1.KeysRange desc: %v", err)
				}
				descKeys = append(descKeys, string(k))
			}
			wantAsc := []string{"k-18", "k-19", "k-20", "k-21", "k-22"}
			wantDesc := []string{"k-22", "k-21", "k-20", "k-19", "k-18"}
			if fmt.Sprint(ascKeys) != fmt.Sprint(wantAsc) {
				t.Fatalf("snap1.ScanRange asc = %v, want %v", ascKeys, wantAsc)
			}
			if fmt.Sprint(descKeys) != fmt.Sprint(wantDesc) {
				t.Fatalf("snap1.KeysRange desc = %v, want %v", descKeys, wantDesc)
			}

			// Close snap1 (idempotent) -> unpins retired segments; subsequent Compact cleans them up!
			if err := snap1.Close(); err != nil {
				t.Fatalf("snap1.Close: %v", err)
			}
			if err := snap1.Close(); err != nil {
				t.Fatalf("second snap1.Close: %v", err)
			}
			if _, err := snap1.Get([]byte("k-00")); !errors.Is(err, tarp.ErrClosed) {
				t.Fatalf("Get after snap1.Close = %v, want ErrClosed", err)
			}
			compAfter, err := db.Compact(ctx)
			if err != nil {
				t.Fatalf("Compact after snap1.Close: %v", err)
			}
			if compAfter.PinnedRetiredDataBytes != 0 {
				t.Fatalf("PinnedRetiredDataBytes after snap1.Close = %d, want 0", compAfter.PinnedRetiredDataBytes)
			}
		})
	}
}

func TestSnapshotConcurrencyAndLifecycleSafety(t *testing.T) {
	for _, mode := range []tarp.IndexMode{tarp.IndexBTree, tarp.IndexMemory} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			ctx := context.Background()
			db, err := tarp.Open(t.TempDir(), tarp.Options{IndexMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = db.Close()
				}
			}()

			for i := range 30 {
				if err := db.Put([]byte(fmt.Sprintf("key-%02d", i)), []byte(fmt.Sprintf("val-%02d", i))); err != nil {
					t.Fatal(err)
				}
			}

			// 1. Concurrent goroutines sharing one Snapshot while writers & Compact mutate DB.
			sharedSnap, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for g := range 4 {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for iter := range 20 {
						k := []byte(fmt.Sprintf("key-%02d", (id*7+iter)%30))
						want := fmt.Sprintf("val-%02d", (id*7+iter)%30)
						got, err := sharedSnap.Get(k)
						if err != nil || string(got) != want {
							t.Errorf("sharedSnap.Get(%q) = %q, %v; want %q", k, got, err, want)
							return
						}
						res, err := sharedSnap.BatchGet([][]byte{k, []byte("absent")})
						if err != nil || !res[0].Found || string(res[0].Value) != want || res[1].Found {
							t.Errorf("sharedSnap.BatchGet = %+v, %v", res, err)
							return
						}
						count := 0
						for _, err := range sharedSnap.ScanRange(ctx, []byte("key-05"), []byte("key-15"), tarp.WithReverse()) {
							if err != nil {
								t.Errorf("sharedSnap.ScanRange: %v", err)
								return
							}
							count++
						}
						if count != 10 {
							t.Errorf("sharedSnap.ScanRange count = %d, want 10", count)
							return
						}
					}
				}(g)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range 30 {
					var b tarp.WriteBatch
					b.Put([]byte(fmt.Sprintf("key-%02d", i)), []byte("overwritten"))
					b.Delete([]byte(fmt.Sprintf("key-%02d", (i+1)%30)))
					if err := db.Apply(&b); err != nil {
						t.Errorf("concurrent Apply: %v", err)
						return
					}
				}
				if _, err := db.Compact(ctx); err != nil {
					t.Errorf("concurrent Compact: %v", err)
				}
			}()
			wg.Wait()
			if err := sharedSnap.Close(); err != nil {
				t.Fatal(err)
			}

			// 2. Closing Snapshot INSIDE a Scan loop body must never deadlock!
			loopSnap, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			seen := 0
			for _, err := range loopSnap.Scan(ctx) {
				if err != nil {
					if errors.Is(err, tarp.ErrClosed) {
						break
					}
					t.Fatalf("loopSnap.Scan: %v", err)
				}
				seen++
				if err := loopSnap.Close(); err != nil {
					t.Fatalf("loopSnap.Close inside loop: %v", err)
				}
			}
			if seen != 1 {
				t.Fatalf("seen = %d before ErrClosed stop, want 1", seen)
			}

			// 2b. Closing Snapshot on the FINAL entry of a 1-key Scan/Keys must still yield ErrClosed.
			singleDB, err := tarp.Open(t.TempDir(), tarp.Options{IndexMode: mode})
			if err != nil {
				t.Fatal(err)
			}
			if err := singleDB.Put([]byte("only"), []byte("one")); err != nil {
				_ = singleDB.Close()
				t.Fatal(err)
			}
			for _, kind := range []string{"scan", "keys"} {
				oneSnap, err := singleDB.Snapshot()
				if err != nil {
					_ = singleDB.Close()
					t.Fatal(err)
				}
				var closeErr error
				var entries int
				if kind == "scan" {
					for _, err := range oneSnap.Scan(ctx) {
						if err != nil {
							closeErr = err
							break
						}
						entries++
						_ = oneSnap.Close()
					}
				} else {
					for _, err := range oneSnap.Keys(ctx) {
						if err != nil {
							closeErr = err
							break
						}
						entries++
						_ = oneSnap.Close()
					}
				}
				if entries != 1 || !errors.Is(closeErr, tarp.ErrClosed) {
					_ = singleDB.Close()
					t.Fatalf("1-key %s close on final entry: entries=%d err=%v, want 1 and ErrClosed", kind, entries, closeErr)
				}
			}
			if err := singleDB.Close(); err != nil {
				t.Fatal(err)
			}

			// 3. Closing DB while an idle Snapshot remains open must NOT deadlock,
			// subsequent Snapshot reads must return ErrClosed, and Snapshot.Close after DB.Close succeeds.
			lingeringSnap, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("db.Close with lingering idle Snapshot: %v", err)
			}
			closed = true

			if _, err := lingeringSnap.Get([]byte("key-00")); !errors.Is(err, tarp.ErrClosed) {
				t.Fatalf("lingeringSnap.Get after db.Close = %v, want ErrClosed", err)
			}
			for _, err := range lingeringSnap.Keys(ctx) {
				if !errors.Is(err, tarp.ErrClosed) {
					t.Fatalf("lingeringSnap.Keys after db.Close = %v, want ErrClosed", err)
				}
			}
			if err := lingeringSnap.Close(); err != nil {
				t.Fatalf("lingeringSnap.Close after db.Close: %v", err)
			}
			if err := lingeringSnap.Close(); err != nil {
				t.Fatalf("second lingeringSnap.Close after db.Close: %v", err)
			}
		})
	}
}

func TestSnapshotBatchGetLimits(t *testing.T) {
	db, err := tarp.Open(t.TempDir(), tarp.Options{MaxBatchGetValueBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("a"), bytes.Repeat([]byte("x"), 80)); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("b"), bytes.Repeat([]byte("y"), 80)); err != nil {
		t.Fatal(err)
	}

	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	// Exceeding MaxBatchGetEntries.
	tooMany := make([][]byte, tarp.MaxBatchGetEntries+1)
	for i := range tooMany {
		tooMany[i] = []byte("a")
	}
	var limitErr *tarp.BatchLimitError
	if _, err := snap.BatchGet(tooMany); !errors.As(err, &limitErr) || limitErr.Kind != tarp.BatchInputCountLimit {
		t.Fatalf("snap.BatchGet(MaxBatchGetEntries+1) = %v, want BatchInputCountLimit", err)
	}

	// Exceeding MaxBatchGetValueBytes both at same revision and after revision advance.
	if _, err := snap.BatchGet([][]byte{[]byte("a"), []byte("b")}); !errors.As(err, &limitErr) || limitErr.Kind != tarp.BatchGetValueBytesLimit {
		t.Fatalf("snap.BatchGet(same revision limit) = %v, want BatchGetValueBytesLimit", err)
	}
	if err := db.Put([]byte("c"), []byte("z")); err != nil {
		t.Fatal(err)
	}
	if _, err := snap.BatchGet([][]byte{[]byte("a"), []byte("b")}); !errors.As(err, &limitErr) || limitErr.Kind != tarp.BatchGetValueBytesLimit {
		t.Fatalf("snap.BatchGet(older revision limit) = %v, want BatchGetValueBytesLimit", err)
	}
}
