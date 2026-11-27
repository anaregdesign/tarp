package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp"
)

func ExampleDB_Apply() {
	dir, err := os.MkdirTemp("", "tarp-queue-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	queued, active := []byte("queued/42"), []byte("active/42")
	if err := db.Put(queued, []byte("ready")); err != nil {
		panic(err)
	}
	var batch tarp.WriteBatch
	batch.Delete(queued)
	batch.Put(active, []byte("running"))
	if err := db.Apply(&batch); err != nil {
		panic(err)
	}
	results, err := db.BatchGet([][]byte{queued, active})
	if err != nil {
		panic(err)
	}
	fmt.Printf("queued %t, active %s\n", results[0].Found, results[1].Value)
	// Output: queued false, active running
}

func TestWriteBatchSizerReservesTrailingRecord(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{MaxRecordBytes: 256, TargetSegmentBytes: 400})
	plan := db.WriteBatchSizer()
	if err := plan.Add(1, 119); err != nil {
		t.Fatal(err)
	}
	var batch tarp.WriteBatch
	batch.Put([]byte("a"), make([]byte, 119))

	trail := plan
	var limit *tarp.BatchLimitError
	if err := trail.Add(1, 120); !errors.As(err, &limit) || limit.Kind != tarp.BatchRecordBytesLimit ||
		limit.Index != 1 || limit.Actual != 337 || limit.Limit != 336 {
		t.Fatalf("plan one byte over capacity: %v", err)
	}
	if err := trail.Add(1, 119); err != nil {
		t.Fatalf("plan exactly at capacity: %v", err)
	}
	if err := plan.Add(1, 119); err != nil {
		t.Fatalf("preview changed original plan: %v", err)
	}
	batch.Put([]byte("b"), make([]byte, 119))
	if err := db.Apply(&batch); err != nil {
		t.Fatalf("Apply planned batch: %v", err)
	}
}

func TestWriteBatchSizerUsesEffectiveLimits(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{MaxRecordBytes: 128, TargetSegmentBytes: 1})
	plan := db.WriteBatchSizer()
	var limit *tarp.BatchLimitError
	if err := plan.Add(1, 80); !errors.As(err, &limit) || limit.Kind != tarp.BatchEntrySizeLimit || limit.Index != 0 {
		t.Fatalf("oversized planned entry: %v", err)
	}
	if err := plan.Add(1, math.MaxUint64); !errors.As(err, &limit) || limit.Kind != tarp.BatchEntrySizeLimit ||
		limit.Index != 0 || !limit.Entry.Overflow {
		t.Fatalf("overflowing planned entry: %v", err)
	}
	if err := plan.Add(0, 0); !errors.Is(err, tarp.ErrEmptyKey) {
		t.Fatalf("empty planned key: %v", err)
	}
	if err := plan.Add(1, 79); err != nil {
		t.Fatalf("normalized segment target: %v", err)
	}
	if err := plan.Add(1, 0); !errors.As(err, &limit) || limit.Kind != tarp.BatchRecordBytesLimit ||
		limit.Index != 1 || limit.Actual != 177 || limit.Limit != 128 {
		t.Fatalf("normalized segment capacity check: %v", err)
	}

	smallCache := openPublicDB(t, t.TempDir(), tarp.Options{ValueCacheBudgetBytes: 140, MaxRecordBytes: 128})
	plan = smallCache.WriteBatchSizer()
	if err := plan.Add(1, 12); !errors.As(err, &limit) || limit.Kind != tarp.BatchEntrySizeLimit || limit.Entry.ValueCacheChargeBytes != 141 {
		t.Fatalf("value cache limit: %v", err)
	}
	if allocs := testing.AllocsPerRun(10, func() {
		s := smallCache.WriteBatchSizer()
		if err := s.Add(1, 11); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("successful plan allocated %g objects", allocs)
	}

	defaultDB := openPublicDB(t, t.TempDir(), tarp.Options{})
	plan = defaultDB.WriteBatchSizer()
	for range tarp.MaxWriteBatchEntries {
		if err := plan.Add(1, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := plan.Add(1, 0); !errors.As(err, &limit) || limit.Kind != tarp.BatchInputCountLimit ||
		limit.Index != -1 || limit.Actual != tarp.MaxWriteBatchEntries+1 || limit.Limit != tarp.MaxWriteBatchEntries {
		t.Fatalf("planned input count limit: %v", err)
	}
}

func TestWriteBatchSizerChargesDuplicatesAndAbsentDeletes(t *testing.T) {
	for _, scenario := range []string{"duplicate", "absent delete", "live delete"} {
		t.Run(scenario, func(t *testing.T) {
			db := openPublicDB(t, t.TempDir(), tarp.Options{MaxRecordBytes: 256, TargetSegmentBytes: 400})
			plan := db.WriteBatchSizer()
			var batch tarp.WriteBatch
			put := func(k string, n int) error {
				batch.Put([]byte(k), make([]byte, n))
				return plan.Add(uint64(len(k)), uint64(n))
			}
			if scenario == "live delete" {
				if err := db.Put([]byte("x"), []byte("survives")); err != nil {
					t.Fatal(err)
				}
			}
			var plannedErr error
			if scenario == "duplicate" {
				if err := errors.Join(put("a", 180), put("a", 119)); err == nil {
					t.Fatal("planning did not charge both versions of a")
				} else {
					plannedErr = err
				}
				batch.Put([]byte("b"), make([]byte, 119))
			} else {
				if err := errors.Join(put("a", 119), put("b", 119)); err != nil {
					t.Fatal(err)
				}
				batch.Delete([]byte("x"))
				plannedErr = plan.Add(1, 0)
			}
			var limit *tarp.BatchLimitError
			if !errors.As(plannedErr, &limit) || limit.Kind != tarp.BatchRecordBytesLimit {
				t.Fatalf("conservative planning result: %v", plannedErr)
			}
			if err := db.Apply(&batch); scenario == "live delete" {
				if !errors.As(err, &limit) || limit.Kind != tarp.BatchRecordBytesLimit ||
					limit.Index != 2 || limit.Actual != 385 || limit.Limit != 336 {
					t.Fatalf("Apply with live Delete: %v", err)
				}
				if got, err := db.Get([]byte("x")); string(got) != "survives" || err != nil {
					t.Fatalf("rejected batch changed live key: %q, %v", got, err)
				}
			} else if err != nil {
				t.Fatalf("Apply conservative overflow: %v", err)
			}
		})
	}
}

func TestWriteBatchMixedCoalescingAndScratchSafety(t *testing.T) {
	for _, mode := range []tarp.IndexMode{tarp.IndexBTree, tarp.IndexMemory} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			db := openPublicDB(t, t.TempDir(), tarp.Options{IndexMode: mode})

			// Nil and empty WriteBatch perform admission/health checks without committing.
			before := db.Stats()
			if err := db.Apply(nil); err != nil {
				t.Fatalf("Apply(nil): %v", err)
			}
			var batch tarp.WriteBatch
			if batch.Len() != 0 {
				t.Fatalf("zero WriteBatch Len() = %d, want 0", batch.Len())
			}
			if err := db.Apply(&batch); err != nil {
				t.Fatalf("Apply(empty): %v", err)
			}
			if after := db.Stats(); after.WriteCommits != before.WriteCommits || after.WrittenRecords != before.WrittenRecords {
				t.Fatalf("empty Apply changed commit stats: before=%+v after=%+v", before, after)
			}

			// Seed initial live keys: k_live_put_put, k_live_del_put, k_live_put_del, k_live_del_del, k_live_del.
			seed := []tarp.Entry{
				{Key: []byte("k_live_put_put"), Value: []byte("v0")},
				{Key: []byte("k_live_del_put"), Value: []byte("v0")},
				{Key: []byte("k_live_put_del"), Value: []byte("v0")},
				{Key: []byte("k_live_del_del"), Value: []byte("v0")},
				{Key: []byte("k_live_del"), Value: []byte("v0")},
			}
			if err := db.BatchPut(seed); err != nil {
				t.Fatal(err)
			}

			// Reuse caller scratch slices across every Put and Delete call.
			keyScratch := make([]byte, 0, 64)
			valScratch := make([]byte, 0, 64)
			putScratch := func(k, v string) {
				keyScratch = append(keyScratch[:0], k...)
				valScratch = append(valScratch[:0], v...)
				batch.Put(keyScratch, valScratch)
				for i := range keyScratch {
					keyScratch[i] = 'X'
				}
				for i := range valScratch {
					valScratch[i] = 'Y'
				}
			}
			delScratch := func(k string) {
				keyScratch = append(keyScratch[:0], k...)
				batch.Delete(keyScratch)
				for i := range keyScratch {
					keyScratch[i] = 'Z'
				}
			}

			// 1. Put -> Put on live and absent keys.
			putScratch("k_live_put_put", "v1")
			putScratch("k_live_put_put", "v2")
			putScratch("k_abs_put_put", "a1")
			putScratch("k_abs_put_put", "a2")

			// 2. Delete -> Put on live and absent keys.
			delScratch("k_live_del_put")
			putScratch("k_live_del_put", "reborn_live")
			delScratch("k_abs_del_put")
			putScratch("k_abs_del_put", "reborn_abs")

			// 3. Put -> Delete on live and absent keys.
			putScratch("k_live_put_del", "temp")
			delScratch("k_live_put_del")
			putScratch("k_abs_put_del", "temp")
			delScratch("k_abs_put_del")

			// 4. Delete -> Delete on live and absent keys + single absent delete.
			delScratch("k_live_del_del")
			delScratch("k_live_del_del")
			delScratch("k_abs_del_del")
			delScratch("k_abs_del_del")
			delScratch("k_live_del")
			delScratch("k_abs_single_del")

			if batch.Len() != 18 {
				t.Fatalf("Len() = %d, want 18", batch.Len())
			}

			commitsBefore := db.Stats().WriteCommits
			if err := db.Apply(&batch); err != nil {
				t.Fatalf("Apply(mixed): %v", err)
			}
			if got := db.Stats().WriteCommits; got != commitsBefore+1 {
				t.Fatalf("WriteCommits = %d, want %d", got, commitsBefore+1)
			}

			// Reset retains capacity and clears Len.
			batch.Reset()
			if batch.Len() != 0 {
				t.Fatalf("Len() after Reset = %d, want 0", batch.Len())
			}

			wantPresent := map[string]string{
				"k_live_put_put": "v2",
				"k_abs_put_put":  "a2",
				"k_live_del_put": "reborn_live",
				"k_abs_del_put":  "reborn_abs",
			}
			for k, want := range wantPresent {
				got, err := db.Get([]byte(k))
				if err != nil || string(got) != want {
					t.Fatalf("Get(%q) = %q, %v; want %q", k, got, err, want)
				}
			}
			wantAbsent := []string{
				"k_live_put_del",
				"k_abs_put_del",
				"k_live_del_del",
				"k_abs_del_del",
				"k_live_del",
				"k_abs_single_del",
			}
			for _, k := range wantAbsent {
				if _, err := db.Get([]byte(k)); !errors.Is(err, tarp.ErrNotFound) {
					t.Fatalf("Get(%q) err = %v, want ErrNotFound", k, err)
				}
			}

			// DB.Delete and DB.BatchDelete MUST retain their ErrNotFound semantics!
			if err := db.Delete([]byte("k_abs_single_del")); !errors.Is(err, tarp.ErrNotFound) {
				t.Fatalf("DB.Delete(absent) = %v, want ErrNotFound", err)
			}
			requireBatchKeyError(t, db.BatchDelete([][]byte{[]byte("k_abs_single_del")}), "BatchDelete", 0, tarp.ErrNotFound)
		})
	}
}

func TestWriteBatchElidedDeletesPreserveRecoveryCompactionAndBackup(t *testing.T) {
	for _, mode := range []tarp.IndexMode{tarp.IndexBTree, tarp.IndexMemory} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			root := t.TempDir()
			dbDir := filepath.Join(root, "db")
			opts := tarp.Options{IndexMode: mode}
			db, err := tarp.Open(dbDir, opts)
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = db.Close()
				}
			}()

			// Batch containing ONLY absent-key deletes and Put->Delete of absent keys:
			// 0 physical records survive, so no orphan tombstone is written and WriteCommits stays 0.
			var b tarp.WriteBatch
			b.Delete([]byte("never-existed-1"))
			b.Delete([]byte("never-existed-2"))
			b.Put([]byte("ephemeral"), []byte("val"))
			b.Delete([]byte("ephemeral"))
			if err := db.Apply(&b); err != nil {
				t.Fatalf("Apply(all-elided): %v", err)
			}
			if st := db.Stats(); st.WriteCommits != 0 || st.WrittenRecords != 0 || st.LiveKeys != 0 {
				t.Fatalf("all-elided batch mutated stats: %+v", st)
			}

			// Now execute multiple mixed batches across >40 distinct keys (testing slot growth),
			// mixing live Puts, live Deletes, and absent Deletes.
			b.Reset()
			for i := range 50 {
				b.Put([]byte(fmt.Sprintf("k-%03d", i)), []byte(fmt.Sprintf("v0-%03d", i)))
				b.Delete([]byte(fmt.Sprintf("ghost-%03d", i)))
			}
			if err := db.Apply(&b); err != nil {
				t.Fatalf("Apply(seed+ghosts): %v", err)
			}

			b.Reset()
			for i := range 50 {
				if i%2 == 0 {
					b.Delete([]byte(fmt.Sprintf("k-%03d", i)))
				} else {
					b.Put([]byte(fmt.Sprintf("k-%03d", i)), []byte(fmt.Sprintf("v1-%03d", i)))
				}
				b.Delete([]byte(fmt.Sprintf("missing-again-%03d", i)))
				b.Put([]byte(fmt.Sprintf("transient-%03d", i)), []byte("temp"))
				b.Delete([]byte(fmt.Sprintf("transient-%03d", i)))
			}
			if err := db.Apply(&b); err != nil {
				t.Fatalf("Apply(mixed-update): %v", err)
			}

			ctx := context.Background()
			if mode == tarp.IndexBTree {
				if err := db.CheckpointIndex(ctx); err != nil {
					t.Fatalf("CheckpointIndex: %v", err)
				}
			}

			// Verify Backup + VerifyBackup + Restore right after mixed batches with absent deletes.
			backupDir := filepath.Join(root, "backup")
			if err := db.Backup(ctx, backupDir); err != nil {
				t.Fatalf("Backup: %v", err)
			}
			info, err := tarp.VerifyBackup(ctx, backupDir)
			if err != nil {
				t.Fatalf("VerifyBackup: %v", err)
			}
			if info.LiveKeys != 25 {
				t.Fatalf("BackupInfo.LiveKeys = %d, want 25", info.LiveKeys)
			}
			restoredDir := filepath.Join(root, "restored")
			if err := tarp.Restore(ctx, tarp.RestoreRequest{Source: backupDir, Destination: restoredDir}); err != nil {
				t.Fatalf("Restore: %v", err)
			}

			// Close and reopen in both Indexed and Full validation modes.
			if err := db.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			closed = true

			for _, validation := range []tarp.OpenValidation{tarp.Indexed, tarp.Full} {
				reopened, err := tarp.Open(dbDir, tarp.Options{IndexMode: mode, OpenValidation: validation})
				if err != nil {
					t.Fatalf("reopen(validation=%v): %v", validation, err)
				}
				for i := range 50 {
					k := []byte(fmt.Sprintf("k-%03d", i))
					got, err := reopened.Get(k)
					if i%2 == 0 {
						if !errors.Is(err, tarp.ErrNotFound) {
							_ = reopened.Close()
							t.Fatalf("reopen Get(%q) = %q, %v; want ErrNotFound", k, got, err)
						}
					} else {
						want := fmt.Sprintf("v1-%03d", i)
						if err != nil || string(got) != want {
							_ = reopened.Close()
							t.Fatalf("reopen Get(%q) = %q, %v; want %q", k, got, err, want)
						}
					}
				}
				if _, err := reopened.Compact(ctx, tarp.WithKeyOrder()); err != nil {
					_ = reopened.Close()
					t.Fatalf("Compact: %v", err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatalf("reopened Close: %v", err)
				}
			}

			// Also verify the restored DB opens cleanly under Full verification.
			restoredDB := openPublicDB(t, restoredDir, tarp.Options{IndexMode: mode, OpenValidation: tarp.Full})
			if st := restoredDB.Stats(); st.LiveKeys != 25 {
				t.Fatalf("restored LiveKeys = %d, want 25", st.LiveKeys)
			}
		})
	}
}

func TestWriteBatchRecordLimitChargesOnlySurvivingRecords(t *testing.T) {
	for _, mode := range []tarp.IndexMode{tarp.IndexMemory, tarp.IndexBTree} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			opts := tarp.Options{IndexMode: mode, MaxRecordBytes: 128, TargetSegmentBytes: 256}
			dir := t.TempDir()
			db := openPublicDB(t, dir, opts)
			var batch tarp.WriteBatch
			for i := range 6 {
				batch.Delete([]byte(fmt.Sprintf("missing-%d", i)))
			}
			before := db.Stats()
			if err := db.Apply(&batch); err != nil || db.Stats() != before {
				t.Fatalf("fully elided Apply = %v, stats = %+v, want %+v", err, db.Stats(), before)
			}

			batch.Put([]byte("live"), []byte("old"))
			if err := db.Apply(&batch); err != nil {
				t.Fatal(err)
			}
			// The live Delete and two 110-byte Puts cannot fit together.
			// The error index still refers to the original queued operation.
			batch.Reset()
			batch.Delete([]byte("live"))
			batch.Put([]byte("b"), nil)
			batch.Delete([]byte("missing"))
			batch.Put([]byte("a"), bytes.Repeat([]byte("x"), 61))
			batch.Put([]byte("b"), bytes.Repeat([]byte("y"), 61))
			var limit *tarp.BatchLimitError
			if err := db.Apply(&batch); !errors.As(err, &limit) || limit.Kind != tarp.BatchRecordBytesLimit ||
				limit.Index != 4 || limit.Actual != 272 || limit.Limit != 192 {
				t.Fatalf("oversized Apply = %v, want 272 record bytes at index 4 (limit 192)", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened := openPublicDB(t, dir, opts)
			results, err := reopened.BatchGet([][]byte{[]byte("live"), []byte("a"), []byte("b")})
			if err != nil || len(results) != 3 || string(results[0].Value) != "old" || results[1].Found || results[2].Found {
				t.Fatalf("reopened BatchGet = %+v, %v", results, err)
			}
		})
	}
}

func TestLargeWriteBatchCommitsAtOneFrontierAndReopens(t *testing.T) {
	const rows = 10_000
	dir := t.TempDir()
	db := openPublicDB(t, dir, tarp.Options{})
	if err := db.Put([]byte("prior"), nil); err != nil {
		t.Fatal(err)
	}
	var batch tarp.WriteBatch
	batch.Delete([]byte("prior"))
	for i := range rows {
		batch.Put([]byte(fmt.Sprintf("row/%05d", i)), nil)
	}
	batch.Put([]byte("progress"), []byte("complete"))
	before, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	if err := db.Apply(&batch); err != nil {
		t.Fatal(err)
	}
	if s := db.Stats(); s.WriteCommits != 2 || s.WrittenRecords != uint64(1+batch.Len()) || s.LiveKeys != rows+1 {
		t.Fatalf("expected one commit for all rows and metadata: %+v", s)
	}
	if _, err := before.Get([]byte("progress")); !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("progress visible in previous snapshot: %v", err)
	}
	if _, err := before.Get([]byte("prior")); err != nil {
		t.Fatalf("previous snapshot lost deleted key: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openPublicDB(t, dir, tarp.Options{})
	if got, err := reopened.Get([]byte("progress")); err != nil || string(got) != "complete" {
		t.Fatalf("reopened progress = %q, %v", got, err)
	}
	if _, err := reopened.Get([]byte("prior")); !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("deleted key after reopen: %v", err)
	}
	seen := 0
	for key, err := range reopened.KeysRange(t.Context(), []byte("row/"), []byte("row0")) {
		if err != nil || string(key) != fmt.Sprintf("row/%05d", seen) {
			t.Fatalf("reopened row %d = %q, %v", seen, key, err)
		}
		seen++
	}
	if seen != rows {
		t.Fatalf("reopened %d rows, want %d", seen, rows)
	}
}

func TestWriteBatchLimitsErrorsAndClosedDB(t *testing.T) {
	opts := tarp.Options{
		MaxRecordBytes:     4 << 10,
		TargetSegmentBytes: 16 << 10,
	}
	db := openPublicDB(t, t.TempDir(), opts)
	if err := db.Put([]byte("keep"), []byte("initial")); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()

	// 1. MaxWriteBatchEntries count limit (Index == -1).
	var tooMany tarp.WriteBatch
	for range tarp.MaxWriteBatchEntries + 1 {
		tooMany.Delete([]byte("keep"))
	}
	var limitErr *tarp.BatchLimitError
	err := db.Apply(&tooMany)
	if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &limitErr) ||
		limitErr.Kind != tarp.BatchInputCountLimit || limitErr.Index != -1 || limitErr.Actual != tarp.MaxWriteBatchEntries+1 || limitErr.Limit != tarp.MaxWriteBatchEntries {
		t.Fatalf("Apply(>MaxWriteBatchEntries) = %v, want BatchInputCountLimit at -1", err)
	}

	// 2. Empty key reports original input index even after duplicates.
	var emptyKey tarp.WriteBatch
	emptyKey.Put([]byte("a"), []byte("1"))
	emptyKey.Delete([]byte("a"))
	emptyKey.Delete(nil)
	requireBatchKeyError(t, db.Apply(&emptyKey), "Apply", 2, tarp.ErrEmptyKey)

	// 3. Entry size limit reports exact input index.
	var hugeEntry tarp.WriteBatch
	hugeEntry.Put([]byte("ok"), []byte("1"))
	hugeEntry.Put([]byte("oversized"), bytes.Repeat([]byte("x"), 5<<10))
	err = db.Apply(&hugeEntry)
	if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &limitErr) ||
		limitErr.Kind != tarp.BatchEntrySizeLimit || limitErr.Index != 1 {
		t.Fatalf("Apply(oversized entry) = %v, want BatchEntrySizeLimit at 1", err)
	}

	// 4. Segment payload record-bytes limit uses COALESCED operations:
	// Five distinct 3.5 KiB Puts exceed 16 KiB segment payload at index 4...
	chunk := bytes.Repeat([]byte("p"), 3500)
	var segOverflow tarp.WriteBatch
	for i := range 5 {
		segOverflow.Put([]byte(fmt.Sprintf("big-%d", i)), chunk)
	}
	err = db.Apply(&segOverflow)
	if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &limitErr) ||
		limitErr.Kind != tarp.BatchRecordBytesLimit || limitErr.Index != 4 {
		t.Fatalf("Apply(segment overflow) = %v, want BatchRecordBytesLimit at 4", err)
	}

	if after := db.Stats(); after != before {
		t.Fatalf("failed Apply calls modified DB stats: before=%+v after=%+v", before, after)
	}

	// ...whereas overwriting or deleting earlier 3.5 KiB Puts in the SAME batch so the
	// coalesced set fits in 16 KiB succeeds!
	var coalescedFits tarp.WriteBatch
	for i := range 5 {
		coalescedFits.Put([]byte(fmt.Sprintf("big-%d", i)), chunk)
	}
	coalescedFits.Delete([]byte("big-0"))
	coalescedFits.Put([]byte("big-1"), []byte("tiny"))
	if err := db.Apply(&coalescedFits); err != nil {
		t.Fatalf("Apply(coalesced fits): %v", err)
	}

	// Closed DB rejects nil, empty, and non-empty Apply with ErrClosed.
	closedDB, err := tarp.Open(t.TempDir(), tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := closedDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closedDB.Apply(nil); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("closed Apply(nil) = %v, want ErrClosed", err)
	}
	var empty tarp.WriteBatch
	if err := closedDB.Apply(&empty); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("closed Apply(empty) = %v, want ErrClosed", err)
	}
}
