package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

	// 1. MaxBatchEntries count limit (Index == -1).
	var tooMany tarp.WriteBatch
	for i := range tarp.MaxBatchEntries + 1 {
		tooMany.Delete([]byte(fmt.Sprintf("k-%d", i)))
	}
	var limitErr *tarp.BatchLimitError
	err := db.Apply(&tooMany)
	if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &limitErr) ||
		limitErr.Kind != tarp.BatchInputCountLimit || limitErr.Index != -1 {
		t.Fatalf("Apply(>MaxBatchEntries) = %v, want BatchInputCountLimit at -1", err)
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
