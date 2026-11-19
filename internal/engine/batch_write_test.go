package engine

import (
	"bytes"
	"errors"
	"maps"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func requireWriteStats(t *testing.T, db *DB, commits, records, liveBytes uint64) {
	t.Helper()
	s := db.Stats()
	if s.WriteCommits != commits || s.WrittenRecords != records || s.LiveRecordBytes != liveBytes {
		t.Fatalf("write stats: commits %d, records %d, live bytes %d; want %d, %d, %d",
			s.WriteCommits, s.WrittenRecords, s.LiveRecordBytes, commits, records, liveBytes)
	}
}

func TestBatchWriteRejectsWholeRequestBeforeMutation(t *testing.T) {
	db := newTestDB(t, testOptions(), &storage.IO{CleanerStart: make(chan struct{})})
	mustPut(t, db, "a", "old")
	entry := Entry{[]byte("a"), []byte("new")}
	before := directoryBytes(t, db.store.Dir)
	frontier := db.store.Current
	for _, tc := range []struct {
		run  func() error
		want error
	}{
		{func() error { return db.BatchPut(nil) }, nil},
		{func() error { return db.BatchDelete(nil) }, nil},
		{func() error { return db.BatchPut([]Entry{entry, {}}) }, ErrEmptyKey},
		{func() error { return db.BatchDelete([][]byte{entry.Key, nil}) }, ErrEmptyKey},
		{func() error { return db.BatchDelete([][]byte{entry.Key, []byte("absent")}) }, ErrNotFound},
		{func() error { return db.BatchPut([]Entry{entry, {entry.Key, make([]byte, 200)}}) }, ErrTooLarge},
		{func() error { return db.BatchPut([]Entry{entry, entry, entry, entry}) }, ErrTooLarge},
		{func() error { return db.BatchDelete([][]byte{{1}, {2}, {3}, {4}}) }, ErrTooLarge},
		{func() error { return db.BatchPut(make([]Entry, MaxBatchEntries+1)) }, ErrTooLarge},
		{func() error { return db.BatchDelete(make([][]byte, MaxBatchEntries+1)) }, ErrTooLarge},
	} {
		if err := tc.run(); !errors.Is(err, tc.want) {
			t.Fatalf("got %v, want %v", err, tc.want)
		}
		if db.store.Current != frontier || !maps.Equal(before, directoryBytes(t, db.store.Dir)) {
			t.Fatal("batch changed files despite admission result", tc.want)
		}
		wantValue(t, db, "a", "old")
		requireWriteStats(t, db, 1, 1, 52)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchPut(nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := db.BatchDelete(nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestBatchWritesRotateAndAccountInInputOrder(t *testing.T) {
	opts := testOptions() // 192 encoded bytes fit exactly in a new log.
	opts.CacheBytes = 144 // one key and 15 bytes of value
	db := newTestDB(t, opts, &storage.IO{CleanerStart: make(chan struct{})})
	mustPut(t, db, "a", "old")
	before := db.store.Current
	oldFile := db.index["a"].File
	newEntry := func(key, value string) Entry {
		return Entry{[]byte(key), []byte(strings.Repeat(value, 15))}
	}
	entries := []Entry{newEntry("a", "1"), newEntry("b", "2"), newEntry("a", "3")}
	if err := db.BatchPut(entries); err != nil {
		t.Fatal(err)
	}
	requireWriteStats(t, db, 2, 4, 128)
	after := db.store.Current
	if after.Generation != before.Generation+1 || after.Sequence != before.Sequence+3 || after.ActiveID == before.ActiveID || after.ActiveEnd != 256 {
		t.Fatalf("batch frontier: %+v -> %+v", before, after)
	}
	for _, entry := range entries {
		entry.Key[0] = 'x'
		entry.Value[0] = 'x'
	}
	file := db.index["a"].File
	if oldFile.LiveBytes.Load() != 0 || oldFile.References() != 0 || file.LiveBytes.Load() != 128 || file.References() != 2 {
		t.Fatalf("live bytes/pins: old %d/%d, new %d/%d", oldFile.LiveBytes.Load(), oldFile.References(), file.LiveBytes.Load(), file.References())
	}
	if v, ok := db.cache.Get([]byte("a")); !ok || !bytes.Equal(v, bytes.Repeat([]byte("3"), 15)) {
		t.Fatalf("last write missing from cache: %q", v)
	}
	if _, ok := db.cache.Get([]byte("b")); ok {
		t.Fatal("expected eviction by final duplicate put")
	}
	keys := [][]byte{[]byte("b"), []byte("a"), []byte("a")}
	if err := db.BatchDelete(keys); err != nil {
		t.Fatal(err)
	}
	requireWriteStats(t, db, 3, 6, 0)
	tombstones := db.store.Files[db.store.Current.ActiveID]
	if db.store.Current.Sequence != after.Sequence+2 || db.store.Current.Generation != after.Generation+1 ||
		tombstones.DeleteBytes.Load() != 98 || file.LiveBytes.Load() != 0 || file.References() != 0 || len(db.index) != 0 || db.cache.Bytes() != 0 {
		t.Fatalf("delete accounting: frontier %+v, tombstones %d, live %d, refs %d", db.store.Current, tombstones.DeleteBytes.Load(), file.LiveBytes.Load(), file.References())
	}
}

func TestBatchWritesRecoverWholeOutcome(t *testing.T) {
	for _, op := range []string{"batch-put", "batch-delete"} {
		for _, tc := range []struct {
			point   string
			outcome Outcome
			apply   bool
		}{
			{"data.write:after", NotApplied, false},
			{"current.rename:before", Unknown, false},
			{"current.rename:after", Unknown, true},
			{"rotation.close:after", Committed, true},
		} {
			t.Run(op+"/"+tc.point, func(t *testing.T) {
				armed := false
				fs := &storage.IO{CleanerStart: make(chan struct{}), Hook: func(p string) error {
					if armed && p == tc.point {
						return syscall.EIO
					}
					return nil
				}}
				opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: 170} // Pairs fit; each batch rotates.
				db := newTestDB(t, opts, fs)
				entries := []Entry{{[]byte("a"), []byte("old")}, {[]byte("b"), []byte("old")}}
				if err := db.BatchPut(entries); err != nil {
					t.Fatal(err)
				}
				armed = true
				var err error
				if op == "batch-put" {
					entries[0].Value, entries[1].Value = []byte("new"), []byte("new")
					err = db.BatchPut(entries)
				} else {
					err = db.BatchDelete([][]byte{entries[0].Key, entries[1].Key})
				}
				var opErr *OpError
				if !errors.As(err, &opErr) || opErr.Operation != op || opErr.Outcome != tc.outcome || !errors.Is(err, syscall.EIO) ||
					errors.Is(err, ErrCommitUncertain) != (tc.outcome == Unknown) {
					t.Fatalf("unexpected batch fault: %v", err)
				}
				if err := db.BatchPut(nil); !errors.Is(err, ErrUnhealthy) {
					t.Fatal(err)
				}
				requireWriteStats(t, db, 1, 2, 104)
				if err := db.Close(); !errors.Is(err, syscall.EIO) {
					t.Fatal(err)
				}
				reopened, err := Open(db.store.Dir, opts)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				want := map[string]string{"a": "old", "b": "old"}
				if tc.apply && op == "batch-put" {
					want = map[string]string{"a": "new", "b": "new"}
				}
				if tc.apply && op == "batch-delete" {
					want = nil
				}
				if got := scanContents(t, reopened.Scan(t.Context())); !maps.Equal(got, want) {
					t.Fatalf("after recovery: %v, want %v", got, want)
				}
				requireWriteStats(t, reopened, 0, 0, uint64(52*len(want)))
			})
		}
	}
}

func TestBatchPublicationWithScanCheckpointAndBackup(t *testing.T) {
	var armed atomic.Bool
	copied, resume := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	db := newTestDB(t, testOptions(), &storage.IO{PutCopied: func(int, int) {
		if armed.CompareAndSwap(true, false) {
			close(copied)
			<-resume
		}
	}})
	entries := []Entry{{[]byte("a"), []byte("old")}, {[]byte("b"), []byte("old")}}
	if err := db.BatchPut(entries); err != nil {
		t.Fatal(err)
	}
	entries[0].Value, entries[1].Value = []byte("new"), []byte("new")
	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- db.BatchPut(entries) }()
	<-copied
	// Commit finished, but the batch is still preparing its owned copies.
	requireWriteStats(t, db, 1, 2, 104)
	old := scanContents(t, db.Scan(t.Context()))
	close(resume)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(old, map[string]string{"a": "old", "b": "old"}) {
		t.Fatal("observed intermediate batch state", old)
	}
	requireWriteStats(t, db, 2, 4, 104)
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	backup, restored := filepath.Join(t.TempDir(), "backup"), filepath.Join(t.TempDir(), "restored")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := Restore(t.Context(), RestoreRequest{
		Source:      backup,
		Destination: restored,
		Limits:      testOptions().RestoreLimits(),
	}); err != nil {
		t.Fatal(err)
	}
	copyDB, err := Open(restored, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	if got := scanContents(t, copyDB.Scan(t.Context())); !maps.Equal(got, map[string]string{"a": "new", "b": "new"}) {
		t.Fatal("restored state", got)
	}
	requireWriteStats(t, db, 2, 4, 104)
	requireWriteStats(t, copyDB, 0, 0, 104)
}
