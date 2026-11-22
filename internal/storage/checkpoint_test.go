package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func TestCheckpointDiscardsOnlyItsOwnSnapshotFiles(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 64, TargetSegmentBytes: format.SegmentHeaderBytes + 64, Handles: 1}
	store, err := openTestStore(t, t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	live := memory.New()
	tx := live.Begin()
	for _, key := range []string{"a", "b"} {
		loc, err := store.Append(format.Put, []byte(key), nil, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Put(key, loc.Address())
		if err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	if err := store.Rotate(); err != nil {
		t.Fatal(err)
	}
	capture, err := store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	view := live.View()
	defer view.Close()
	moves := store.NewRelocations()
	defer moves.Close()
	checkpoint := store.NewDataCheckpoint(capture)
	var reserved []*DataSegment
	reserve := func() (*DataSegment, error) {
		segment, err := store.ReserveSnapshotSegment()
		if err != nil {
			return nil, err
		}
		reserved = append(reserved, segment)
		if len(reserved) == 2 {
			// The exclusive create must leave this other file untouched.
			err = os.WriteFile(segment.Path, []byte("existing"), 0600)
		}
		return segment, err
	}
	if err := checkpoint.Copy(t.Context(), memory.New(), view, moves, reserve); !errors.Is(err, os.ErrExist) || len(reserved) != 2 {
		t.Fatalf("copy with %d reserved files: %v", len(reserved), err)
	}
	if err := checkpoint.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reserved[0].Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unpublished owned file after discard: %v", err)
	}
	if data, err := os.ReadFile(reserved[1].Path); err != nil || string(data) != "existing" {
		t.Fatalf("preexisting file after discard: %q, %v", data, err)
	}
	for _, segment := range reserved {
		if store.Segments[segment.ID] != nil {
			t.Fatalf("unpublished snapshot %d entered the DATA segment registry", segment.ID)
		}
	}
}

func TestCheckpointRetriesOwnedOutputAfterHeaderAndDiscardFailures(t *testing.T) {
	fs := &IO{Hook: func(point string) error {
		switch point {
		case "compact.segment.write:before":
			return syscall.ENOSPC
		case "compact.discard:before":
			return syscall.EACCES
		}
		return nil
	}}
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 64, TargetSegmentBytes: 256, Handles: 1}
	s, err := openTestStore(t, t.TempDir(), cfg, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loc, err := s.Append(format.Put, []byte("x"), nil, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Rotate(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.RetireAfterCheckpoint(); err != nil {
		t.Fatal(err)
	}
	cap, err := s.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer cap.Release()
	cp := s.NewDataCheckpoint(cap)
	live := memory.New()
	tx := live.Begin()
	if err = tx.Put("x", loc.Address()); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	view := live.View()
	defer view.Close()
	moves := s.NewRelocations()
	defer moves.Close()
	if err = cp.CopyByKey(t.Context(), view, 1, moves, s.ReserveSnapshotSegment); !errors.Is(err, syscall.ENOSPC) || len(cp.segments) != 1 {
		t.Fatalf("failed to retain ownership of incomplete output: %v, %d files", err, len(cp.segments))
	}
	f := cp.segments[0]
	// No parity output was created. A different file at the candidate name
	// must also survive targeted retirement of our incomplete DATA.
	parity := filepath.Join(s.Dir, parityName(cap.Current.UUID, f.ID))
	if err = os.WriteFile(parity, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = cp.Discard(); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("discard error = %v", err)
	}
	cp.QueueFailedDiscard()
	if !s.OrderedCleanupPending() {
		t.Fatal("lost incomplete DATA retirement")
	}
	if _, _, err = s.RetireAfterCheckpoint(); err != nil || s.OrderedCleanupPending() {
		t.Fatalf("retry left work: %v", err)
	}
	if _, err = os.Stat(f.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete DATA after retry: %v", err)
	}
	if b, err := os.ReadFile(parity); err != nil || string(b) != "existing" {
		t.Fatalf("unowned parity after retry = %q, %v", b, err)
	}
}

func TestCheckpointRejectsAStaleLocationWithTheRightLiveCount(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 128, TargetSegmentBytes: 512, Handles: 1}
	db, err := openTestStore(t, t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	viewIndex := memory.New()
	for _, value := range []string{"old", "current"} {
		loc, err := db.Append(format.Put, []byte("key"), []byte(value), func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if value == "old" {
			tx := viewIndex.Begin()
			if err := tx.Put("key", loc.Address()); err != nil {
				t.Fatal(err)
			}
			tx.Commit()
		}
	}
	if err := db.Rotate(); err != nil {
		t.Fatal(err)
	}
	before := db.Current
	capture, err := db.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	view := viewIndex.View()
	defer view.Close()
	checkpoint := db.NewDataCheckpoint(capture)
	moves := db.NewRelocations()
	defer moves.Close()
	if err := checkpoint.Copy(t.Context(), memory.New(), view, moves, db.ReserveSnapshotSegment); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("stale derived index accepted: %v", err)
	}
	if err := checkpoint.Discard(); err != nil {
		t.Fatal(err)
	}
	if db.Current != before {
		t.Fatal("replaced authoritative DATA after stale lookup")
	}
}

// Ten times as many keys must move through the same fixed page caches. Test
// charged residency directly so allocator and GC timing cannot mask growth.
func TestCheckpointSpoolsTenfoldGrowthWithinFixedPageCaches(t *testing.T) {
	var smallBytes int64
	for _, n := range []int{256, 2560} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			const liveBudget, historyBudget = 512 << 10, 256 << 10
			live, err := btree.NewIn(t.TempDir(), liveBudget)
			if err != nil {
				t.Fatal(err)
			}
			defer live.Close()
			history, err := btree.NewIn(t.TempDir(), historyBudget)
			if err != nil {
				t.Fatal(err)
			}
			defer history.Close()
			liveMeter, historyMeter := live.(index.Meter), history.(index.Meter)
			var livePeak, historyPeak uint64
			io := &IO{Hook: func(point string) error {
				if point == "compact.record:after" {
					livePeak = max(livePeak, liveMeter.Metrics().CacheBytes)
					historyPeak = max(historyPeak, historyMeter.Metrics().CacheBytes)
				}
				return nil
			}}
			cfg := Config{ValueCacheBudgetBytes: 2 << 20, MaxRecordBytes: 1024, TargetSegmentBytes: 64 << 10, Handles: 4}
			store, _, err := OpenIndexed(t.Context(), t.TempDir(), cfg, false, io, func(*Store) (index.Backend, error) { return live, nil }, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			const batch = 64
			suffix := strings.Repeat("k", 190)
			for start := 0; start < n; start += batch {
				entries := make([]Entry, batch)
				for i := range entries {
					entries[i].Key = []byte(fmt.Sprintf("%08d-%s", start+i, suffix))
				}
				locs, err := store.AppendBatch(format.Put, entries, func() error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				tx := live.Begin()
				for i, entry := range entries {
					if err := tx.Put(string(entry.Key), locs[i].Address()); err != nil {
						tx.Abort()
						t.Fatal(err)
					}
				}
				tx.Commit()
			}
			if err := store.Rotate(); err != nil {
				t.Fatal(err)
			}
			capture, err := store.Capture()
			if err != nil {
				t.Fatal(err)
			}
			defer capture.Release()
			view := live.View()
			defer view.Close()
			checkpoint := store.NewDataCheckpoint(capture)
			moves := store.NewRelocations()
			defer moves.Close()
			if err := checkpoint.Copy(t.Context(), history, view, moves, store.ReserveSnapshotSegment); err != nil {
				t.Fatal(err)
			}
			// A sorted builder publishes its first cache pages after the DATA scan.
			historyPeak = max(historyPeak, historyMeter.Metrics().CacheBytes)
			if moves.count != n || moves.fd == nil || history.Count() != n {
				t.Fatalf("spool count %d, replay count %d, want %d", moves.count, history.Count(), n)
			}
			if livePeak == 0 || historyPeak == 0 || livePeak > liveBudget || historyPeak > historyBudget {
				t.Fatalf("foreground cache = %d, replay cache = %d", livePeak, historyPeak)
			}
			if len(moves.segments) > len(capture.Segments)+len(checkpoint.Entries()) {
				t.Fatalf("spool retained %d DATA files", len(moves.segments))
			}
			if err := checkpoint.Protect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := checkpoint.Install(); err != nil {
				t.Fatal(err)
			}
			work, err := store.BeginClean(checkpoint.Entries())
			if err != nil || work == nil {
				t.Fatalf("select installed snapshots: %v", err)
			}
			defer work.Release()
			cleanHistory, err := btree.NewIn(t.TempDir(), historyBudget)
			if err != nil {
				t.Fatal(err)
			}
			defer cleanHistory.Close()
			cleanMeter := cleanHistory.(index.Meter)
			var cleanPeak uint64
			cleanMoves := store.NewRelocations()
			defer cleanMoves.Close()
			keep := func(string, Location) (bool, error) {
				cleanPeak = max(cleanPeak, cleanMeter.Metrics().CacheBytes)
				return true, nil
			}
			if err := work.Copy(t.Context(), io, cleanMoves, cleanHistory, func() error { return nil }, keep); err != nil {
				t.Fatal(err)
			}
			if cleanPeak == 0 || cleanPeak > historyBudget || cleanMoves.count != n || cleanMoves.bytes != moves.bytes {
				t.Fatalf("clean cache %d, relocations %d/%d bytes, checkpoint %d/%d", cleanPeak, cleanMoves.count, cleanMoves.bytes, moves.count, moves.bytes)
			}
			if smallBytes == 0 {
				smallBytes = moves.bytes
			} else if moves.bytes != 10*smallBytes || historyMeter.Metrics().PageReads == 0 {
				t.Fatalf("spool grew from %d to %d bytes, replay page reads %d", smallBytes, moves.bytes, historyMeter.Metrics().PageReads)
			}
		})
	}
}

// A full output fits one complete write buffer plus a partial flush. The
// second write therefore fails during rollover, before any output is synced.
func TestOrderedCheckpointCoalescesWritesAndRetriesFailedSegmentFlush(t *testing.T) {
	const count = 512
	ctx := t.Context()
	fs := &IO{}
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 512, TargetSegmentBytes: 48 << 10, Handles: 2}
	s, err := openTestStore(t, t.TempDir(), cfg, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	live := memory.New()
	tx := live.Begin()
	keyAt := func(i int) string { return fmt.Sprintf("%04d", i) }
	for start := 0; start < count; start += 128 {
		entries := make([]Entry, 128)
		for i := range entries {
			k := keyAt(count - 1 - start - i)
			entries[i] = Entry{Key: []byte(k), Value: []byte(strings.Repeat(k, 64))}
		}
		locs, err := s.AppendBatch(format.Put, entries, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		for i, loc := range locs {
			if err := tx.Put(string(entries[i].Key), loc.Address()); err != nil {
				t.Fatal(err)
			}
		}
	}
	tx.Commit()
	if err = s.Rotate(); err != nil {
		t.Fatal(err)
	}
	cap, err := s.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer cap.Release()
	view := live.View()
	defer view.Close()
	var calls int
	fs.Hook = func(point string) error {
		if point == "compact.write:before" {
			calls++
			if calls == 2 {
				return syscall.ENOSPC
			}
		}
		return nil
	}
	cp := s.NewDataCheckpoint(cap)
	moves := s.NewRelocations()
	err = cp.CopyByKey(ctx, view, count, moves, s.ReserveSnapshotSegment)
	var destination *destinationError
	if !errors.Is(err, syscall.ENOSPC) || !errors.As(err, &destination) || len(cp.Entries()) != 0 || s.Current != cap.Current {
		t.Fatalf("failed flush: entries %d, candidate published %t, err %v", len(cp.Entries()), s.Current != cap.Current, err)
	}
	if err := errors.Join(cp.Discard(), moves.Close()); err != nil {
		t.Fatal(err)
	}
	for _, f := range cp.segments {
		if _, err := os.Stat(f.Path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("incomplete snapshot still present: %v", err)
		}
	}
	calls = 0
	fs.Hook = func(point string) error {
		if point == "compact.write:before" {
			calls++
		}
		return nil
	}
	cp = s.NewDataCheckpoint(cap)
	moves = s.NewRelocations()
	defer moves.Close()
	if err = cp.CopyByKey(ctx, view, count, moves, s.ReserveSnapshotSegment); err != nil {
		t.Fatal(err)
	}
	if err = cp.Protect(ctx); err != nil {
		t.Fatal(err)
	}
	if err = cp.Install(); err != nil {
		t.Fatal(err)
	}
	rowCount, maxCalls := 0, 0
	for _, e := range cp.Entries() {
		data, err := os.ReadFile(s.Segments[e.ID].Path)
		if err != nil {
			t.Fatal(err)
		}
		payload := data[format.SegmentHeaderBytes:]
		maxCalls += (len(payload) + (32 << 10) - 1) / (32 << 10)
		for len(payload) > 0 {
			key := keyAt(rowCount)
			row := format.EncodeRecord(format.Put, uint64(count-rowCount), []byte(key), []byte(strings.Repeat(key, 64)))
			if !bytes.HasPrefix(payload, row) {
				t.Fatalf("snapshot %d differs at row %d", e.ID, rowCount)
			}
			payload = payload[len(row):]
			rowCount++
		}
	}
	if rowCount != count || len(cp.Entries()) < 3 || calls < 1 || calls > maxCalls {
		t.Fatalf("rows %d, files %d, compact.write calls %d (limit %d)", rowCount, len(cp.Entries()), calls, maxCalls)
	}
}
