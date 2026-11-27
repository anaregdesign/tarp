package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestCleanerInitialInventoryUsesBoundedWorkerPasses(t *testing.T) {
	const records = cleanerInventoryWindow + 4
	dir := t.TempDir()
	opts := Options{IndexMode: IndexMemory, MaxRecordBytes: 4096 - format.SegmentHeaderBytes, TargetSegmentBytes: 4096}
	key := func(i int) string { return fmt.Sprintf("key-%04d", i) }
	value := strings.Repeat("v", 3000)
	seed, err := OpenWithHooks(dir, opts, nil, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { seed.Close() })
	for i := range records {
		mustPut(t, seed, key(i), value)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	open := func(t *testing.T, start <-chan struct{}) *DB {
		t.Helper()
		db, err := OpenWithHooks(dir, opts, nil, &Hooks{CleanerStart: start, ParityStart: make(chan struct{})})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		q := &db.cleaner.candidates
		if q.seeded != 0 || q.ready || len(q.files) != 0 || q.seedThrough != records-1 {
			t.Fatalf("Open seeded cleaner inventory while worker was parked: seeded %d, files %d, ready %t, end %d", q.seeded, len(q.files), q.ready, q.seedThrough)
		}
		return db
	}
	t.Run("cancel and Close between windows", func(t *testing.T) {
		db := open(t, make(chan struct{}))
		if again, retry := db.cleanPass(t.Context()); !again || retry || db.cleaner.candidates.seeded != cleanerInventoryWindow {
			t.Fatalf("first window seeded %d, again %t, retry %t", db.cleaner.candidates.seeded, again, retry)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		done := make(chan struct{})
		func() {
			db.writer.Lock()
			defer db.writer.Unlock()
			go func() {
				defer close(done)
				db.cleanPass(ctx)
			}()
			for len(db.cleaner.gate) != 0 {
				if ctx.Err() != nil {
					t.Fatal("pass did not acquire maintenance slot")
				}
				runtime.Gosched()
			}
			cancel()
		}()
		<-done
		if q := &db.cleaner.candidates; q.seeded != cleanerInventoryWindow || q.ready {
			t.Fatalf("canceled pass advanced inventory: seeded %d, ready %t", q.seeded, q.ready)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		if len(db.cleaner.candidates.files) != 0 {
			t.Fatal("Close retained the partial inventory")
		}
	})
	t.Run("mutations, damage and pin release before inventory completes", func(t *testing.T) {
		start := make(chan struct{})
		db := open(t, start)
		entries := db.store.CatalogEntries()
		first := entries.At(0)
		firstPath := db.store.Segments[first.ID].Path
		oldActive := db.store.Segments[db.store.Current.ActiveID]
		snap, err := db.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		defer snap.Close()

		newValue := strings.Repeat("n", len(value))
		mustPut(t, db, key(0), newValue)
		mustPut(t, db, key(records-1), newValue)
		if firstTracked := db.cleaner.candidates.files[first.ID]; firstTracked != nil || len(db.cleaner.candidates.files) != 2 {
			t.Fatal("preseed writes did not track only the newly sealed DATA files")
		}

		// A DATA failure in the next inventory window must remain queued
		// until the cleaner knows which files belong to the selected layout.
		damaged := entries.At(cleanerInventoryWindow)
		segment := db.store.Segments[damaged.ID]
		raw, err := os.ReadFile(segment.Path)
		if err != nil {
			t.Fatal(err)
		}
		raw[len(raw)-1] ^= 1
		if err := os.WriteFile(segment.Path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(selectedParity(t, db, damaged)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Get([]byte(key(cleanerInventoryWindow))); !errors.Is(err, ErrDataUnavailable) {
			t.Fatalf("damaged DATA read = %v", err)
		}
		if again, retry := db.cleanPass(t.Context()); !again || retry {
			t.Fatalf("initial window did not schedule another pass: again %t, retry %t", again, retry)
		}
		q := &db.cleaner.candidates
		if q.seeded != cleanerInventoryWindow || q.ready || q.debt != uint64(first.Bytes+oldActive.Size.Load()) {
			t.Fatalf("first window seeded %d, ready %t, reclaimable %d", q.seeded, q.ready, q.debt)
		}
		if db.cleaner.damage[damaged.ID] != segment || q.files[damaged.ID] != nil {
			t.Fatal("damage event was consumed before its DATA file was seeded")
		}
		if got, err := snap.Get([]byte(key(0))); err != nil || string(got) != value {
			t.Fatalf("snapshot lost the previous value: %v", err)
		}
		if err := snap.Close(); err != nil {
			t.Fatal(err)
		}

		close(start)
		waitForUnlink(t, db, firstPath)
		waitForUnlink(t, db, oldActive.Path)
		db.writer.Lock()
		ready, seeded, debt := q.ready, q.seeded, q.debt
		blocked := db.cleaner.blocked[damaged.ID]
		db.writer.Unlock()
		if !ready || seeded != records-1 || debt != 0 || blocked == nil {
			t.Fatalf("inventory did not converge: ready %t, seeded %d, reclaimable %d, blocked %v", ready, seeded, debt, blocked)
		}
		wantValue(t, db, key(0), newValue)
		wantValue(t, db, key(records-1), newValue)
		if err := db.Close(); !errors.Is(err, ErrDataUnavailable) {
			t.Fatalf("Close lost selected DATA damage: %v", err)
		}
	})
}

func TestCleanerCandidateQueueBoundsWorkAndRetriesReleasedPins(t *testing.T) {
	s := &storage.Store{Segments: make(map[uint64]*storage.DataSegment)}
	var q cleanerCandidates
	const size, files = 2048, 3 * cleanerCandidateWindow
	for i := range files {
		id := uint64(i + 1)
		entry := format.CatalogEntry{ID: id, Kind: format.RawLogKind, Bytes: size}
		segment := &storage.DataSegment{}
		s.Segments[id] = segment
		segment.LiveBytes.Store(size - format.SegmentHeaderBytes)
		q.track(s, entry, i, 4096)
		if q.pending.Len() != i {
			t.Fatal("full segment became a candidate")
		}
		segment.LiveBytes.Store(128)
		q.changed(q.files[id], segment, 4096)
		q.changed(q.files[id], segment, 4096)
	}
	if q.pending.Len() != files || q.debt != files*(size-format.SegmentHeaderBytes-128) {
		t.Fatalf("queue length %d, projected DATA savings %d", q.pending.Len(), q.debt)
	}
	taken := q.take()
	if len(taken) != cleanerCandidateWindow || q.pending.Len() != files-cleanerCandidateWindow {
		t.Fatal("candidate work exceeded the pass limit")
	}
	for _, file := range taken {
		q.deferPin(file, 1)
	}
	q.resumePins(1, true)
	if q.pinned.Len() != cleanerCandidateWindow {
		t.Fatal("pin-blocked work retried without a pin release")
	}
	q.resumePins(2, true)
	if q.pinned.Len() != 0 || q.pending.Len() != files {
		t.Fatal("pin release did not return candidates to the work queue")
	}
	q.remove(taken[0].entry.ID)
	if q.pending.Len() != files-1 || q.debt != (files-1)*(size-format.SegmentHeaderBytes-128) {
		t.Fatal("retired work remained in the queue or DATA savings")
	}
}
