package engine

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestSnapshotsShareDataMembershipAcrossRevisions(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode IndexMode
	}{{"btree", IndexBTree}, {"memory", IndexMemory}} {
		t.Run(mode.name, func(t *testing.T) {
			opts := Options{IndexMode: mode.mode, MaxRecordBytes: 256, TargetSegmentBytes: 500}
			db := newTestDBWithHooks(t, opts, nil, parkedMaintenance())
			value := strings.Repeat("a", 160)
			mustPut(t, db, "old", value)
			mustPut(t, db, "live", value)
			mustPut(t, db, "hot", "before") // Rotates.
			old := testLoc(t, db, "old").Segment
			if testLoc(t, db, "hot").Segment == old {
				t.Fatal("expected two DATA segments")
			}
			capture := func() *Snapshot {
				t.Helper()
				snap, err := db.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = snap.Close() })
				return snap
			}
			first := capture()
			set := first.segments
			if err := first.Close(); err != nil || db.store.HasDataRetentionPin(old) {
				t.Fatalf("idle set still retains DATA: %v", err)
			}
			before := capture()
			mustPut(t, db, "hot", "after") // Same DATA membership.
			after := capture()
			if before.segments != set || after.segments != set || before.Sequence() == after.Sequence() {
				t.Fatal("sequential and overlapping captures must share unchanged membership")
			}
			assertValue := func(s *Snapshot, key, want string) {
				t.Helper()
				got, err := s.Get([]byte(key))
				if string(got) != want || err != nil {
					t.Fatalf("snapshot Get(%q) = %q, %v; want %q", key, got, err, want)
				}
			}
			assertValue(before, "hot", "before")
			assertValue(after, "hot", "after")

			if err := db.Delete([]byte("old")); err != nil {
				t.Fatal(err)
			}
			result, err := db.Compact(t.Context())
			if err != nil || result.PinnedRetiredDataBytes == 0 || old.References() != 0 || !db.store.DataReferenced(old) {
				t.Fatalf("Compact with retained view: %+v, %v, direct references %d", result, err, old.References())
			}
			current := capture()
			if current.segments == set || current.segments.files[old.ID] != nil {
				t.Fatal("snapshot after checkpoint still has retired membership")
			}
			assertValue(before, "old", value)
			if err := before.Close(); err != nil || !db.store.DataReferenced(old) {
				t.Fatalf("first close released the shared file: %v", err)
			}
			assertValue(after, "old", value)
			if err := after.Close(); err != nil || db.store.DataReferenced(old) {
				t.Fatalf("last close retained the retired file: %v", err)
			}
			if _, err := db.Compact(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(old.Path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retired DATA still on disk: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := current.Get([]byte("live")); !errors.Is(err, ErrClosed) {
				t.Fatalf("snapshot read after DB.Close: %v", err)
			}
		})
	}
}

// Done is first called when the point read joins the shared repair wait.
type pointRepairWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *pointRepairWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestSnapshotGetContextLeavesSharedRepairAndReleasesPins(t *testing.T) {
	paused, resume := make(chan struct{}), make(chan struct{})
	var renames atomic.Int32
	fs := &storage.IO{Hook: func(point string) error {
		if point == "data-repair.rename:before" && renames.Add(1) == 1 {
			close(paused)
			<-resume
		}
		return nil
	}}
	db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
	damageProtectedReadFixture(t, db)
	file := testLoc(t, db, "target").Segment
	baseRefs := file.References()
	release := sync.OnceFunc(func() { close(resume) })
	defer release()

	ctx, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	await := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-ctx.Done():
			t.Fatal("timed out waiting for repair")
		}
	}
	type result struct {
		value []byte
		err   error
	}
	leader := make(chan result, 1)
	go func() {
		value, err := db.GetContext(ctx, []byte("target"))
		leader <- result{value, err}
	}()
	await(paused)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	followerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	waiting := &pointRepairWaitContext{Context: followerCtx, waiting: make(chan struct{})}
	follower := make(chan result, 1)
	go func() {
		value, err := snap.GetContext(waiting, []byte("target"))
		follower <- result{value, err}
	}()
	await(waiting.waiting)
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if !db.store.HasDataRetentionPin(file) {
		t.Fatal("Close released the capture while a point read still used it")
	}
	cancel()
	select {
	case got := <-follower:
		if got.value != nil || !errors.Is(got.err, context.Canceled) || errors.Is(got.err, ErrCommitUncertain) {
			t.Fatalf("waiting GetContext returned %q, %v", got.value, got.err)
		}
	case <-ctx.Done():
		t.Fatal("canceling a waiting point read did not release it")
	}
	if db.store.HasDataRetentionPin(file) || file.References() != baseRefs+1 {
		t.Fatalf("canceled read left capture ownership or changed the leader's reference count: %d", file.References())
	}
	release()
	select {
	case got := <-leader:
		if string(got.value) != "original" || got.err != nil {
			t.Fatalf("leading GetContext returned %q, %v", got.value, got.err)
		}
	case <-ctx.Done():
		t.Fatal("leading repair did not finish")
	}
	if renames.Load() != 1 || file.References() != baseRefs {
		t.Fatalf("renames = %d, references = %d; want 1, %d", renames.Load(), file.References(), baseRefs)
	}
	wantValue(t, db, "target", "original")
}

func TestSnapshotGetContextPreservesRepairPublicationOutcome(t *testing.T) {
	for _, test := range []struct {
		phase     string
		uncertain bool
	}{{"data-repair.sync:after", false}, {"data-repair.dirsync:before", true}} {
		t.Run(test.phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fs := &storage.IO{Hook: func(point string) error {
				if point == test.phase {
					cancel()
					if test.uncertain {
						return ctx.Err()
					}
				}
				return nil
			}}
			db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
			damageProtectedReadFixture(t, db)
			file := testLoc(t, db, "target").Segment
			baseRefs := file.References()
			snap, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			value, err := snap.GetContext(ctx, []byte("target"))
			var op *OpError
			if value != nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrCommitUncertain) != test.uncertain ||
				!errors.As(err, &op) || op.Outcome != NotApplied {
				t.Fatalf("GetContext returned %q, %v", value, err)
			}
			if err := snap.Close(); err != nil {
				t.Fatal(err)
			}
			if got := file.References(); got != baseRefs || db.store.HasDataRetentionPin(file) {
				t.Fatalf("references after Close = %d, want %d", got, baseRefs)
			}
			if test.uncertain {
				if err := db.Put([]byte("other"), nil); !errors.Is(err, ErrUnhealthy) {
					t.Fatalf("Put after uncertain DATA replacement: %v", err)
				}
			} else {
				wantValue(t, db, "target", "original")
			}
		})
	}
}

func TestSnapshotBatchGetRejectsMixedCacheHitsBeforeCopying(t *testing.T) {
	const copies = 128
	db := newTestDBWithHooks(t, Options{IndexMode: IndexMemory, MaxBatchGetValueBytes: 96}, nil, parkedMaintenance())
	mustPut(t, db, "hot", strings.Repeat("v", 64))
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	keys := make([][]byte, copies+1)
	keys[0] = []byte("missing")
	for i := 1; i < len(keys); i++ {
		keys[i] = []byte("hot")
	}
	// The index plan needs a few slices. A rejected batch must never allocate
	// the 128 independent value copies requested by its cached inputs.
	allocs := testing.AllocsPerRun(3, func() {
		results, err := snapshot.BatchGet(keys)
		var limit *BatchLimitError
		if results != nil || !errors.As(err, &limit) || limit.Kind != BatchGetValueBytesLimit ||
			limit.Index != 2 || limit.Actual != 128 || limit.Limit != 96 {
			t.Fatalf("rejected BatchGet returned %v, %v", results, err)
		}
	})
	if allocs >= copies {
		t.Fatalf("rejected BatchGet made %.0f allocations for %d cached inputs", allocs, copies)
	}
}
