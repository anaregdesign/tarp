package engine

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestStagedApplyReopensFromHeaderOnlyActiveLog(t *testing.T) {
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			db := newTestDBWithHooks(t, testOptions(), nil, parkedMaintenance())
			value := strings.Repeat("v", 72)
			if err := db.ApplyStaged(t.Context(), func(yield func(MixedOp, error) bool) {
				for i := range count {
					if !yield(MixedOp{Key: []byte{byte('a' + i)}, Value: []byte(value)}, nil) {
						return
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			if stats := db.Stats(); stats.SelectedSegments != uint64(count) || stats.LiveKeys != uint64(count) {
				t.Fatalf("staged files = %+v", stats)
			}
			// Reopen the raw published selection. Close would write a new
			// checkpoint and could hide an invalid staged catalog transition.
			reopened := openRefreshFixture(t, db)
			if got := reopened.RecoveryInfo().LastUserSequence; got != uint64(count) {
				t.Fatalf("recovered sequence = %d, want %d", got, count)
			}
			for i := range count {
				wantValue(t, reopened, string(rune('a'+i)), value)
			}
			checkStatsAgainstReplay(t, reopened)
		})
	}
}

func TestStagedApplyUsesLatestLayoutAfterConcurrentCompaction(t *testing.T) {
	db := newTestDBWithHooks(t, testOptions(), nil, parkedMaintenance())
	mustPut(t, db, "before", "initial")
	first, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.ApplyStaged(t.Context(), func(yield func(MixedOp, error) bool) {
			if yield(MixedOp{Key: []byte("before"), Delete: true}, nil) {
				close(first)
				<-release
				yield(MixedOp{Key: []byte("progress"), Value: []byte("committed")}, nil)
			}
		})
	}()
	select {
	case <-first:
	case err := <-done:
		t.Fatalf("staging returned before the first record: %v", err)
	}
	released := false
	defer func() {
		if !released {
			close(release)
			<-done
		}
	}()
	err := db.ApplyStaged(t.Context(), func(func(MixedOp, error) bool) {
		t.Error("a competing staged stream was consumed")
	})
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("competing staged commit: %v", err)
	}
	mustPut(t, db, "concurrent", "keep")
	if _, err := db.CompactByKey(t.Context()); err != nil {
		t.Fatal(err)
	}
	close(release)
	released = true
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	db = openRefreshFixture(t, db)
	wantValue(t, db, "concurrent", "keep")
	wantValue(t, db, "progress", "committed")
	if _, err := db.Get([]byte("before")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("staged Delete after compaction: %v", err)
	}
	checkStatsAgainstReplay(t, db)
}

func TestStagedApplySelectorFailurePreservesWholeCommit(t *testing.T) {
	for _, tc := range []struct {
		point   string
		outcome Outcome
	}{
		{"staged.current.write:before", NotApplied},
		{"staged.current.rename:after", Unknown},
	} {
		t.Run(tc.point, func(t *testing.T) {
			armed := false
			fs := &storage.IO{Hook: func(point string) error {
				if armed && point == tc.point {
					armed = false
					return syscall.ENOSPC
				}
				return nil
			}}
			db := newTestDBWithHooks(t, testOptions(), fs, parkedMaintenance())
			mustPut(t, db, "a", "old-a")
			mustPut(t, db, "b", "old-b")
			if err := db.CheckpointIndex(t.Context()); err != nil {
				t.Fatal(err)
			}
			before := db.store.Current
			files := directoryBytes(t, db.store.Dir)
			prior := db.store.Catalog.Index
			value := strings.Repeat("v", 72)
			mutations := func(yield func(MixedOp, error) bool) {
				if !yield(MixedOp{Key: []byte("a"), Delete: true}, nil) {
					return
				}
				for _, key := range []string{"b", "c", "d"} {
					if !yield(MixedOp{Key: []byte(key), Value: []byte(value)}, nil) {
						return
					}
				}
			}

			armed = true
			err := db.ApplyStaged(t.Context(), mutations)
			var op *OpError
			if armed || !errors.Is(err, ErrNoSpace) || !errors.As(err, &op) || op.Outcome != tc.outcome {
				t.Fatalf("injected error returned %v, want %s", err, tc.outcome)
			}
			if tc.outcome == NotApplied {
				if db.health() != nil || db.store.Current != before || !maps.Equal(files, directoryBytes(t, db.store.Dir)) {
					t.Fatalf("unapplied commit changed health, frontier or files: %v", db.health())
				}
				old := openRefreshFixture(t, db)
				if got := old.RecoveryInfo().LastUserSequence; got != before.LastUserSequence {
					t.Fatalf("unapplied commit reopened at sequence %d", got)
				}
				wantValue(t, old, "a", "old-a")
				wantValue(t, old, "b", "old-b")
				if _, err := old.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
					t.Fatalf("old selection exposes new key: %v", err)
				}
				if err := db.ApplyStaged(t.Context(), mutations); err != nil {
					t.Fatalf("retry failed: %v", err)
				}
				checkStatsAgainstReplay(t, db)
			} else {
				if !errors.Is(err, ErrCommitUncertain) || !errors.Is(db.health(), ErrUnhealthy) {
					t.Fatalf("uncertain selection returned %v, health %v", err, db.health())
				}
			}

			reopened := openRefreshFixture(t, db)
			info := reopened.RecoveryInfo()
			if info.Path != RecoveryFastCheckpointReplay || info.CheckpointID != prior.ID || info.CoveredSequence != before.LastUserSequence ||
				info.LastUserSequence != before.LastUserSequence+4 {
				t.Fatalf("staged commit replay over earlier INDEX: %+v", info)
			}
			if _, err := reopened.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("reopen exposes removed key: %v", err)
			}
			for _, key := range []string{"b", "c", "d"} {
				wantValue(t, reopened, key, value)
			}
			checkStatsAgainstReplay(t, reopened)
		})
	}
}
