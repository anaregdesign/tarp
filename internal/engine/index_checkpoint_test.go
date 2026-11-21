package engine

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestCheckpointIndexFailureOutcomeAndReopen(t *testing.T) {
	for _, tc := range []struct {
		point   string
		outcome Outcome
	}{
		{"index.write:before", NotApplied},
		{"index.sync:after", NotApplied},
		{"index.rename:before", NotApplied},
		{"index.rename:after", NotApplied},
		{"index.dirsync:after", NotApplied},
		{"index.catalog.write:after", NotApplied},
		{"index.catalog.sync:after", NotApplied},
		{"index.current.write:before", NotApplied},
		{"index.current.rename:before", Unknown},
		{"index.current.rename:after", Unknown},
		{"index.current.dirsync:after", Unknown},
		{"clean.retire.unlink:before", Committed},
	} {
		t.Run(tc.point, func(t *testing.T) {
			var armed atomic.Bool
			fs := &storage.IO{Hook: func(p string) error {
				if p == tc.point && armed.CompareAndSwap(true, false) {
					return syscall.ENOSPC
				}
				return nil
			}}
			dir := t.TempDir()
			db, err := OpenWithHooks(dir, Options{}, fs, &Hooks{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mustPut(t, db, "first", "one")
			if err := db.CheckpointIndex(t.Context()); err != nil {
				t.Fatal(err)
			}
			old := db.store.Catalog.Index.ID
			mustPut(t, db, "second", "two")
			armed.Store(true)
			err = db.CheckpointIndex(t.Context())
			var op *OpError
			if armed.Load() || !errors.As(err, &op) || op.Outcome != tc.outcome || !errors.Is(err, ErrNoSpace) {
				t.Fatalf("fault result: %v", err)
			}
			if tc.outcome == NotApplied && old != db.store.Catalog.Index.ID {
				t.Fatal("failed checkpoint replaced old selection")
			}
			if tc.outcome == Unknown {
				if !errors.Is(err, ErrCommitUncertain) || !errors.Is(db.health(), ErrUnhealthy) {
					t.Fatalf("uncertain result: %v, %v", err, db.health())
				}
			} else if err := db.CheckpointIndex(t.Context()); err != nil {
				t.Fatalf("retry: %v", err)
			}
			_ = db.Close()
			reopened, err := Open(dir, Options{OpenValidation: Indexed})
			if err != nil {
				t.Fatal(err)
			}
			wantValue(t, reopened, "first", "one")
			wantValue(t, reopened, "second", "two")
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckpointIndexConcurrentRotationPreservesTail(t *testing.T) {
	for _, point := range []string{"index.scan:before", "index.dirsync:before"} {
		t.Run(point, func(t *testing.T) {
			scanning, resume := make(chan struct{}), make(chan struct{})
			var arm atomic.Bool
			fs := &storage.IO{Hook: func(p string) error {
				if p == point && arm.CompareAndSwap(true, false) {
					close(scanning)
					<-resume
				}
				return nil
			}}
			dir := t.TempDir()
			opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: 128, OpenValidation: Indexed}
			db, err := OpenWithHooks(dir, opts, fs, &Hooks{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			mustPut(t, db, "a", "original")
			arm.Store(true)
			done := make(chan error, 1)
			go func() { done <- db.CheckpointIndex(t.Context()) }()
			<-scanning
			if err := db.CheckpointIndex(t.Context()); !errors.Is(err, ErrBusy) {
				close(resume)
				t.Fatalf("second checkpoint: %v", err)
			}
			if point == "index.dirsync:before" {
				db.writer.Lock()
				_, _, err := db.store.Cleanup()
				db.writer.Unlock()
				if err != nil {
					close(resume)
					t.Fatal(err)
				}
			}
			mustPut(t, db, "a", "tail")
			mustPut(t, db, "b", "second")
			close(resume)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if db.store.Catalog.Index.Sequence != 1 || db.store.Current.Sequence != 3 {
				t.Fatalf("frontiers %d %d", db.store.Catalog.Index.Sequence, db.store.Current.Sequence)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = OpenWithHooks(dir, opts, nil, &Hooks{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if !db.store.IndexCheckpointHit {
				t.Fatal("miss on rotated suffix")
			}
			wantValue(t, db, "a", "tail")
			wantValue(t, db, "b", "second")
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := db.CheckpointIndex(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled checkpoint: %v", err)
			}
		})
	}
}

func TestFailedAutomaticIndexCheckpointReleasesLock(t *testing.T) {
	dir := t.TempDir()
	opts := Options{IndexMode: IndexBTree}
	fs := &storage.IO{Hook: func(p string) error {
		if p == "index.sync:before" {
			return syscall.ENOSPC
		}
		return nil
	}}
	db, err := OpenWithIO(dir, opts, fs)
	if err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "k", "v")
	if err := db.Close(); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, reopened, "k", "v")
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
