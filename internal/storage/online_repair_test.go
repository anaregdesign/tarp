package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index/memory"
	"github.com/anaregdesign/tarp/internal/publication"
)

// Done first runs when the follower selects on the leading repair and its own
// cancellation. This lets the test verify true overlap without timing sleeps.
type repairWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *repairWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func openedRepairFixture(t *testing.T) (repairFixture, *Store, *File) {
	t.Helper()
	fixture := newRepairFixture(t, 9000)
	s, _, err := Open(fixture.dir, Config{Handles: 2, RecordBytes: 1 << 20, ValueCacheBudgetBytes: 1 << 20, TargetSegmentBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := s.Files[s.Catalog.Files[0].ID]
	f.Pin()
	t.Cleanup(func() {
		f.Unpin()
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return fixture, s, f
}

func TestReadRepairSharesPublicationAndHandlesLaterDamage(t *testing.T) {
	fixture, s, f := openedRepairFixture(t)
	old, err := s.readers.acquire(f)
	if err != nil {
		t.Fatal(err)
	}
	defer s.readers.release(old)
	bad := bytes.Clone(fixture.originals[0])
	bad[128] ^= 1
	if err := os.WriteFile(f.Path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var verified, renames atomic.Int64
	s.IO = &IO{Hook: func(point string) error {
		switch point {
		case "repair.verify:before":
			verified.Add(1)
		case "repair.rename:before":
			if renames.Add(1) == 1 {
				close(entered)
				<-resume
			}
		}
		return nil
	}}
	leader := make(chan error, 1)
	go func() { leader <- s.RepairRead(t.Context(), f, ErrCorrupt) }()
	<-entered
	// Staging is private even across an unrelated live DB root sweep.
	names, err := filepath.Glob(filepath.Join(s.Dir, workspaceName, "tarp-repair-*"))
	if err != nil || len(names) != 1 {
		t.Fatalf("repair staging: %v, %v", names, err)
	}
	if _, _, err := s.Cleanup(); err != nil {
		t.Fatal(err)
	}
	ctx := &repairWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	follower := make(chan error, 1)
	go func() { follower <- s.RepairRead(ctx, f, syscall.EIO) }()
	<-ctx.waiting
	stop, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.RepairRead(stop, f, ErrCorrupt); err != context.Canceled {
		t.Fatalf("canceled follower: %v", err)
	}
	close(resume)
	if err := errors.Join(<-leader, <-follower); err != nil {
		t.Fatal(err)
	}
	if verified.Load() != 1 || renames.Load() != 1 {
		t.Fatalf("verifications=%d, renames=%d", verified.Load(), renames.Load())
	}
	var b [1]byte
	if _, err := old.fd.ReadAt(b[:], 128); err != nil || b[0] != bad[128] {
		t.Fatalf("old lease changed: %x %v", b, err)
	}
	fresh, err := s.readers.acquire(f)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fresh.fd.ReadAt(b[:], 128)
	s.readers.release(fresh)
	if err != nil || b[0] != fixture.originals[0][128] {
		t.Fatalf("fresh lease: %x %v", b, err)
	}
	// A prior success must not suppress a later repair on the same file.
	if err := os.Truncate(f.Path, 8500); err != nil {
		t.Fatal(err)
	}
	if err := s.RepairRead(t.Context(), f, io.ErrUnexpectedEOF); err != nil {
		t.Fatal(err)
	}
	if renames.Load() != 2 {
		t.Fatalf("second damage skipped: %d publications", renames.Load())
	}
	if got, err := os.ReadFile(f.Path); err != nil || !bytes.Equal(got, fixture.originals[0]) {
		t.Fatalf("wrong bytes after repair: %v", err)
	}
}

func TestReadRepairRetainsUncertainPublicationUntilReopen(t *testing.T) {
	_, s, f := openedRepairFixture(t)
	if err := os.Truncate(f.Path, 8500); err != nil {
		t.Fatal(err)
	}
	var starts int
	s.IO = &IO{Hook: func(point string) error {
		switch point {
		case "repair.verify:before":
			starts++
		case "repair.dirsync:before":
			return context.Canceled
		}
		return nil
	}}
	for _, cause := range []error{io.ErrUnexpectedEOF, syscall.EIO} {
		err := s.RepairRead(t.Context(), f, cause)
		var fault *Fault
		var repair *UncertainReadRepair
		if !errors.As(err, &repair) || !errors.As(repair, &fault) || fault.Outcome != publication.Unknown || !errors.Is(err, cause) || err == context.Canceled {
			t.Fatalf("uncertain replacement lost cause or outcome: %v", err)
		}
	}
	if starts != 1 {
		t.Fatalf("retried uncertain publication %d times", starts)
	}
}

func TestReadRepairCancellationCanHandOverToAnotherCaller(t *testing.T) {
	_, s, f := openedRepairFixture(t)
	if err := os.Truncate(f.Path, 8500); err != nil {
		t.Fatal(err)
	}
	started, resume := make(chan struct{}), make(chan struct{})
	leaderCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var starts atomic.Int64
	s.IO = &IO{Hook: func(point string) error {
		if point == "repair.verify:before" && starts.Add(1) == 1 {
			close(started)
			<-resume
		}
		return nil
	}}
	leader := make(chan error, 1)
	go func() { leader <- s.RepairRead(leaderCtx, f, ErrCorrupt) }()
	<-started
	ctx := &repairWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	follower := make(chan error, 1)
	go func() { follower <- s.RepairRead(ctx, f, ErrCorrupt) }()
	<-ctx.waiting
	cancel()
	close(resume)
	if err := <-leader; err != context.Canceled {
		t.Fatalf("leader cancellation: %v", err)
	}
	if err := <-follower; err != nil {
		t.Fatal(err)
	}
	if starts.Load() != 2 {
		t.Fatalf("leaders after cancellation: %d", starts.Load())
	}
}

func TestReadRepairRejectsUnpinnedActiveAndNonMediaFailures(t *testing.T) {
	_, s, f := openedRepairFixture(t)
	s.IO = &IO{Hook: func(point string) error { t.Fatalf("unexpected I/O: %s", point); return nil }}
	for _, cause := range []error{os.ErrPermission, errors.Join(ErrCorrupt, os.ErrPermission), &IndexWorkError{Err: syscall.EIO}} {
		if got := s.RepairRead(t.Context(), f, cause); got != cause {
			t.Fatalf("non-media error changed: %v", got)
		}
	}
	f.Unpin()
	got := s.RepairRead(t.Context(), f, ErrCorrupt)
	f.Pin()
	if got != ErrCorrupt {
		t.Fatalf("unpinned repair: %v", got)
	}
	active := s.Files[s.Current.ActiveID]
	active.Pin()
	got = s.RepairRead(t.Context(), active, ErrCorrupt)
	active.Unpin()
	if got != ErrCorrupt {
		t.Fatalf("active repair: %v", got)
	}
	s.IO = nil
}

func TestReadRepairsFreshlyInstalledCompactionOutput(t *testing.T) {
	for _, name := range []string{"checkpoint", "cleaner"} {
		t.Run(name, func(t *testing.T) {
			cfg := Config{Handles: 1, RecordBytes: 1024, ValueCacheBudgetBytes: 1024, TargetSegmentBytes: 4096}
			s, _, err := Open(t.TempDir(), cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			ready := func() error { return nil }
			old, err := s.Append(format.Put, []byte("k"), []byte("value"), ready)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Rotate(); err != nil {
				t.Fatal(err)
			}
			history := memory.New()
			defer history.Close()
			moves := s.NewRelocations()
			defer moves.Close()
			if name == "checkpoint" {
				live := memory.New()
				defer live.Close()
				tx := live.Begin()
				if err := tx.Put("k", old.Address()); err != nil {
					t.Fatal(err)
				}
				tx.Commit()
				view := live.View()
				defer view.Close()
				cap, err := s.Capture()
				if err != nil {
					t.Fatal(err)
				}
				defer cap.Release()
				work := s.NewCheckpoint(cap)
				if err := work.Copy(t.Context(), history, view, moves, s.ReserveSnapshotFile); err != nil {
					t.Fatal(err)
				}
				if err := work.Install(); err != nil {
					t.Fatal(err)
				}
			} else {
				work, err := s.BeginClean(s.Catalog.Files)
				if err != nil || work == nil {
					t.Fatalf("BeginClean: %v", err)
				}
				defer work.Release()
				if err := work.Copy(t.Context(), s.IO, moves, history, ready, func(string, Location) (bool, error) { return true, nil }); err != nil {
					t.Fatal(err)
				}
				if err := s.InstallCleaned(work, nil); err != nil {
					t.Fatal(err)
				}
			}
			entry := s.Catalog.Files[0]
			f := s.Files[entry.ID]
			if entry.ID == old.File.ID || f.Kind == format.Log {
				t.Fatalf("expected replacement, got %+v", entry)
			}
			f.Pin()
			defer f.Unpin()
			if err := s.EnsureParity(t.Context(), s.Current.UUID, CapturedFile{File: f, Entry: entry}, false, nil); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(f.Path); err != nil {
				t.Fatal(err)
			}
			if moves.count != 1 {
				t.Fatalf("got %d moves", moves.count)
			}
			err = moves.Each(func(key string, _, next Location) error {
				value, err := s.Read(next, key)
				if err != nil || string(value) != "value" {
					t.Fatalf("read newly installed DATA: %q, %v", value, err)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
