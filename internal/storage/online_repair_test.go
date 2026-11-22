package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

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

func openedRepairFixture(t *testing.T) (repairFixture, *Store, *DataSegment) {
	t.Helper()
	fixture := newRepairFixture(t, 9000)
	s, err := openTestStore(t, fixture.dir, Config{Handles: 2, MaxRecordBytes: 1 << 20, ValueCacheBudgetBytes: 1 << 20, TargetSegmentBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f := s.Segments[s.Catalog.Entries[0].ID]
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
		case "data-repair.verify:before":
			verified.Add(1)
		case "data-repair.rename:before":
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
		case "data-repair.verify:before":
			starts++
		case "data-repair.dirsync:before":
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
		if point == "data-repair.verify:before" && starts.Add(1) == 1 {
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
	active := s.Segments[s.Current.ActiveID]
	active.Pin()
	got = s.RepairRead(t.Context(), active, ErrCorrupt)
	active.Unpin()
	if got != ErrCorrupt {
		t.Fatalf("active repair: %v", got)
	}
	s.IO = nil
}

func TestUnavailableReadThrottlesRepairAndAcceptsRestoredParity(t *testing.T) {
	fixture, s, f := openedRepairFixture(t)
	parity := filepath.Join(s.Dir, parityName(s.Current.UUID, f.ID))
	saved, err := os.ReadFile(parity)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(parity); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(f.Path, 8500); err != nil {
		t.Fatal(err)
	}
	var attempts int
	s.IO = &IO{Hook: func(point string) error {
		if point == "data-repair.verify:before" {
			attempts++
		}
		return nil
	}}
	first := fmt.Errorf("first record: %w", io.ErrUnexpectedEOF)
	err = s.RepairRead(t.Context(), f, first)
	var local *UnavailableDataError
	if !IsDataUnavailable(err) || !errors.As(err, &local) || local.FileID != f.ID ||
		!errors.Is(err, ErrDataUnavailable) || !errors.Is(err, first) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first failure: %v", err)
	}
	// Control expiry without relying on the test process being scheduled
	// again within the production cooldown.
	r := f.sealed.Load()
	r.mu.Lock()
	startedCooldown := !r.retryAfter.IsZero()
	r.retryAfter = time.Now().Add(time.Hour)
	r.mu.Unlock()
	if !startedCooldown {
		t.Fatal("missing repair cooldown")
	}
	info := f.UnavailableData()
	if info == nil || info.FileID != f.ID || errors.Is(info, first) {
		t.Fatalf("file status includes the wrong identity or a caller-specific error: %v", info)
	}
	info.FileID = 0
	err = s.RepairRead(t.Context(), f, syscall.EIO)
	if !IsDataUnavailable(err) || !errors.Is(err, syscall.EIO) || errors.Is(err, first) ||
		attempts != 1 || f.UnavailableData().FileID != f.ID {
		t.Fatalf("repeated read made %d repair attempts: %v", attempts, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.RepairRead(ctx, f, ErrCorrupt); err != context.Canceled {
		t.Fatalf("canceled read during cooldown: %v", err)
	}
	current := filepath.Join(s.Dir, "CURRENT")
	moved := filepath.Join(t.TempDir(), "frontier")
	if err := os.Rename(current, moved); err != nil {
		t.Fatal(err)
	}
	err = s.RepairRead(t.Context(), f, syscall.EIO)
	if IsDataUnavailable(err) || !errors.Is(err, syscall.EIO) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cooldown masked a missing root: %v", err)
	}
	if err := os.Rename(moved, current); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(parity, saved, 0600); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.retryAfter = time.Time{}
	r.mu.Unlock()
	if err := s.RepairRead(t.Context(), f, io.ErrUnexpectedEOF); err != nil || f.UnavailableData() != nil || attempts != 2 {
		t.Fatalf("retry after restoring parity: attempts %d, err %v, status %v", attempts, err, f.UnavailableData())
	}
	if b, err := os.ReadFile(f.Path); err != nil || !bytes.Equal(b, fixture.originals[0]) {
		t.Fatalf("recovered bytes: %v", err)
	}
}

func TestUnavailableReadRetriesOncePrivateStagingSpaceReturns(t *testing.T) {
	for _, tc := range []struct {
		point string
		cause error
	}{{"data-repair.write:before", syscall.ENOSPC}, {"data-repair.sync:before", syscall.EDQUOT}} {
		t.Run(tc.point, func(t *testing.T) {
			fixture, s, f := openedRepairFixture(t)
			b := bytes.Clone(fixture.originals[0])
			b[101] ^= 1
			if err := os.WriteFile(f.Path, b, 0600); err != nil {
				t.Fatal(err)
			}
			s.IO = &IO{Hook: func(point string) error {
				if point == tc.point {
					return tc.cause
				}
				return nil
			}}
			err := s.RepairRead(t.Context(), f, ErrCorrupt)
			if !IsDataUnavailable(err) || !errors.Is(err, tc.cause) {
				t.Fatalf("repair space error: %v", err)
			}
			if rem, err := filepath.Glob(filepath.Join(s.Dir, workspaceName, "tarp-repair-*")); err != nil || len(rem) != 0 {
				t.Fatalf("left private repair files: %v, %v", rem, err)
			}
			if got, err := os.ReadFile(f.Path); err != nil || !bytes.Equal(got, b) {
				t.Fatalf("original DATA changed on failed staging: %v", err)
			}
			s.IO = nil
			r := f.sealed.Load()
			r.mu.Lock()
			r.retryAfter = time.Time{}
			r.mu.Unlock()
			if err := s.RepairRead(t.Context(), f, ErrCorrupt); err != nil || f.UnavailableData() != nil {
				t.Fatalf("retry after space restored: %v, %v", err, f.UnavailableData())
			}
			if got, err := os.ReadFile(f.Path); err != nil || !bytes.Equal(got, fixture.originals[0]) {
				t.Fatalf("retry failed to replace DATA: %v", err)
			}
		})
	}
}

func TestReconstructionDistinguishesParityReadFromOutputMediaError(t *testing.T) {
	fixture, s, f := openedRepairFixture(t)
	entry := s.Catalog.Entries[0]
	fd, err := OpenRegular(filepath.Join(s.Dir, parityName(s.Current.UUID, f.ID)))
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	source := bytes.NewReader(fixture.originals[0])
	// Read the valid parity header, then fail within its stripe.
	parity := io.MultiReader(io.LimitReader(fd, 128), iotest.ErrReader(syscall.EIO))
	err = format.RecoverDataFromParity(t.Context(), source, parity, io.Discard, s.Current.UUID, entry)
	if !errors.Is(err, syscall.EIO) || !unavailableReconstruction(err) || unavailableReconstruction(&destinationError{syscall.EIO}) {
		t.Fatalf("media read classification: %v", err)
	}
}

func TestUnavailableReadRequiresVerifiedInputFailure(t *testing.T) {
	for _, mode := range []string{"two damaged blocks", "invalid parity", "staged write", "staged readback", "sync", "wrong root", "absent root"} {
		t.Run(mode, func(t *testing.T) {
			fixture, s, f := openedRepairFixture(t)
			parity := filepath.Join(s.Dir, parityName(s.Current.UUID, f.ID))
			b := bytes.Clone(fixture.originals[0])
			b[200] ^= 1
			if mode == "two damaged blocks" {
				b[6500] ^= 1
			}
			if err := os.WriteFile(f.Path, b, 0600); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "invalid parity":
				if err := os.Truncate(parity, 0); err != nil {
					t.Fatal(err)
				}
			case "staged write", "sync":
				at := map[string]string{"staged write": "data-repair.write:before", "sync": "data-repair.sync:before"}[mode]
				s.IO = &IO{Hook: func(point string) error {
					if point == at {
						return ErrCorrupt
					}
					return nil
				}}
			case "staged readback":
				s.IO = &IO{WriteAt: func(fd *os.File, p []byte, off int64) (int, error) {
					p = bytes.Clone(p)
					p[0] ^= 1
					return fd.WriteAt(p, off)
				}}
			case "wrong root":
				if err := os.Remove(parity); err != nil {
					t.Fatal(err)
				}
				current := s.Current
				current.UUID[0] ^= 1
				if err := os.WriteFile(filepath.Join(s.Dir, "CURRENT"), format.EncodeFrontier(current), 0600); err != nil {
					t.Fatal(err)
				}
			case "absent root":
				moved := filepath.Join(t.TempDir(), "old")
				if err := os.Rename(s.Dir, moved); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.Rename(moved, s.Dir); err != nil {
						t.Error(err)
					}
				}()
			}
			err := s.RepairRead(t.Context(), f, ErrCorrupt)
			wantLocal := mode == "two damaged blocks" || mode == "invalid parity"
			if !errors.Is(err, ErrCorrupt) || IsDataUnavailable(err) != wantLocal ||
				errors.Is(err, ErrDataUnavailable) != wantLocal || (f.UnavailableData() != nil) != wantLocal {
				t.Fatalf("classification: %v", err)
			}
			if mode == "invalid parity" && !errors.Is(err, format.ErrInvalidParity) {
				t.Fatalf("lost sidecar error: %v", err)
			}
		})
	}
}

func TestDataUnavailableClassificationPreservesIndependentFailures(t *testing.T) {
	err := &UnavailableDataError{FileID: 19, Err: ErrCorrupt}
	if !IsDataUnavailable(errors.Join(fmt.Errorf("copy: %w", err), err)) {
		t.Fatal("lost source identity")
	}
	for _, mixed := range []error{
		errors.Join(err, syscall.EIO), errors.Join(context.Canceled, err), &IndexWorkError{Err: err},
		&destinationError{err}, fault("publication", publication.Unknown, true, err), &UncertainReadRepair{Err: err},
	} {
		if IsDataUnavailable(mixed) {
			t.Fatalf("suppressed independent failure: %v", mixed)
		}
	}
}

func TestReadRepairsFreshlyInstalledCompactionOutput(t *testing.T) {
	for _, name := range []string{"checkpoint", "cleaner"} {
		t.Run(name, func(t *testing.T) {
			cfg := Config{Handles: 1, MaxRecordBytes: 1024, ValueCacheBudgetBytes: 1024, TargetSegmentBytes: 4096}
			s, err := openTestStore(t, t.TempDir(), cfg, nil)
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
				work := s.NewDataCheckpoint(cap)
				if err := work.Copy(t.Context(), history, view, moves, s.ReserveSnapshotSegment); err != nil {
					t.Fatal(err)
				}
				if err := work.Protect(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := work.Install(); err != nil {
					t.Fatal(err)
				}
			} else {
				work, err := s.BeginClean(s.Catalog.Entries)
				if err != nil || work == nil {
					t.Fatalf("BeginClean: %v", err)
				}
				defer work.Release()
				if err := work.Copy(t.Context(), s.IO, moves, history, ready, func(string, Location) (bool, error) { return true, nil }); err != nil {
					t.Fatal(err)
				}
				if err := s.ProtectCleaned(t.Context(), work, ready); err != nil {
					t.Fatal(err)
				}
				if err := s.InstallCleaned(work, nil); err != nil {
					t.Fatal(err)
				}
			}
			entry := s.Catalog.Entries[0]
			f := s.Segments[entry.ID]
			if entry.ID == old.Segment.ID || f.Kind == format.RawLogKind {
				t.Fatalf("expected replacement, got %+v", entry)
			}
			f.Pin()
			defer f.Unpin()
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
