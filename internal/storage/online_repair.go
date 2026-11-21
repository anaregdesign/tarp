package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
)

// Readers can still hold a pinned file after a newer catalog replaces it.
// Bind its trusted extent once; a repair never consults mutable catalog state.
type sealedRepair struct {
	uuid  [16]byte
	entry format.File

	mu        sync.Mutex
	running   *readRepair
	uncertain error
}

type readRepair struct {
	done chan struct{}
	err  error
}

// UncertainReadRepair retains the original read failure and the publication
// fault when the durability of a sealed DATA replacement is unknown.
type UncertainReadRepair struct{ Err error }

func (e *UncertainReadRepair) Error() string { return e.Err.Error() }
func (e *UncertainReadRepair) Unwrap() error { return e.Err }

func (f *File) markSealed(uuid [16]byte, entry format.File) {
	f.sealed.CompareAndSwap(nil, &sealedRepair{uuid: uuid, entry: entry})
}

// RepairRead checks one eligible physical read failure against the pinned
// file's immutable extent. A nil result authorizes exactly one checked retry;
// it never certifies the failed record, a caller's index or any cached value.
func (s *Store) RepairRead(ctx context.Context, f *File, cause error) error {
	if cause == nil {
		return errors.New("storage: read repair requires a source failure")
	}
	if f == nil || f.refs.Load() <= 0 || !repairableRead(cause) {
		return cause
	}
	r := f.sealed.Load()
	if r == nil || r.entry.ID != f.ID || f.Path != filepath.Join(s.Dir, DataName(r.uuid, f.ID)) {
		return cause
	}
	err := r.run(ctx, func() error {
		if err := s.IO.Check("repair.verify:before"); err != nil {
			return err
		}
		if _, err := s.repairSealed(ctx, r.uuid, r.entry, true); err != nil {
			return err
		}
		// Another completed publication may already have replaced the path.
		// All subsequent reads must acquire the verified inode, while leases
		// on an older inode remain valid until their own read returns.
		return s.readers.invalidate(f)
	})
	if err == nil {
		return nil
	}
	var fault *Fault
	if errors.As(err, &fault) {
		if fault.Outcome == publication.Unknown {
			return &UncertainReadRepair{Err: errors.Join(cause, err)}
		}
	} else {
		if onlyCauses(err, context.Canceled) {
			return context.Canceled
		}
		if onlyCauses(err, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
	}
	return errors.Join(cause, err)
}

func repairableRead(err error) bool {
	var fault *Fault
	var work *IndexWorkError
	var destination *destinationError
	if errors.As(err, &fault) || errors.As(err, &work) || errors.As(err, &destination) || errors.Is(err, format.ErrInvalidParity) {
		return false
	}
	return onlyCauses(err, ErrCorrupt, syscall.EIO, io.EOF, io.ErrUnexpectedEOF, os.ErrNotExist)
}

// Share attempts in progress, but never cache a successful verification across
// distinct failures: a previously repaired file can suffer new media damage.
// A directory publication with unknown durability can only be resolved by reopen.
func (r *sealedRepair) run(ctx context.Context, repair func() error) error {
	for {
		r.mu.Lock()
		if r.uncertain != nil {
			err := r.uncertain
			r.mu.Unlock()
			return err
		}
		if err := ctx.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
		if call := r.running; call != nil {
			r.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-call.done:
			}
			if onlyCauses(call.err, context.Canceled, context.DeadlineExceeded) {
				// The leading operation was canceled; a live caller can lead.
				continue
			}
			return call.err
		}
		call := &readRepair{done: make(chan struct{})}
		r.running = call
		r.mu.Unlock()

		call.err = repair()
		r.mu.Lock()
		var f *Fault
		if errors.As(call.err, &f) && f.Outcome == publication.Unknown {
			r.uncertain = call.err
		}
		r.running = nil
		close(call.done)
		r.mu.Unlock()
		return call.err
	}
}
