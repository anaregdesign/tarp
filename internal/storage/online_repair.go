package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
)

// Readers can still hold a pinned file after a newer catalog replaces it.
// Bind its trusted extent once; a repair never consults mutable catalog state.
type sealedRepair struct {
	uuid  [16]byte
	entry format.CatalogEntry

	mu               sync.Mutex
	running          *readRepair
	uncertain        error
	repairGeneration atomic.Uint64 // successful full-file verifications or repairs
	unavailable      atomic.Pointer[UnavailableDataError]
	retryAfter       time.Time
}

type readRepair struct {
	done chan struct{}
	err  error
}

var ErrDataUnavailable = errors.New("tarp: sealed DATA unavailable")

// UnavailableDataError identifies a damaged sealed file that could not be
// restored from its recovery material. Err retains the read and recovery causes.
type UnavailableDataError struct {
	FileID uint64
	Err    error
}

func (e *UnavailableDataError) Error() string {
	return fmt.Sprintf("%s (file %d): %v", ErrDataUnavailable, e.FileID, e.Err)
}
func (e *UnavailableDataError) Unwrap() error        { return e.Err }
func (e *UnavailableDataError) Is(target error) bool { return target == ErrDataUnavailable }

// IsDataUnavailable permits continued service only when every failure in
// the result is a known file-local error.
func IsDataUnavailable(err error) bool {
	for err != nil {
		switch e := err.(type) {
		case *UnavailableDataError:
			return true
		case *Fault, *UncertainReadRepair, *IndexWorkError, *destinationError:
			return false
		case interface{ Unwrap() []error }:
			causes := e.Unwrap()
			for _, cause := range causes {
				if !IsDataUnavailable(cause) {
					return false
				}
			}
			return len(causes) > 0
		default:
			err = errors.Unwrap(err)
		}
	}
	return false
}

// UnavailableData returns the last confirmed physical failure. Values whose
// record checks pass can still be read from the same file.
func (f *DataSegment) UnavailableData() *UnavailableDataError {
	r := f.sealed.Load()
	if r == nil {
		return nil
	}
	local := r.unavailable.Load()
	if local == nil {
		return nil
	}
	saved := *local
	return &saved
}

// UncertainReadRepair retains the original read failure and the publication
// fault when the durability of a sealed DATA replacement is unknown.
type UncertainReadRepair struct{ Err error }

func (e *UncertainReadRepair) Error() string { return e.Err.Error() }
func (e *UncertainReadRepair) Unwrap() error { return e.Err }

func (f *DataSegment) markSealed(uuid [16]byte, entry format.CatalogEntry) {
	f.sealed.CompareAndSwap(nil, &sealedRepair{uuid: uuid, entry: entry})
}

// RepairRead checks one eligible physical read failure against the pinned
// file's immutable extent. A nil result authorizes exactly one checked retry;
// it never certifies the failed record, a caller's index or any cached value.
func (s *Store) RepairRead(ctx context.Context, f *DataSegment, cause error) error {
	if cause == nil {
		return errors.New("storage: read repair requires a source failure")
	}
	if f == nil || f.pins.Load() <= 0 || !repairableRead(cause) {
		return cause
	}
	r := f.sealed.Load()
	if r == nil || r.entry.ID != f.ID || f.Path != filepath.Join(s.Dir, DataName(r.uuid, f.ID)) {
		return cause
	}
	err := r.run(ctx, s.DamageChanged, func() error { return s.checkRepairRoot(r.uuid) }, func() error {
		if err := s.IO.Check("data-repair.verify:before"); err != nil {
			return err
		}
		if _, err := s.repairSealedData(ctx, r.uuid, r.entry, true); err != nil {
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
	if local, ok := err.(*UnavailableDataError); ok {
		return &UnavailableDataError{FileID: local.FileID, Err: errors.Join(cause, local.Err)}
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

// Share attempts in progress and briefly suppress repeated full-file recovery
// of confirmed damage. An operator may later restore the recovery material.
// A successful verification cannot be cached across distinct failures.
// A directory publication with unknown durability can only be resolved by reopen.
func (r *sealedRepair) run(ctx context.Context, damageChanged func(), checkRoot, repair func() error) error {
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
		if local := r.unavailable.Load(); local != nil && time.Now().Before(r.retryAfter) {
			r.mu.Unlock()
			// A new fault may affect the entire DB, including a caller
			// still reading a bad inode after the directory disappeared.
			if err := checkRoot(); err != nil {
				return errors.Join(local.Err, err)
			}
			return local
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
		if call.err == nil {
			r.repairGeneration.Add(1)
		}
		prior := r.unavailable.Load()
		if local, ok := call.err.(*UnavailableDataError); ok {
			r.unavailable.Store(local)
			r.retryAfter = time.Now().Add(time.Second)
		} else if call.err == nil || r.uncertain != nil {
			r.unavailable.Store(nil)
		}
		next := r.unavailable.Load()
		// A cached read failure cannot schedule the same expensive checkpoint
		// again. Reconsider the work when loss is first observed or a formerly
		// blocked file can be fully read again.
		newGap := next != nil && errors.Is(next.Err, format.ErrUnrecoverableDataBlocks) &&
			(prior == nil || !errors.Is(prior.Err, format.ErrUnrecoverableDataBlocks))
		if damageChanged != nil && (newGap || prior != nil && next == nil) {
			damageChanged()
		}
		r.running = nil
		close(call.done)
		r.mu.Unlock()
		return call.err
	}
}
