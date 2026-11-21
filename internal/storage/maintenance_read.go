package storage

import (
	"context"
	"errors"
)

// A dataReadError identifies the physical input. The file may become sealed
// while an operation is reading a pinned, previously active prefix.
type dataReadError struct {
	id  uint64
	err error
}

func (e *dataReadError) Error() string { return e.err.Error() }
func (e *dataReadError) Unwrap() error { return e.err }

func selectedReadFailure(id uint64, err error) error {
	if err == nil || id == 0 || !repairableRead(err) {
		return err
	}
	var source *SourceError
	if !errors.As(err, &source) {
		err = &SourceError{Err: err}
	}
	return &dataReadError{id, err}
}

// Require an unambiguous file error. A separate stage or close failure must
// reach the caller, even if a read of sealed DATA failed in the same attempt.
func failedDataRead(err error) *dataReadError {
	for err != nil {
		switch e := err.(type) {
		case *dataReadError:
			return e
		case interface{ Unwrap() []error }:
			causes := e.Unwrap()
			if len(causes) != 1 {
				return nil
			}
			err = causes[0]
		default:
			err = errors.Unwrap(err)
		}
	}
	return nil
}

// RetryMaintenance reruns private work against the same pinned input.
// A clean pass performs no additional DATA reads. Repair is attempted at most
// once per damaged DATA file, and only before the operation has published.
func (s *Store) RetryMaintenance(ctx context.Context, sources []CapturedFile, run func() error, reset func() error) error {
	var attempted map[uint64]bool
	for {
		err := run()
		read := failedDataRead(err)
		if read == nil || attempted[read.id] {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var file *File
		for _, src := range sources {
			if src.Entry.ID == read.id {
				file = src.File
				break
			}
		}
		if file == nil {
			return err
		}
		if err := s.RepairRead(ctx, file, err); err != nil {
			return err
		}
		if reset != nil {
			if err := reset(); err != nil {
				return err
			}
		}
		if attempted == nil {
			attempted = make(map[uint64]bool)
		}
		attempted[read.id] = true
	}
}
