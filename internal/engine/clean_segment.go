package engine

import (
	"context"
	"errors"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Clean one immutable file or repack a contiguous group. Foreground mutations
// may continue during the scan and during conditional index relocation.
func (db *DB) cleanSegment(ctx context.Context, entries ...format.File) (result error) {
	db.writer.Lock()
	if err := db.health(); err != nil {
		db.writer.Unlock()
		return err
	}
	work, err := db.store.BeginClean(entries)
	db.writer.Unlock()
	if err != nil {
		db.compactFailure(err)
		return operationError("clean", "select", err)
	}
	if work == nil {
		return nil
	}
	defer work.Release()
	keepOutput := false
	defer func() {
		if keepOutput {
			return
		}
		if err := work.Discard(db.store.IO); err != nil {
			db.writer.Lock()
			db.store.QueueFailedCleanDiscard(work)
			db.writer.Unlock()
			result = errors.Join(result, err)
		}
	}()

	moves := db.store.NewRelocations()
	defer func() {
		if closeErr := moves.Close(); closeErr != nil {
			if result != nil {
				// Preserve the primary operation's publication outcome.
				result = errors.Join(result, closeErr)
			} else {
				result = &OpError{Operation: "clean", Phase: "scratch cleanup", Outcome: Committed, Err: publicCause(closeErr)}
			}
		}
	}()
	err = func() (result error) {
		var history index.Backend
		if entries[0].Kind == format.Snapshot {
			var err error
			history, err = db.newIndex()
			if err != nil {
				return &storage.IndexWorkError{Err: err}
			}
			defer func() {
				if err := history.Close(); err != nil {
					result = errors.Join(result, &storage.IndexWorkError{Err: err})
				}
			}()
		}
		return work.Copy(ctx, db.store.IO, moves, history, func() error { return db.cleanYield(ctx) }, func(key string, old storage.Location) (bool, error) {
			current, exists, err := db.lookupLocation(key)
			if err != nil {
				return false, &storage.SourceError{Err: err}
			}
			return exists && current == old, nil
		})
	}()
	if err != nil {
		db.compactFailure(err)
		return operationError("clean", "copy", err)
	}
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return operationError("clean", "pre-publication", err)
	}
	// Prepare the still-unselected immutable manifest outside the writer gate.
	err = db.store.InstallCleaned(work, func(prepare func() error) error {
		db.writer.Unlock()
		err := prepare()
		db.writer.Lock()
		if err != nil {
			return err
		}
		if db.health() != nil {
			// A failed foreground CURRENT can be uncertain. This cleaner has
			// not attempted its own CURRENT and may discard its staging files.
			return ErrUnhealthy
		}
		return ctx.Err()
	})
	if err != nil {
		var fault *storage.Fault
		if errors.As(err, &fault) && fault.Outcome == Unknown {
			keepOutput = true
		}
		// An unpublished staging-space failure is retryable. A doubtful
		// CURRENT must stop all publishers before this writer gate opens.
		publicErr := operationError("clean", "publication", err)
		if keepOutput || !errors.Is(publicErr, ErrNoSpace) {
			db.storageFailure(err)
			db.compactFailure(err)
		}
		db.writer.Unlock()
		return publicErr
	}
	keepOutput = true
	db.resetParity()
	// Cleaning retains every tombstone. Replace only the physical file counts
	// and sizes, including a possible metadata-only cleaned interval.
	var oldData, newData dataTotals
	for _, source := range entries {
		if source.HasPhysicalFile() {
			oldData.addFile(source.Bytes, 0)
		}
	}
	if n := work.OutputBytes(); n > 0 {
		newData.addFile(n, 0)
	}
	db.replaceDataStats(oldData, newData)
	db.writer.Unlock()

	db.relocate(moves, storage.RelocationBatchCount, "clean.remap:after")
	if err := db.health(); err != nil {
		return &OpError{Operation: "clean", Phase: "post-publication", Outcome: Committed, Err: err}
	}
	return nil
}
