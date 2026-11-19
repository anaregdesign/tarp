package engine

import (
	"context"
	"errors"

	"github.com/anaregdesign/tarp/internal/format"
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
		if !keepOutput {
			result = errors.Join(result, work.Discard(db.store.IO))
		}
	}()

	moves, err := work.Copy(ctx, db.store.IO, func() error { return db.cleanYield(ctx) }, func(key string, old storage.Location) bool {
		db.state.Lock()
		defer db.state.Unlock()
		return db.index[key] == old
	})
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
		if errors.As(err, &fault) && fault.Outcome == string(Unknown) {
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
	// Cleaning retains every tombstone. Replace only the physical file counts
	// and sizes, including a possible metadata-only cleaned interval.
	var oldData, newData dataTotals
	for _, source := range entries {
		if source.ID != 0 {
			oldData.addFile(source.Bytes, 0)
		}
	}
	if n := work.OutputBytes(); n > 0 {
		newData.addFile(n, 0)
	}
	db.replaceDataStats(oldData, newData)
	db.writer.Unlock()

	for _, m := range moves {
		db.state.Lock()
		if db.index[m.Key] == m.Old {
			m.New.File.Pin()
			m.New.File.LiveBytes.Add(m.New.Bytes)
			db.index[m.Key] = m.New
			m.Old.File.LiveBytes.Add(-m.Old.Bytes)
			m.Old.File.Unpin()
		}
		db.state.Unlock()
		if err := db.store.IO.Check("clean.remap:after"); err != nil {
			db.poison(err)
		}
	}
	if err := db.health(); err != nil {
		return &OpError{Operation: "clean", Phase: "post-publication", Outcome: Committed, Err: err}
	}
	return nil
}
