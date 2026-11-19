package engine

import (
	"context"
	"errors"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// CompactResult counts logical removed file bytes and retired bytes still
// protected by leases. ReclaimedBytes is not a net filesystem-free-space delta.
type CompactResult struct {
	ReclaimedBytes int64
	PinnedBytes    int64
}

// Compact replaces the complete committed prefix with an online checkpoint.
// It requires temporary space and does not impose a total disk quota.
func (db *DB) Compact(ctx context.Context) (result CompactResult, returned error) {
	if err := db.admit(); err != nil {
		return result, err
	}
	defer db.done()
	if ctx == nil {
		return result, validContextError("compact")
	}
	if err := ctx.Err(); err != nil {
		return result, operationError("compact", "context", err)
	}
	if !db.compact.CompareAndSwap(false, true) {
		return result, ErrBusy
	}
	db.resumeCleaner()
	defer func() {
		db.compact.Store(false)
		db.notifyCleaner()
	}()
	select {
	case <-db.cleaner.gate:
		defer func() { db.cleaner.gate <- struct{}{} }()
	case <-ctx.Done():
		return result, operationError("compact", "wait", ctx.Err())
	}
	db.writer.Lock()
	if err := db.health(); err != nil {
		db.writer.Unlock()
		return result, err
	}
	if db.store.Current.Sequence == db.store.Catalog.Cutoff {
		n, pinned, err := db.store.Cleanup()
		db.writer.Unlock()
		return CompactResult{ReclaimedBytes: n, PinnedBytes: pinned}, compactCleanupError(err)
	}
	if db.store.Current.ActiveEnd != format.SegmentHeaderBytes {
		if err := db.sealActive(); err != nil {
			db.storageFailure(err)
			db.writer.Unlock()
			// Sealing can publish a new active log at the old cutoff.
			// The requested checkpoint has not reached publication.
			op := operationError("compact", "rotation", err)
			op.Outcome = NotApplied
			return result, op
		}
	}
	capture, err := db.store.Capture()
	if err != nil {
		db.poison(err)
		db.writer.Unlock()
		return result, operationError("compact", "capture", err)
	}
	db.writer.Unlock()
	released := false
	defer func() {
		if !released {
			capture.Release()
		}
	}()
	moves, err := capture.Replay(ctx, db.store.IO)
	if err != nil {
		db.compactFailure(err)
		return result, operationError("compact", "scan", err)
	}
	checkpoint := db.store.NewCheckpoint(capture)
	defer func() {
		if returned != nil && db.health() == nil {
			if err := checkpoint.Discard(); err != nil {
				returned = errors.Join(returned, err)
			}
		}
	}()
	if err := checkpoint.Copy(ctx, moves, db.reserveSnapshotFile); err != nil {
		db.compactFailure(err)
		return result, operationError("compact", "copy", err)
	}
	oldData, newData := checkpointDataTotals(capture, checkpoint.Entries())
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return result, operationError("compact", "pre-publication", err)
	}
	err = checkpoint.Install()
	if err == nil {
		db.replaceDataStats(oldData, newData)
	}
	db.storageFailure(err)
	db.writer.Unlock()
	if err != nil {
		return result, operationError("compact", "publication", err)
	}
	db.relocate(moves, "compact.remap:after")
	capture.Release()
	released = true
	db.writer.Lock()
	defer db.writer.Unlock()
	if err := db.health(); err != nil {
		return result, &OpError{Operation: "compact", Phase: "post-publication", Outcome: Committed, Err: err}
	}
	result.ReclaimedBytes, result.PinnedBytes, err = db.store.Cleanup()
	return result, compactCleanupError(err)
}

func (db *DB) reserveSnapshotFile() (*storage.File, error) {
	db.writer.Lock()
	defer db.writer.Unlock()
	if err := db.health(); err != nil {
		return nil, err
	}
	return db.store.ReserveSnapshotFile()
}

// relocate moves index ownership only while it still names the copied record.
// Each change holds the state lock briefly; concurrent puts and deletes win.
func (db *DB) relocate(moves []storage.Relocation, after string) {
	for _, move := range moves {
		db.state.Lock()
		if db.index[move.Key] == move.Old {
			move.New.File.Pin()
			move.New.File.LiveBytes.Add(move.New.Bytes)
			db.index[move.Key] = move.New
			move.Old.File.LiveBytes.Add(-move.Old.Bytes)
			move.Old.File.Unpin()
		}
		db.state.Unlock()
		if err := db.store.IO.Check(after); err != nil {
			db.poison(err)
		}
	}
}

func compactCleanupError(err error) error {
	if err == nil {
		return nil
	}
	return &OpError{Operation: "compact", Phase: "cleanup", Outcome: Committed, Err: publicCause(err)}
}

func (db *DB) storageFailure(err error) {
	var failed *storage.Fault
	if errors.As(err, &failed) && failed.Unsafe {
		db.poison(err)
	}
}

func (db *DB) compactFailure(err error) {
	var source *storage.SourceError
	if errors.As(err, &source) || errors.Is(err, format.ErrCorrupt) ||
		errors.Is(err, format.ErrUnsupported) || errors.Is(err, syscall.EIO) {
		db.poison(err)
	}
}
