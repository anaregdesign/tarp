package engine

import (
	"context"
	"errors"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

// CompactResult reports the outcome of this Compact call's cleanup.
type CompactResult struct {
	UnlinkedBytes          int64 // Logical lengths of files unlinked during cleanup.
	PinnedRetiredDataBytes int64 // Logical lengths of retired DATA files still held by leases during cleanup.
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
	if err := ctx.Err(); err != nil {
		db.writer.Unlock()
		return result, operationError("compact", "wait", err)
	}
	if err := db.health(); err != nil {
		db.writer.Unlock()
		return result, err
	}
	if db.store.Current.Sequence == db.store.Catalog.Cutoff {
		n, pinned, err := db.store.Cleanup()
		db.writer.Unlock()
		return CompactResult{UnlinkedBytes: n, PinnedRetiredDataBytes: pinned}, compactCleanupError(err)
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
	live := db.indexView()
	db.writer.Unlock()
	released := false
	defer func() {
		if !released {
			capture.Release()
		}
	}()
	checkpoint := db.store.NewCheckpoint(capture)
	defer func() {
		if returned != nil && db.health() == nil {
			if err := checkpoint.Discard(); err != nil {
				returned = errors.Join(returned, err)
			}
		}
	}()
	reserve := func() (*storage.File, error) {
		db.writer.Lock()
		defer db.writer.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := db.health(); err != nil {
			return nil, err
		}
		return db.store.ReserveSnapshotFile()
	}
	moves := db.store.NewRelocations()
	defer func() {
		if err := moves.Close(); err != nil {
			if returned != nil {
				returned = errors.Join(returned, err)
			} else {
				returned = compactCleanupError(err)
			}
		}
	}()
	err = db.store.RetryMaintenance(ctx, capture.Files, func() error {
		history, err := db.newIndex()
		if err != nil {
			return &storage.IndexWorkError{Err: err}
		}
		copyErr := checkpoint.Copy(ctx, history, live, moves, reserve)
		if err := history.Close(); err != nil {
			return errors.Join(copyErr, &storage.IndexWorkError{Err: err})
		}
		return copyErr
	}, func() error {
		discardErr := errors.Join(checkpoint.Discard(), moves.Close())
		checkpoint = db.store.NewCheckpoint(capture)
		moves = db.store.NewRelocations()
		return discardErr
	})
	live.Close()
	if err != nil {
		db.compactFailure(err)
		return result, operationError("compact", "copy", err)
	}
	oldData, newData := checkpointDataTotals(capture, checkpoint.Entries())
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return result, operationError("compact", "pre-publication", err)
	}
	maxBatch := storage.RelocationBatchCount
	err = checkpoint.Install()
	if err == nil {
		db.resetParity()
		db.replaceDataStats(oldData, newData)
		if resident, ok := db.keys.(index.ResidentRelocator); ok && db.opts.IndexMode == IndexMemory {
			db.memoryRemap = &memoryCheckpointRemap{index: resident, staged: make(map[string]struct{})}
			maxBatch = 128 // The spool still caps a batch of small keys at 8 KiB.
		}
	}
	db.storageFailure(err)
	db.writer.Unlock()
	if err != nil {
		return result, operationError("compact", "publication", err)
	}
	db.relocate(moves, maxBatch, "compact.remap:after")
	capture.Release()
	released = true
	db.writer.Lock()
	defer db.writer.Unlock()
	db.memoryRemap = nil
	if err := db.health(); err != nil {
		return result, &OpError{Operation: "compact", Phase: "post-publication", Outcome: Committed, Err: err}
	}
	result.UnlinkedBytes, result.PinnedRetiredDataBytes, err = db.store.Cleanup()
	return result, compactCleanupError(err)
}

// The replacement layout is already committed. A partial remap fail-stops the
// instance, leaving the authoritative DATA for a fresh indexed Open.
func (db *DB) relocate(moves *storage.Relocations, maxBatch int, after string) {
	err := moves.EachBatch(maxBatch, func(batch []storage.Relocation) error {
		if err := db.relocateBatch(batch); err != nil {
			return err
		}
		if fs := db.store.IO; fs != nil && fs.Hook != nil {
			for range batch {
				if err := fs.Check(after); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		db.poison(err)
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
	var work *storage.IndexWorkError
	if errors.As(err, &source) || !errors.As(err, &work) && (errors.Is(err, format.ErrCorrupt) ||
		errors.Is(err, format.ErrUnsupported) || errors.Is(err, syscall.EIO)) {
		db.poison(err)
	}
}
