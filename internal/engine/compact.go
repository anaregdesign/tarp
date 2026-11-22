package engine

import (
	"context"
	"errors"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

// CompactResult reports the published checkpoint and its cleanup.
type CompactResult struct {
	DroppedKeys            uint64 // Unrecoverable live key versions omitted from the published checkpoint.
	UnlinkedBytes          int64  // Logical lengths of files unlinked during cleanup.
	PinnedRetiredDataBytes int64  // Retired DATA bytes retained by segment pins or live index references during cleanup.
}

// Compact builds a full DATA checkpoint through the captured user sequence.
// It requires temporary space and does not impose a total disk quota.
func (db *DB) Compact(ctx context.Context) (CompactResult, error) {
	var result CompactResult
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
	if !db.manualCheckpoint.CompareAndSwap(false, true) {
		return result, ErrBusy
	}
	db.cancelAutoRecovery()
	db.resumeCleaner()
	defer func() {
		db.manualCheckpoint.Store(false)
		db.notifyCleaner()
	}()
	select {
	case <-db.cleaner.gate:
		defer func() { db.cleaner.gate <- struct{}{} }()
	case <-ctx.Done():
		return result, operationError("compact", "wait", ctx.Err())
	}
	return db.compactCheckpoint(ctx, false)
}

var errCheckpointChanged = errors.New("a lost key changed during checkpoint recovery")

// The caller owns the cleaner gate. If a lost key changed after capture, the
// tail could start with its Delete. Capture again so replay always sees a valid
// predecessor, including when a Delete was followed by a Put of the same key.
func (db *DB) compactCheckpoint(ctx context.Context, force bool) (CompactResult, error) {
	for {
		result, err := db.checkpointAttempt(ctx, force)
		if err != errCheckpointChanged {
			return result, err
		}
	}
}

func (db *DB) checkpointAttempt(ctx context.Context, force bool) (result CompactResult, returned error) {
	db.writer.Lock()
	if err := ctx.Err(); err != nil {
		db.writer.Unlock()
		return result, operationError("compact", "wait", err)
	}
	if err := db.health(); err != nil {
		db.writer.Unlock()
		return result, err
	}
	if !force && db.store.Current.LastUserSequence == db.store.Catalog.CheckpointCutoffSequence && !db.hasUnavailableSource() {
		n, pinned, err := db.store.Cleanup()
		db.writer.Unlock()
		return CompactResult{UnlinkedBytes: n, PinnedRetiredDataBytes: pinned}, compactCleanupError(err)
	}
	if db.store.Current.ActiveCommittedEndOffset != format.SegmentHeaderBytes {
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
	checkpoint := db.store.NewDataCheckpoint(capture)
	defer func() {
		if returned != nil && db.health() == nil {
			if err := checkpoint.Discard(); err != nil {
				returned = errors.Join(returned, err)
			}
		}
	}()
	reserve := func() (*storage.DataSegment, error) {
		db.writer.Lock()
		defer db.writer.Unlock()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := db.health(); err != nil {
			return nil, err
		}
		return db.store.ReserveSnapshotSegment()
	}
	moves, lost := db.store.NewRelocations(), db.store.NewRelocations()
	defer func() {
		if err := errors.Join(moves.Close(), lost.Close()); err != nil {
			if returned != nil {
				returned = errors.Join(returned, err)
			} else {
				returned = compactCleanupError(err)
			}
		}
	}()
	reset := func() error {
		discardErr := errors.Join(checkpoint.Discard(), moves.Close())
		checkpoint = db.store.NewDataCheckpoint(capture)
		moves = db.store.NewRelocations()
		return discardErr
	}
	copyData := func(salvage bool) (err error) {
		history, err := db.newIndex()
		if err != nil {
			return &storage.IndexWorkError{Err: err}
		}
		defer func() {
			if closeErr := history.Close(); closeErr != nil {
				err = errors.Join(err, &storage.IndexWorkError{Err: closeErr})
			}
		}()
		if salvage {
			return checkpoint.CopyRecovered(ctx, history, live, moves, lost, reserve, db.cachedSource)
		}
		return checkpoint.Copy(ctx, history, live, moves, reserve)
	}
	err = db.store.RetryMaintenance(ctx, capture.Segments, func() error { return copyData(false) }, reset)
	if storage.IsDataUnavailable(err) && !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.EDQUOT) {
		if err = reset(); err == nil {
			err = copyData(true)
		}
	}
	live.Close()
	if err != nil {
		db.compactFailure(err)
		return result, operationError("compact", "copy", err)
	}
	if err := checkpoint.Protect(ctx); err != nil {
		return result, operationError("compact", "output parity", err)
	}
	releaseLossGuard, err := checkpoint.GuardLosses(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrLossSourceRepaired) {
			return result, errCheckpointChanged
		}
		var uncertain *storage.UncertainReadRepair
		if errors.As(err, &uncertain) {
			db.poison(err)
		}
		return result, operationError("compact", "confirm losses", err)
	}
	oldData, newData := checkpointDataTotals(capture, checkpoint.Entries())
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		releaseLossGuard()
		db.writer.Unlock()
		return result, operationError("compact", "pre-publication", err)
	}
	losses, err := db.prepareLosses(ctx, lost)
	if err != nil {
		if err == errCheckpointChanged && db.store.Current.LastUserSequence == capture.Current.LastUserSequence {
			// No foreground commit could have changed the captured address.
			err = errors.Join(format.ErrCorrupt, errors.New("checkpoint lost address disagrees with live index"))
			db.poison(err)
		}
		releaseLossGuard()
		db.writer.Unlock()
		if err == errCheckpointChanged {
			return result, err
		}
		return result, operationError("compact", "prepare losses", err)
	}
	maxBatch := storage.RelocationBatchCount
	if err = errors.Join(ctx.Err(), db.health()); err == nil {
		err = checkpoint.Install()
	}
	if err == nil {
		db.resetParity()
		result.DroppedKeys = db.publishLosses(losses, lost, oldData, newData)
		if resident, ok := db.keys.(index.ResidentRelocator); ok && db.opts.IndexMode == IndexMemory {
			db.memoryRemap = &memoryCheckpointRemap{index: resident, staged: make(map[string]struct{})}
			maxBatch = 128 // The spool still caps a batch of small keys at 8 KiB.
		}
	}
	if err != nil && losses != nil {
		losses.tx.Abort()
	}
	db.storageFailure(err)
	releaseLossGuard()
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

// cachedSource borrows immutable cache bytes only while the indexed version
// still has the captured address. Storage verifies them against the record CRC.
func (db *DB) cachedSource(key string, old storage.Location) ([]byte, bool, error) {
	db.indexGate.RLock()
	defer db.indexGate.RUnlock()
	current, ok, err := db.locationLocked(key)
	if err != nil || !ok || current != old {
		return nil, false, err
	}
	db.state.Lock()
	defer db.state.Unlock()
	value, ok := db.cache.Peek([]byte(key))
	return value, ok, nil
}

type lostFileTotals struct {
	keys, bytes int64
}

type checkpointLosses struct {
	tx    index.Tx
	files map[*storage.DataSegment]lostFileTotals
}

// Stage all deletions against the current index under the writer gate. No key
// becomes absent until CURRENT selects the replacement. Counts aggregated by
// file keep spilled loss lists bounded in BTree mode.
func (db *DB) prepareLosses(ctx context.Context, lost *storage.Relocations) (*checkpointLosses, error) {
	var losses *checkpointLosses
	err := lost.Each(func(key string, old, _ storage.Location) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if losses == nil {
			losses = &checkpointLosses{db.keys.Begin(), make(map[*storage.DataSegment]lostFileTotals)}
		}
		addr, found, err := losses.tx.Apply(index.Mutation{Key: key, Delete: true})
		if err != nil {
			var lookup *index.LookupError
			if errors.As(err, &lookup) {
				db.poison(lookup.Err)
			}
			return err
		}
		if !found || addr != old.Address() {
			return errCheckpointChanged
		}
		total := losses.files[old.Segment]
		total.keys++
		total.bytes += old.RecordBytes
		losses.files[old.Segment] = total
		return nil
	})
	if err != nil && losses != nil {
		losses.tx.Abort()
		return nil, err
	}
	return losses, err
}

// Publish the prepared index, invalidate cached/in-flight reads, and change
// both logical and physical gauges as one reader-visible state transition.
func (db *DB) publishLosses(losses *checkpointLosses, lost *storage.Relocations, old, next dataTotals) uint64 {
	if losses == nil {
		db.replaceDataStats(old, next)
		return 0
	}
	db.indexGate.Lock()
	defer db.indexGate.Unlock()
	db.state.Lock()
	defer db.state.Unlock()
	losses.tx.Commit()
	if batch, ok := lost.Resident(); ok {
		for _, move := range batch {
			db.cache.Delete(move.Key)
		}
	} else {
		// Avoid scratch I/O under the reader locks for a large damaged region.
		db.cache.Clear()
	}
	db.revision++
	var dropped uint64
	for segment, total := range losses.files {
		if segment.LiveKeys.Add(-total.keys) == 0 {
			delete(db.segments, segment.ID)
		}
		segment.LiveBytes.Add(-total.bytes)
		dropped += uint64(total.keys)
		db.stats.LiveRecordBytes -= uint64(total.bytes)
	}
	db.stats.LiveKeys -= dropped
	db.replaceDataStatsLocked(old, next)
	return dropped
}

// Only the selected catalog matters: a retired file may still be pinned.
func (db *DB) hasUnavailableSource() bool {
	for _, entry := range db.store.Catalog.Entries {
		if entry.HasPhysicalFile() && db.store.Segments[entry.ID].UnavailableData() != nil {
			return true
		}
	}
	return false
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
		db.readFailure(err)
	}
}
