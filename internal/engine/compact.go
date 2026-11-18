package engine

import (
	"context"
	"errors"
	"os"
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
		if err := db.store.Rotate(); err != nil {
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
	var reader storage.SnapshotReader
	var files []*storage.File
	var outputs []format.File
	var fd *os.File
	var active *storage.File
	offset := int64(format.SegmentHeaderBytes)
	attempted := false
	finishFile := func() error {
		if fd == nil {
			return nil
		}
		err := errors.Join(db.store.IO.Sync(fd, "compact.sync"), fd.Close())
		fd = nil
		if err == nil {
			active.Size.Store(offset)
			outputs = append(outputs, format.File{ID: active.ID, Bytes: offset, Kind: format.Snapshot})
		}
		return err
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			db.compactFailure(closeErr)
			returned = errors.Join(returned, operationError("compact", "source-close", closeErr))
		}
		if fd != nil {
			returned = errors.Join(returned, fd.Close())
		}
		if returned != nil && !attempted && db.health() == nil {
			db.writer.Lock()
			if db.health() == nil {
				returned = errors.Join(returned, db.store.DiscardOutputs(files))
			}
			db.writer.Unlock()
		}
	}()
	buffer := make([]byte, 32<<10)
	for i := range moves {
		move := &moves[i]
		loc := move.Old
		if err := ctx.Err(); err != nil {
			return result, operationError("compact", "copy", err)
		}
		if fd == nil || offset > db.opts.TargetSegmentBytes-loc.Bytes {
			if err := finishFile(); err != nil {
				db.compactFailure(err)
				return result, operationError("compact", "output-sync", err)
			}
			db.writer.Lock()
			if err := db.health(); err != nil {
				db.writer.Unlock()
				return result, err
			}
			active, fd, err = db.store.NewSnapshotFile()
			db.writer.Unlock()
			if err != nil {
				db.compactFailure(err)
				return result, operationError("compact", "output-create", err)
			}
			files = append(files, active)
			offset = format.SegmentHeaderBytes
		}
		if err := reader.CopyRecord(ctx, db.store.IO, loc, move.Key, fd, offset, buffer); err != nil {
			db.compactFailure(err)
			return result, operationError("compact", "copy", err)
		}
		move.New = loc
		move.New.File, move.New.Offset = active, offset
		offset += loc.Bytes
	}
	if err := reader.Close(); err != nil {
		db.compactFailure(err)
		return result, operationError("compact", "source-close", err)
	}
	if err := finishFile(); err != nil {
		db.compactFailure(err)
		return result, operationError("compact", "output-sync", err)
	}
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return result, operationError("compact", "pre-publication", err)
	}
	attempted = true
	err = db.store.InstallCheckpoint(capture, outputs)
	db.storageFailure(err)
	db.writer.Unlock()
	if err != nil {
		return result, operationError("compact", "publication", err)
	}
	for _, move := range moves {
		db.state.Lock()
		current, exists := db.index[move.Key]
		if exists && current.Sequence == move.Old.Sequence && current.File == move.Old.File && current.Offset == move.Old.Offset {
			move.New.File.Pin()
			move.New.File.LiveBytes.Add(move.New.Bytes)
			current.File.LiveBytes.Add(-current.Bytes)
			db.index[move.Key] = move.New
			current.File.Unpin()
		}
		db.state.Unlock()
		if err := db.store.IO.Check("compact.remap:after"); err != nil {
			db.poison(err)
		}
	}
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
