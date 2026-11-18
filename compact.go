package tarp

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
			return result, operationError("compact", "rotation", err)
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
	index, err := capture.Replay(ctx, db.store.IO)
	if err != nil {
		db.compactFailure(err)
		return result, operationError("compact", "scan", err)
	}
	type relocation struct{ old, new storage.Location }
	moves := make(map[string]relocation, len(index))
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
	for key, loc := range index {
		if err := ctx.Err(); err != nil {
			return result, operationError("compact", "copy", err)
		}
		if fd == nil || offset > db.opts.MaxDataFileBytes-loc.Bytes {
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
		if err := db.store.CopyRecord(ctx, loc, key, fd, offset, buffer); err != nil {
			db.compactFailure(err)
			return result, operationError("compact", "copy", err)
		}
		next := storage.Location{File: active, Offset: offset, Bytes: loc.Bytes, ValueBytes: loc.ValueBytes, Sequence: loc.Sequence}
		moves[key] = relocation{old: loc, new: next}
		offset += loc.Bytes
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
	for key, move := range moves {
		db.state.Lock()
		current, exists := db.index[key]
		if exists && current.Sequence == move.old.Sequence && current.File == move.old.File && current.Offset == move.old.Offset {
			move.new.File.Pin()
			move.new.File.LiveBytes.Add(move.new.Bytes)
			current.File.LiveBytes.Add(-current.Bytes)
			db.index[key] = move.new
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
