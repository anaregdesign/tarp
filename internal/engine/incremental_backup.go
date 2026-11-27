package engine

import (
	"context"
	"errors"

	"github.com/anaregdesign/tarp/internal/storage"
)

type IncrementalBackupRequest = storage.IncrementalBackupRequest
type IncrementalBackupResult = storage.IncrementalBackupResult

func (db *DB) BackupIncremental(ctx context.Context, req IncrementalBackupRequest) (IncrementalBackupResult, error) {
	const op = "incremental-backup"
	var result IncrementalBackupResult
	if ctx == nil {
		return result, validContextError(op)
	}
	if err := ctx.Err(); err != nil {
		return result, operationError(op, "context", err)
	}
	if err := db.admit(); err != nil {
		return result, err
	}
	defer db.done()
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return result, operationError(op, "capture", err)
	}
	capture, err := db.store.Capture()
	if err != nil {
		db.poison(err)
	}
	db.writer.Unlock()
	if err != nil {
		return result, operationError(op, "capture", err)
	}
	defer func() {
		capture.Release()
		db.cleanerPinsReleased()
	}()
	yield := func() error { return db.yieldToWriter(ctx, "incremental-backup.paced:after") }
	err = db.store.RetryMaintenance(ctx, capture.Segments, func() error {
		var err error
		result, err = storage.IncrementalBackup(ctx, capture, db.store.Config, db.store.IO, req, yield)
		return err
	}, nil)
	if err != nil {
		var source *storage.SourceError
		if errors.As(err, &source) {
			db.readFailure(err)
		}
		return IncrementalBackupResult{}, operationError(op, "copy", err)
	}
	return result, nil
}

func PruneIncrementalBackupObjects(ctx context.Context, repository string) error {
	const op = "prune-incremental-backup-objects"
	if ctx == nil {
		return validContextError(op)
	}
	if err := storage.PruneIncrementalBackupObjects(ctx, repository, nil); err != nil {
		return operationError(op, "prune", err)
	}
	return nil
}
