package engine

import (
	"context"
	"errors"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

// Backup durably publishes a consistent point-in-time directory while writes
// continue. Destination must not exist, and its parent must be caller-controlled.
func (db *DB) Backup(ctx context.Context, destination string) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if ctx == nil {
		return validContextError("backup")
	}
	if err := ctx.Err(); err != nil {
		return operationError("backup", "context", err)
	}
	if !db.backup.CompareAndSwap(false, true) {
		return ErrBusy
	}
	defer func() {
		db.backup.Store(false)
		db.resumeCleaner()
		db.notifyCleaner()
	}()
	db.writer.Lock()
	if err := db.health(); err != nil {
		db.writer.Unlock()
		return err
	}
	capture, err := db.store.Capture()
	if err != nil {
		db.poison(err)
		db.writer.Unlock()
		return operationError("backup", "capture", err)
	}
	db.writer.Unlock()
	defer capture.Release()
	// Yield briefly between bounded copy batches only while durable mutations
	// are queued or committing. A continuously busy writer cannot starve Backup.
	yield := func() error {
		if db.cleaner.writers.Load() == 0 {
			return nil
		}
		timer := time.NewTimer(2 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return db.store.IO.Check("backup.paced:after")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := storage.Backup(ctx, capture, db.store.Config, db.store.IO, destination, yield); err != nil {
		var source *storage.SourceError
		if errors.As(err, &source) {
			db.poison(err)
		}
		return operationError("backup", "copy", err)
	}
	return nil
}

// Restore verifies a completed backup and publishes a new, closed database.
// It never overwrites an existing destination. Source backups must be immutable.
func Restore(ctx context.Context, backup, destination string, opts Options) error {
	return restoreWithIO(ctx, backup, destination, opts, nil)
}

func restoreWithIO(ctx context.Context, backup, destination string, opts Options, fs *storage.IO) error {
	if ctx == nil {
		return validContextError("restore")
	}
	opts, err := opts.normalized()
	if err != nil {
		return err
	}
	if err := storage.Restore(ctx, backup, destination, storage.Config{CacheBytes: opts.CacheBytes, RecordBytes: opts.MaxRecordBytes}, fs); err != nil {
		return operationError("restore", "validation/copy", err)
	}
	return nil
}
