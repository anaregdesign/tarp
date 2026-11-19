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

// RestoreRequest describes the backup to verify and the new database to publish.
type RestoreRequest struct {
	Source      string
	Destination string
	Limits      RestoreLimits
}

// Restore verifies a completed backup and publishes a new, closed database.
// It never overwrites an existing destination. Source backups must be immutable.
func Restore(ctx context.Context, req RestoreRequest) error {
	return restoreWithIO(ctx, req, nil)
}

// VerifyBackup checks the complete immutable source without writing files.
func VerifyBackup(ctx context.Context, source string, limits RestoreLimits) error {
	if ctx == nil {
		return validContextError("verify-backup")
	}
	cfg, err := limits.backupConfig()
	if err != nil {
		return err
	}
	if err := storage.VerifyBackup(ctx, source, cfg); err != nil {
		return operationError("verify-backup", "validation", err)
	}
	return nil
}

func (l RestoreLimits) backupConfig() (storage.Config, error) {
	limits, err := l.normalized()
	return storage.Config{CacheBytes: limits.CacheBytes, RecordBytes: limits.MaxRecordBytes}, err
}

func restoreWithIO(ctx context.Context, req RestoreRequest, fs *storage.IO) error {
	if ctx == nil {
		return validContextError("restore")
	}
	cfg, err := req.Limits.backupConfig()
	if err != nil {
		return err
	}
	if err := storage.Restore(ctx, req.Source, req.Destination, cfg, fs); err != nil {
		return operationError("restore", "validation/copy", err)
	}
	return nil
}
