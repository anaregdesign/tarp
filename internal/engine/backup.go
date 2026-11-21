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
	if err := ctx.Err(); err != nil {
		db.writer.Unlock()
		return operationError("backup", "wait", err)
	}
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
	err = db.store.RetryMaintenance(ctx, capture.Files, func() error {
		return storage.BackupWithIndex(ctx, capture, db.store.Config, db.store.IO, destination, yield, db.newIndex)
	}, nil)
	if err != nil {
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
func VerifyBackup(ctx context.Context, source string) (storage.BackupInfo, error) {
	if ctx == nil {
		return storage.BackupInfo{}, validContextError("verify-backup")
	}
	info, err := storage.VerifyBackup(ctx, source)
	if err != nil {
		return storage.BackupInfo{}, operationError("verify-backup", "validation", err)
	}
	return info, nil
}

// VerifyBackupWithScratch sorts the key history using scratch outside the backup
// to avoid repeated DATA passes on high-cardinality images.
func VerifyBackupWithScratch(ctx context.Context, source, scratch string) (storage.BackupInfo, error) {
	if ctx == nil {
		return storage.BackupInfo{}, validContextError("verify-backup")
	}
	info, err := storage.VerifyBackupWithScratch(ctx, source, scratch)
	if err != nil {
		return storage.BackupInfo{}, operationError("verify-backup", "validation", err)
	}
	return info, nil
}

func (l RestoreLimits) backupConfig() (storage.Config, error) {
	limits, err := l.normalized()
	return storage.Config{ValueCacheBudgetBytes: limits.ValueCacheBudgetBytes, RecordBytes: limits.MaxRecordBytes}, err
}

// Check reports whether these destination limits can hold the measured live records.
func (l RestoreLimits) Check(required RestoreLimits) error {
	cfg, err := l.backupConfig()
	if err != nil {
		return err
	}
	return publicCause(cfg.CheckLimits(storage.RequiredLimits{
		ValueCacheBudgetBytes: required.ValueCacheBudgetBytes, RecordBytes: required.MaxRecordBytes,
	}))
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
