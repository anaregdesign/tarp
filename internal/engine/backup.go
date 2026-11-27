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
	return db.backupImage(ctx, destination, nil)
}

// BackupWithSnapshot gives the caller a borrowed view of the frozen backup cut
// while its validated copy is still private.
func (db *DB) BackupWithSnapshot(ctx context.Context, destination string, inspect func(*Snapshot) error) error {
	if inspect == nil {
		return operationError("backup", "snapshot", errors.New("tarp: nil snapshot callback"))
	}
	return db.backupImage(ctx, destination, inspect)
}

func (db *DB) backupImage(ctx context.Context, destination string, inspect func(*Snapshot) error) error {
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
	db.cancelAutoRecovery()
	defer func() {
		db.backup.Store(false)
		db.resumeCleaner()
		db.cleanerPinsReleased()
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
	defer capture.Release()
	var snapshot *Snapshot
	if inspect != nil {
		snapshot, err = db.captureSnapshot()
		if err != nil {
			db.writer.Unlock()
			return err
		}
		defer snapshot.Close()
	}
	db.writer.Unlock()
	yield := func() error {
		return db.yieldToWriter(ctx, "backup.paced:after")
	}
	var beforePublish func() error
	if inspect != nil {
		beforePublish = func() error { return inspect(snapshot) }
	}
	err = db.store.RetryMaintenance(ctx, capture.Segments, func() error {
		return storage.Backup(ctx, capture, db.store.Config, db.store.IO, destination, yield, db.newIndex, beforePublish)
	}, nil)
	if err != nil {
		var inspection *storage.BackupInspectionError
		if errors.As(err, &inspection) {
			return &OpError{Operation: "backup", Phase: "snapshot", Outcome: NotApplied, Err: err}
		}
		var source *storage.SourceError
		if errors.As(err, &source) {
			db.readFailure(err)
		}
		return operationError("backup", "copy", err)
	}
	return nil
}

// Briefly give queued or committing mutations time to use the disk and writer
// gate. Maintenance advances one batch per pause even under continuous writes.
func (db *DB) yieldToWriter(ctx context.Context, after string) error {
	if db.cleaner.writers.Load() == 0 {
		return nil
	}
	timer := time.NewTimer(2 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return db.store.IO.Check(after)
	case <-ctx.Done():
		return ctx.Err()
	}
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
	return storage.Config{ValueCacheBudgetBytes: limits.ValueCacheBudgetBytes, MaxRecordBytes: limits.MaxRecordBytes}, err
}

// Check reports whether these destination limits can hold the measured live records.
func (l RestoreLimits) Check(sizes storage.LiveRecordSizes) error {
	cfg, err := l.backupConfig()
	if err != nil {
		return err
	}
	return publicCause(cfg.CheckLimits(sizes))
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
