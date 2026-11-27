package engine

import (
	"context"

	"github.com/anaregdesign/tarp/internal/storage"
)

type RecoveryPath = storage.RecoveryPath
type RecoveryReason = storage.RecoveryReason
type RecoveryInfo = storage.RecoveryInfo
type IndexCheckpointStatus = storage.IndexCheckpointStatus

const (
	RecoveryCreated              = storage.RecoveryCreated
	RecoveryFullReplay           = storage.RecoveryFullReplay
	RecoveryCheckpointReplay     = storage.RecoveryCheckpointReplay
	RecoveryFastCheckpointReplay = storage.RecoveryFastCheckpointReplay

	RecoveryNoReason           = storage.RecoveryNoReason
	RecoveryNoCheckpoint       = storage.RecoveryNoCheckpoint
	RecoveryCheckpointMissing  = storage.RecoveryCheckpointMissing
	RecoveryCheckpointUnusable = storage.RecoveryCheckpointUnusable
	RecoveryReplayBudget       = storage.RecoveryReplayBudget
	RecoveryOptionLimits       = storage.RecoveryOptionLimits
	RecoveryFullValidation     = storage.RecoveryFullValidation
	RecoveryEagerIndex         = storage.RecoveryEagerIndex
)

func (db *DB) RecoveryInfo() RecoveryInfo { return db.store.RecoveryInfo() }

func (db *DB) IndexCheckpointStatus(ctx context.Context) (IndexCheckpointStatus, error) {
	const op = "index checkpoint status"
	if ctx == nil {
		return IndexCheckpointStatus{}, validContextError(op)
	}
	if err := db.admit(); err != nil {
		return IndexCheckpointStatus{}, err
	}
	defer db.done()
	for {
		db.writer.Lock()
		if err := ctx.Err(); err != nil {
			db.writer.Unlock()
			return IndexCheckpointStatus{}, operationError(op, "capture", err)
		}
		current, catalog, entries := db.store.Current, db.store.Catalog, db.store.CatalogEntries()
		db.writer.Unlock()
		status, err := db.store.ReadIndexCheckpointStatus(ctx, current, catalog, entries)
		if err != nil {
			return IndexCheckpointStatus{}, operationError(op, "metadata", err)
		}
		if status.Reason != RecoveryCheckpointMissing {
			return status, nil
		}
		// A later publication may have retired the captured INDEX before
		// its file was opened. Retry only if the selection moved.
		db.writer.Lock()
		selected := db.store.Catalog.Index.ID
		db.writer.Unlock()
		if selected == status.CheckpointID {
			return status, nil
		}
	}
}
