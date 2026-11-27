package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
)

// RecoveryPath describes how an Open obtained its live index and validated DATA.
type RecoveryPath = engine.RecoveryPath

const (
	// RecoveryCreated indicates Open initialized a new database.
	RecoveryCreated = engine.RecoveryCreated
	// RecoveryFullReplay indicates Open rebuilt the index from authoritative DATA.
	RecoveryFullReplay = engine.RecoveryFullReplay
	// RecoveryCheckpointReplay indicates Open imported the complete checkpoint
	// index and replayed the DATA beyond its coverage.
	RecoveryCheckpointReplay = engine.RecoveryCheckpointReplay
	// RecoveryFastCheckpointReplay indicates Indexed Open reused durable index
	// pages and replayed the changed DATA.
	RecoveryFastCheckpointReplay = engine.RecoveryFastCheckpointReplay
)

// RecoveryReason explains why Open did not reuse durable index pages.
type RecoveryReason = engine.RecoveryReason

const (
	// RecoveryNoReason applies to creation, successful fast recovery, and an
	// available checkpoint estimate within the budget.
	RecoveryNoReason = engine.RecoveryNoReason
	// RecoveryNoCheckpoint indicates the selected catalog has no INDEX descriptor.
	RecoveryNoCheckpoint = engine.RecoveryNoCheckpoint
	// RecoveryCheckpointMissing indicates the selected INDEX file does not exist.
	RecoveryCheckpointMissing = engine.RecoveryCheckpointMissing
	// RecoveryCheckpointUnusable indicates derived checkpoint data could not be used.
	RecoveryCheckpointUnusable = engine.RecoveryCheckpointUnusable
	// RecoveryReplayBudget indicates changed DATA exceeded the fast replay byte budget.
	RecoveryReplayBudget = engine.RecoveryReplayBudget
	// RecoveryOptionLimits indicates checkpoint size bounds could not prove the
	// selected live records fit the Open options, so DATA replay checked exact sizes.
	RecoveryOptionLimits = engine.RecoveryOptionLimits
	// RecoveryFullValidation indicates Full Open used eager import or rebuilt
	// its index from DATA for a layered checkpoint.
	RecoveryFullValidation = engine.RecoveryFullValidation
	// RecoveryEagerIndex indicates the selected backend uses eager import or
	// rebuilds its index from DATA for a layered checkpoint.
	RecoveryEagerIndex = engine.RecoveryEagerIndex
)

// RecoveryInfo records the last successful Open of a handle. It remains
// available after Close. It describes the checkpoint selected at Open, even
// if the checkpoint was unusable and full replay supplied the index.
type RecoveryInfo struct {
	Path             RecoveryPath   // Recovery path used at Open.
	Reason           RecoveryReason // Reason fast recovery was skipped or abandoned.
	LastUserSequence uint64         // User sequence Open recovered.
	CheckpointID     uint64         // Selected INDEX file ID, or zero if absent.
	CoveredSequence  uint64         // User sequence claimed by the selected INDEX descriptor.
	// ChangedDataBytes is the length of selected DATA outside the checkpoint's
	// physical coverage, including file headers and the committed active prefix.
	// A changed layout can add bytes without advancing the user sequence. It
	// is a logical length, independent of actual bytes read or checksummed.
	ChangedDataBytes uint64
	// ChangedDataBytesExact is false when the logical length is a lower bound.
	// Inexact zero means no estimate was available.
	ChangedDataBytesExact bool
	// ReplayBudgetBytes is the fast replay budget at Open. Zero indicates Open
	// had no fast replay path configured.
	ReplayBudgetBytes uint64
}

// RecoveryInfo returns this handle's immutable Open report without file I/O.
// Checkpoint or write activity after Open leaves the report unchanged.
func (db *DB) RecoveryInfo() RecoveryInfo {
	return RecoveryInfo((*engine.DB)(db).RecoveryInfo())
}

// RecoveryInfo returns the Open report of the underlying encoded database.
func (db *TypedDB[K, V]) RecoveryInfo() RecoveryInfo {
	return db.core.RecoveryInfo()
}

// IndexCheckpointStatus estimates the current DATA replay backlog of an
// Indexed B+tree Open. It describes one committed frontier and may become
// stale as writes and DATA layout changes proceed.
type IndexCheckpointStatus struct {
	LastUserSequence uint64 // User sequence sampled at the committed frontier.
	CheckpointID     uint64 // Selected INDEX file ID, or zero if absent.
	CoveredSequence  uint64 // User sequence claimed by the selected INDEX descriptor.
	// ChangedDataBytes includes physical files outside checkpoint coverage and
	// the committed active prefix. Same-sequence relocation adds changed DATA.
	ChangedDataBytes uint64
	// ChangedDataBytesExact is false for a lower bound. Inexact zero means no
	// estimate was available.
	ChangedDataBytesExact bool
	ReplayBudgetBytes     uint64         // DATA byte limit of fast Indexed replay.
	Reason                RecoveryReason // Metadata availability or budget result.
}

// IndexCheckpointStatus reads the selected INDEX tip metadata to report the
// current replay backlog. It authenticates metadata outside the writer gate.
// INDEX pages, ancestors, and DATA are checked by the corresponding Open or
// validation operation. Use [DB.RecoveryInfo] for a handle's immutable Open report.
func (db *DB) IndexCheckpointStatus(ctx context.Context) (IndexCheckpointStatus, error) {
	status, err := (*engine.DB)(db).IndexCheckpointStatus(ctx)
	return IndexCheckpointStatus(status), err
}

// IndexCheckpointStatus reports the checkpoint backlog of the encoded database.
func (db *TypedDB[K, V]) IndexCheckpointStatus(ctx context.Context) (IndexCheckpointStatus, error) {
	return db.core.IndexCheckpointStatus(ctx)
}
