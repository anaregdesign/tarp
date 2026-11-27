package storage

import (
	"errors"
	"fmt"
	"math"

	"github.com/anaregdesign/tarp/internal/format"
)

type RecoveryPath string

const (
	RecoveryCreated              RecoveryPath = "created"
	RecoveryFullReplay           RecoveryPath = "full_replay"
	RecoveryCheckpointReplay     RecoveryPath = "checkpoint_replay"
	RecoveryFastCheckpointReplay RecoveryPath = "fast_checkpoint_replay"
)

type RecoveryReason string

const (
	RecoveryNoReason           RecoveryReason = ""
	RecoveryNoCheckpoint       RecoveryReason = "no_checkpoint"
	RecoveryCheckpointMissing  RecoveryReason = "checkpoint_missing"
	RecoveryCheckpointUnusable RecoveryReason = "checkpoint_unusable"
	RecoveryReplayBudget       RecoveryReason = "replay_budget"
	RecoveryOptionLimits       RecoveryReason = "option_limits"
	RecoveryFullValidation     RecoveryReason = "full_validation"
	RecoveryEagerIndex         RecoveryReason = "eager_index"
)

type RecoveryInfo struct {
	Path                  RecoveryPath
	Reason                RecoveryReason
	LastUserSequence      uint64
	CheckpointID          uint64
	CoveredSequence       uint64
	ChangedDataBytes      uint64
	ChangedDataBytesExact bool
	ReplayBudgetBytes     uint64
}

func (s *Store) RecoveryInfo() RecoveryInfo { return s.recoveryInfo }

var errIndexCheckpointMissing = fmt.Errorf("%w: INDEX file is missing", ErrIndexCheckpointUnusable)
var errIndexCheckpointOptions = fmt.Errorf("%w: checkpoint size bounds exceed Open options", ErrIndexCheckpointUnusable)

type indexReplayBudgetError struct{ bytes uint64 }

func (e *indexReplayBudgetError) Error() string {
	return fmt.Sprintf("storage: changed DATA exceeds the INDEX replay budget (%d bytes or more)", e.bytes)
}

func (*indexReplayBudgetError) Unwrap() error { return ErrIndexCheckpointUnusable }

func (r *RecoveryInfo) noteCheckpointFailure(err error) {
	var budget *indexReplayBudgetError
	switch {
	case errors.As(err, &budget):
		r.Reason = RecoveryReplayBudget
		r.ChangedDataBytes = budget.bytes
		r.ChangedDataBytesExact = false
	case errors.Is(err, errIndexCheckpointMissing):
		r.Reason = RecoveryCheckpointMissing
	case errors.Is(err, errIndexCheckpointOptions):
		r.Reason = RecoveryOptionLimits
	default:
		r.Reason = RecoveryCheckpointUnusable
	}
}

// Sum selected file lengths, excluding metadata-only log intervals. Overflow
// leaves the maximum representable value as a lower bound.
func (r *RecoveryInfo) noteChangedData(entries []format.CatalogEntry, activeBytes uint64) {
	r.ChangedDataBytes = activeBytes
	r.ChangedDataBytesExact = true
	for _, entry := range entries {
		size := uint64(entry.Bytes)
		if size > math.MaxUint64-r.ChangedDataBytes {
			r.ChangedDataBytes = math.MaxUint64
			r.ChangedDataBytesExact = false
			return
		}
		r.ChangedDataBytes += size
	}
}

func (r *RecoveryInfo) noteCheckpointSuccess(indexed, fast bool, image *Image) {
	if fast {
		r.Path, r.Reason = RecoveryFastCheckpointReplay, RecoveryNoReason
		return
	}
	r.Path, r.Reason = RecoveryCheckpointReplay, RecoveryFullValidation
	if indexed {
		r.Reason = RecoveryEagerIndex
	}
	suffix := image.Catalog.Entries[image.Catalog.Index.CoveredCatalogEntries:]
	r.noteChangedData(suffix, uint64(image.Current.ActiveCommittedEndOffset))
}
