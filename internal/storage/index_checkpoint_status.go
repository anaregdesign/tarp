package storage

import (
	"context"
	"errors"
	"math"

	"github.com/anaregdesign/tarp/internal/format"
)

type IndexCheckpointStatus struct {
	LastUserSequence      uint64
	CheckpointID          uint64
	CoveredSequence       uint64
	ChangedDataBytes      uint64
	ChangedDataBytesExact bool
	ReplayBudgetBytes     uint64
	Reason                RecoveryReason
}

// ReadIndexCheckpointStatus authenticates the selected INDEX tip metadata for
// a coherent view captured under the writer gate. All file reads are outside
// the gate. The immutable entry tree can outlive a newer catalog selection.
func (s *Store) ReadIndexCheckpointStatus(ctx context.Context, current format.Frontier, catalog format.Catalog, entries CatalogEntries) (_ IndexCheckpointStatus, result error) {
	cp := catalog.Index
	status := IndexCheckpointStatus{
		LastUserSequence: current.LastUserSequence, CheckpointID: cp.ID,
		CoveredSequence: cp.CoveredSequence, ReplayBudgetBytes: maxIndexedReplayBytes,
	}
	if cp.ID == 0 {
		status.Reason = RecoveryNoCheckpoint
		return status, nil
	}
	if cp.CoveredSequence > current.LastUserSequence || cp.CoveredSequence < catalog.CheckpointCutoffSequence {
		status.Reason = RecoveryCheckpointUnusable
		return status, nil
	}
	file, err := s.readIndexFile(ctx, catalog, nil)
	if IsIndexCheckpointUnusable(err) {
		status.Reason = RecoveryCheckpointUnusable
		if errors.Is(err, errIndexCheckpointMissing) {
			status.Reason = RecoveryCheckpointMissing
		}
		return status, nil
	}
	if err != nil {
		return IndexCheckpointStatus{}, err
	}
	defer func() { result = errors.Join(result, file.fd.Close()) }()
	covered := make(map[uint64]format.CatalogEntry, len(file.layout.Entries))
	for _, entry := range file.layout.Entries {
		if err := ctx.Err(); err != nil {
			return IndexCheckpointStatus{}, err
		}
		if entry.HasPhysicalFile() {
			covered[entry.ID] = entry
		}
	}
	changed := uint64(current.ActiveCommittedEndOffset)
	status.ChangedDataBytesExact = true
	for entry := range entries.All() {
		if err := ctx.Err(); err != nil {
			return IndexCheckpointStatus{}, err
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		if old, ok := covered[entry.ID]; ok {
			if old != entry {
				status.ChangedDataBytesExact = false
				status.Reason = RecoveryCheckpointUnusable
				return status, nil
			}
			continue
		}
		if size := uint64(entry.Bytes); size > math.MaxUint64-changed {
			changed = math.MaxUint64
			status.ChangedDataBytesExact = false
		} else {
			changed += size
		}
	}
	status.ChangedDataBytes = changed
	if changed > status.ReplayBudgetBytes {
		status.Reason = RecoveryReplayBudget
	}
	return status, nil
}
