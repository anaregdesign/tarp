package engine

import (
	"context"
	"math/bits"

	"github.com/anaregdesign/tarp/internal/format"
)

// KeyOrderCompactionEstimate describes rewrite work at one committed frontier.
type KeyOrderCompactionEstimate struct {
	Sequence                     uint64 // Last user sequence at estimation.
	CheckpointCutoffSequence     uint64 // Last user sequence covered by the current DATA checkpoint.
	OrderedBase                  bool   // The current snapshot base is known to be key-ordered.
	PendingLogRecordBytes        uint64 // Encoded bytes in the selected log suffix, excluding segment headers.
	RewriteRecordBytesUpperBound uint64 // Upper bound on encoded record bytes in new snapshot segments for one attempt at this cut.
}

// EstimateKeyOrderCompaction inspects the selected catalog and live record
// accounting under the writer gate.
func (db *DB) EstimateKeyOrderCompaction(ctx context.Context) (KeyOrderCompactionEstimate, error) {
	const op = "estimate-key-order-compaction"
	var estimate KeyOrderCompactionEstimate
	if err := db.admit(); err != nil {
		return estimate, err
	}
	defer db.done()
	if ctx == nil {
		return estimate, validContextError(op)
	}
	if err := ctx.Err(); err != nil {
		return estimate, operationError(op, "context", err)
	}
	db.writer.Lock()
	defer db.writer.Unlock()
	if err := ctx.Err(); err != nil {
		return estimate, operationError(op, "context", err)
	}
	if err := db.health(); err != nil {
		return estimate, err
	}
	// A selected layout can precede its remap. Wait for the counters to
	// describe the same catalog before using them in a bound.
	if db.layoutRemapping {
		return estimate, ErrBusy
	}
	store := db.store
	active := store.Segments[store.Current.ActiveID]
	estimate.Sequence = store.Current.LastUserSequence
	estimate.CheckpointCutoffSequence = store.Catalog.CheckpointCutoffSequence
	estimate.OrderedBase = store.HasOrderedBase()
	estimate.PendingLogRecordBytes = uint64(store.Current.ActiveCommittedEndOffset - format.SegmentHeaderBytes)
	suffixKeys := uint64(active.LiveKeys.Load())
	suffixLiveBytes := uint64(active.LiveBytes.Load())
	var affectedBytes, untouchedBytes, largestUntouched uint64
	var unavailable bool
	for _, entry := range store.Catalog.Entries {
		if err := ctx.Err(); err != nil {
			return KeyOrderCompactionEstimate{}, operationError(op, "context", err)
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		segment := store.Segments[entry.ID]
		unavailable = unavailable || segment.UnavailableData() != nil
		recordBytes := uint64(entry.Bytes - format.SegmentHeaderBytes)
		liveBytes := uint64(segment.LiveBytes.Load())
		if entry.Kind != format.SnapshotKind {
			estimate.PendingLogRecordBytes += recordBytes
			suffixKeys += uint64(segment.LiveKeys.Load())
			suffixLiveBytes += liveBytes
		} else if liveBytes < recordBytes {
			// An overwrite or delete can make remaining live records in
			// this segment move even when the suffix adds no live keys.
			affectedBytes += liveBytes
		} else {
			untouchedBytes += liveBytes
			largestUntouched = max(largestUntouched, liveBytes)
		}
	}
	if err := ctx.Err(); err != nil {
		return KeyOrderCompactionEstimate{}, operationError(op, "context", err)
	}
	possibleExtra := untouchedBytes
	if estimate.OrderedBase && !unavailable {
		// A final Put in the suffix can force at most one additional
		// ordered range to move. A final Delete in a range with every
		// original record still live cannot change that range.
		if hi, lo := bits.Mul64(suffixKeys, largestUntouched); hi == 0 {
			possibleExtra = min(possibleExtra, lo)
		}
	}
	estimate.RewriteRecordBytesUpperBound = suffixLiveBytes + affectedBytes + possibleExtra
	return estimate, nil
}
