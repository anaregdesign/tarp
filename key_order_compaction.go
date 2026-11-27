package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
)

// KeyOrderCompactionEstimate describes a [WithKeyOrder] checkpoint at a
// committed frontier. PendingLogRecordBytes counts retained Put and Delete
// records after CheckpointCutoffSequence. RewriteRecordBytesUpperBound bounds
// the bytes of encoded records written to new snapshot segments in one healthy
// copy attempt at Sequence. With an unknown order or known DATA damage, it
// allows for copying every live record at that cut.
//
// The estimate excludes DATA segment headers, input reads, retries, repairs,
// PARITY, scratch, INDEX, metadata, cleanup and filesystem write amplification.
type KeyOrderCompactionEstimate = engine.KeyOrderCompactionEstimate

// EstimateKeyOrderCompaction estimates rewrite work for [DB.Compact] with
// [WithKeyOrder] without reading DATA or starting maintenance. It inspects the
// selected catalog under the writer gate and returns [ErrBusy] while maintenance
// is updating the index to match a newly published layout. A concurrent write
// or layout change can make the estimate stale before Compact captures its own
// frontier.
func (db *DB) EstimateKeyOrderCompaction(ctx context.Context) (KeyOrderCompactionEstimate, error) {
	return (*engine.DB)(db).EstimateKeyOrderCompaction(ctx)
}

// EstimateKeyOrderCompaction estimates rewrite work on the underlying DB.
func (db *TypedDB[K, V]) EstimateKeyOrderCompaction(ctx context.Context) (KeyOrderCompactionEstimate, error) {
	return db.core.EstimateKeyOrderCompaction(ctx)
}
