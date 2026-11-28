package tarp

import (
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
)

// StagedMutation is a final operation on an encoded key. A Delete ignores
// Value and succeeds without writing a record if the key is absent.
type StagedMutation = engine.MixedOp

const (
	// MaxStagedApplyEntries is the input count limit of one ApplyStaged call.
	MaxStagedApplyEntries = engine.MaxStagedApplyEntries
	// MaxStagedApplyBytes is the total encoded input byte limit, including
	// record headers and Deletes of absent keys.
	MaxStagedApplyBytes = engine.MaxStagedApplyBytes
	// MaxStagedApplySegments limits newly staged DATA segments in one commit.
	MaxStagedApplySegments = engine.MaxStagedApplySegments
)

// ApplyStaged durably commits a stream of final mutations at one frontier,
// even when the writes span multiple DATA segments. It requires IndexBTree.
// Keys must be in strictly increasing encoded-byte order. To replace a key
// multiple times, supply its final operation. Values can be reused after
// yield returns.
//
// Tarp first spools the stream to private scratch. The producer runs outside
// the writer gate and may read or write this DB; it must not call Close on
// this handle. A producer error, invalid input or cancellation before
// publication leaves all staged mutations unapplied. Other writes committed
// while staging are included when applying the final mutations. Once
// publication starts, the commit protocol determines its outcome.
//
// The writer gate is held while preparing the new DATA and index pages and
// publishing the commit. Allow space for the input spool and replacement DATA
// simultaneously. Another ApplyStaged on this DB returns ErrBusy while a
// stream is active. Successful publication clears the value cache.
func (db *DB) ApplyStaged(ctx context.Context, mutations iter.Seq2[StagedMutation, error]) error {
	return (*engine.DB)(db).ApplyStaged(ctx, mutations)
}
