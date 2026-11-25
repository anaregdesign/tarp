package tarp

import (
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
)

// Snapshot is an immutable point-in-time read view of a [DB].
//
// All point reads, batch reads, key scans, and value scans on a Snapshot
// observe the exact database state at [Snapshot.Sequence]. Holding a Snapshot
// does not block foreground writes or compaction; DATA segments referenced by
// the Snapshot remain retained on disk until [Snapshot.Close] is called.
//
// Snapshot is safe for concurrent use by multiple goroutines. Always call Close
// when finished to release retained index and segment resources.
type Snapshot engine.Snapshot

// Snapshot captures a consistent point-in-time read view of db.
func (db *DB) Snapshot() (*Snapshot, error) {
	snap, err := (*engine.DB)(db).Snapshot()
	return (*Snapshot)(snap), err
}

// Sequence returns the exact LastUserSequence published in this snapshot (0 when empty).
func (s *Snapshot) Sequence() uint64 {
	return (*engine.Snapshot)(s).Sequence()
}

// Get returns an independently owned copy of key's value in this snapshot, or [ErrNotFound].
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	return (*engine.Snapshot)(s).Get(key)
}

// BatchGet reads keys from this snapshot in input order. It enforces
// [MaxBatchEntries] and Options.MaxBatchGetValueBytes identically to [DB.BatchGet].
func (s *Snapshot) BatchGet(keys [][]byte) ([]BatchResult, error) {
	return (*engine.Snapshot)(s).BatchGet(keys)
}

// Scan yields all entries in this snapshot in unspecified order.
func (s *Snapshot) Scan(ctx context.Context) iter.Seq2[Entry, error] {
	return (*engine.Snapshot)(s).Scan(ctx)
}

// ScanPrefix yields entries in this snapshot whose encoded key starts with prefix.
func (s *Snapshot) ScanPrefix(ctx context.Context, prefix []byte) iter.Seq2[Entry, error] {
	return (*engine.Snapshot)(s).ScanPrefix(ctx, prefix)
}

// ScanRange yields entries in this snapshot with encoded keys in [lower, upper)
// in strictly ascending bytewise lexicographic order, or strictly descending
// order when [WithReverse] is passed.
func (s *Snapshot) ScanRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[Entry, error] {
	return (*engine.Snapshot)(s).ScanRange(ctx, lower, upper, isReverseScan(opts))
}

// Keys yields all live keys in this snapshot without reading DATA segments.
func (s *Snapshot) Keys(ctx context.Context) iter.Seq2[[]byte, error] {
	return (*engine.Snapshot)(s).Keys(ctx)
}

// KeysPrefix yields live keys in this snapshot matching prefix without reading DATA segments.
func (s *Snapshot) KeysPrefix(ctx context.Context, prefix []byte) iter.Seq2[[]byte, error] {
	return (*engine.Snapshot)(s).KeysPrefix(ctx, prefix)
}

// KeysRange yields live keys in this snapshot in [lower, upper) in strictly
// ascending bytewise lexicographic order, or strictly descending order when
// [WithReverse] is passed, without reading DATA segments.
func (s *Snapshot) KeysRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[[]byte, error] {
	return (*engine.Snapshot)(s).KeysRange(ctx, lower, upper, isReverseScan(opts))
}

// Close releases the snapshot's index view and unpins retained DATA segments.
// Close is idempotent; subsequent reads or scans on s return [ErrClosed].
func (s *Snapshot) Close() error {
	return (*engine.Snapshot)(s).Close()
}
