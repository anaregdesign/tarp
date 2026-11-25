package tarp

import (
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
)

// Snapshot is an immutable point-in-time read view of a [DB].
//
// All point reads, batch reads, key scans, and value scans on a Snapshot
// observe the database state at [Snapshot.Sequence]. Foreground writes and
// compaction can proceed while the Snapshot pins its DATA segments.
//
// Share the returned *Snapshot across goroutines. Copying the Snapshot value
// duplicates its resource bookkeeping. Call Close when finished to release the
// retained index and segment resources.
type Snapshot engine.Snapshot

// Snapshot captures one committed state across multiple calls, such as a key
// scan followed by point lookups while writers continue. Close the returned
// [Snapshot] after the last read. For one known set of keys, [DB.BatchGet]
// reads them together and releases the read resources before returning.
func (db *DB) Snapshot() (*Snapshot, error) {
	snap, err := (*engine.DB)(db).Snapshot()
	return (*Snapshot)(snap), err
}

// Sequence returns the last committed user sequence at capture, or zero before the first write.
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

// Close rejects subsequent reads and scans with [ErrClosed]. It releases the
// index view and DATA segment pins once the last in-flight operation or iterator
// has returned. Calling Close more than once returns nil.
func (s *Snapshot) Close() error {
	return (*engine.Snapshot)(s).Close()
}
