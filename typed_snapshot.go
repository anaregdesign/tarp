package tarp

import (
	"context"
	"iter"
)

// TypedSnapshot provides an immutable, typed point-in-time read view of a
// [View] or [TypedDB]. Safe for concurrent reads across goroutines until Close.
type TypedSnapshot[K, V any] struct {
	snap   *Snapshot
	codecs View[K, V]
}

// Snapshot captures a consistent typed point-in-time read view of db.
func (db *View[K, V]) Snapshot() (*TypedSnapshot[K, V], error) {
	snap, err := db.core.Snapshot()
	if err != nil {
		return nil, err
	}
	return &TypedSnapshot[K, V]{
		snap:   snap,
		codecs: *db,
	}, nil
}

// Sequence returns the last committed user sequence at capture, or zero before the first write.
func (s *TypedSnapshot[K, V]) Sequence() uint64 {
	return s.snap.Sequence()
}

// Get returns the decoded value of key in this snapshot, or the zero value of V and an error.
func (s *TypedSnapshot[K, V]) Get(key K) (V, error) {
	var zero V
	k, err := s.codecs.encodeKey(key)
	if err != nil {
		return zero, err
	}
	data, err := s.snap.Get(k)
	if err != nil {
		return zero, err
	}
	return s.codecs.decodeValue(data)
}

// BatchGet reads and decodes keys from this snapshot in input order.
func (s *TypedSnapshot[K, V]) BatchGet(keys []K) ([]TypedBatchResult[V], error) {
	return s.codecs.batchGet(keys, s.snap)
}

// Scan decodes all entries in this snapshot.
func (s *TypedSnapshot[K, V]) Scan(ctx context.Context) iter.Seq2[TypedEntry[K, V], error] {
	return s.ScanPrefix(ctx, nil)
}

// ScanPrefix decodes entries in this snapshot whose encoded key starts with prefix.
func (s *TypedSnapshot[K, V]) ScanPrefix(ctx context.Context, prefix []byte) iter.Seq2[TypedEntry[K, V], error] {
	codecs := s.codecs
	rows := s.snap.ScanPrefix(ctx, prefix)
	return func(yield func(TypedEntry[K, V], error) bool) {
		codecs.decodeEntries(rows, yield)
	}
}

// ScanRange decodes entries in this snapshot in [lower, upper) in ascending or
// [WithReverse] descending encoded-key order.
func (s *TypedSnapshot[K, V]) ScanRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[TypedEntry[K, V], error] {
	codecs := s.codecs
	rows := s.snap.ScanRange(ctx, lower, upper, opts...)
	return func(yield func(TypedEntry[K, V], error) bool) {
		codecs.decodeEntries(rows, yield)
	}
}

// Keys decodes all live keys in this snapshot without reading DATA segments.
func (s *TypedSnapshot[K, V]) Keys(ctx context.Context) iter.Seq2[K, error] {
	return s.KeysPrefix(ctx, nil)
}

// KeysPrefix decodes live keys in this snapshot matching prefix without reading DATA segments.
func (s *TypedSnapshot[K, V]) KeysPrefix(ctx context.Context, prefix []byte) iter.Seq2[K, error] {
	codecs := s.codecs
	rows := s.snap.KeysPrefix(ctx, prefix)
	return func(yield func(K, error) bool) {
		codecs.decodeKeys(rows, yield)
	}
}

// KeysRange decodes live keys in this snapshot in [lower, upper) in ascending or
// [WithReverse] descending encoded-key order without reading DATA segments.
func (s *TypedSnapshot[K, V]) KeysRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[K, error] {
	codecs := s.codecs
	rows := s.snap.KeysRange(ctx, lower, upper, opts...)
	return func(yield func(K, error) bool) {
		codecs.decodeKeys(rows, yield)
	}
}

// Close releases the underlying snapshot's resources. Calling Close more than once returns nil.
func (s *TypedSnapshot[K, V]) Close() error {
	return s.snap.Close()
}
