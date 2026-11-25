package tarp

import (
	"bytes"
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
)

// ScanOption configures ordered range scans ([DB.ScanRange], [DB.KeysRange],
// and their typed and snapshot counterparts).
type ScanOption struct {
	reverse bool
}

// WithReverse reverses ordered range iteration so keys in [lower, upper) are
// yielded in strictly descending bytewise lexicographic order.
func WithReverse() ScanOption {
	return ScanOption{reverse: true}
}

func isReverseScan(opts []ScanOption) bool {
	for _, opt := range opts {
		if opt.reverse {
			return true
		}
	}
	return false
}

// PrefixRange computes cloned half-open [lower, upper) bounds matching all
// keys that satisfy bytes.HasPrefix(key, prefix). Pass the bounds to
// [DB.KeysRange] or [DB.ScanRange] for an ordered prefix scan. An empty prefix returns
// (nil, nil). An all-0xff prefix returns (bytes.Clone(prefix), nil).
func PrefixRange(prefix []byte) (lower, upper []byte) {
	if len(prefix) == 0 {
		return nil, nil
	}
	lower = bytes.Clone(prefix)
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			upper = bytes.Clone(prefix[:i+1])
			upper[i]++
			return lower, upper
		}
	}
	return lower, nil
}

// ScanRange yields live entries with encoded keys in the half-open range
// [lower, upper) in strictly ascending bytewise lexicographic order, or
// strictly descending order when [WithReverse] is passed.
//
// An empty or nil lower bound is unbounded below; an empty or nil upper bound
// is unbounded above. When len(upper) > 0 and bytes.Compare(lower, upper) >= 0,
// ScanRange yields zero entries after checking context and handle health.
// ScanRange copies lower and upper before returning. Each iteration captures a
// fresh point-in-time snapshot and returns caller-owned key and value slices.
func (db *DB) ScanRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[Entry, error] {
	return (*engine.DB)(db).ScanRange(ctx, lower, upper, isReverseScan(opts))
}

// KeysRange yields live keys in the half-open range [lower, upper) in strictly
// ascending bytewise lexicographic order, or strictly descending order when
// [WithReverse] is passed. KeysRange copies lower and upper before returning,
// captures an index-only snapshot, and never reads or pins DATA segments.
func (db *DB) KeysRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[[]byte, error] {
	return (*engine.DB)(db).KeysRange(ctx, lower, upper, isReverseScan(opts))
}

// ScanRange decodes the entries selected by [DB.ScanRange] in ascending or
// [WithReverse] descending encoded-key order. The lower and upper bounds are
// raw encoded-key bounds and do not need to encode complete K values.
func (db View[K, V]) ScanRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[TypedEntry[K, V], error] {
	rows := db.core.ScanRange(ctx, lower, upper, opts...)
	return func(yield func(TypedEntry[K, V], error) bool) {
		db.decodeEntries(rows, yield)
	}
}

// KeysRange decodes the keys selected by [DB.KeysRange] in ascending or
// [WithReverse] descending encoded-key order without reading DATA segments.
func (db View[K, V]) KeysRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[K, error] {
	rows := db.core.KeysRange(ctx, lower, upper, opts...)
	return func(yield func(K, error) bool) {
		db.decodeKeys(rows, yield)
	}
}
