package tarp

import (
	"context"
	"fmt"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
)

// Keys yields the live keys in unspecified order. Each iteration captures a
// fresh point-in-time snapshot. Memory mode copies the live index; B+tree
// mode captures a root and streams its pages. Neither mode reads DATA.
// Each returned key belongs to the caller. The loop body may call any DB method,
// including Close. An error is yielded once with a nil key and ends iteration.
func (db *DB) Keys(ctx context.Context) iter.Seq2[[]byte, error] {
	return (*engine.DB)(db).Keys(ctx)
}

// KeysPrefix yields the keys whose encoded bytes start with encodedPrefix.
// A nil or empty prefix matches every key. KeysPrefix copies the prefix
// before returning. The iterator has the same snapshot, order, and ownership
// guarantees as [DB.Keys] and does not read DATA.
func (db *DB) KeysPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[[]byte, error] {
	return (*engine.DB)(db).KeysPrefix(ctx, encodedPrefix)
}

// KeysRange yields live keys in the half-open range [lower, upper) in strictly
// ascending bytewise lexicographic order, or strictly descending order when
// [WithReverse] is passed. KeysRange copies lower and upper before returning,
// captures an index-only snapshot, and never reads or pins DATA segments.
func (db *DB) KeysRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[[]byte, error] {
	return (*engine.DB)(db).KeysRange(ctx, lower, upper, isReverseScan(opts))
}

// Keys decodes each key in a DB.Keys snapshot. The iterator retains the DB and
// key codec from this call. A DB or key codec error yields the zero value of K
// and ends iteration.
func (db *View[K, V]) Keys(ctx context.Context) iter.Seq2[K, error] {
	return db.KeysPrefix(ctx, nil)
}

// KeysPrefix decodes the keys selected by [DB.KeysPrefix]. The prefix is a
// prefix of the stored key bytes and does not need to encode a complete K.
func (db *View[K, V]) KeysPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[K, error] {
	view := *db
	rows := view.core.KeysPrefix(ctx, encodedPrefix)
	return func(yield func(K, error) bool) {
		view.decodeKeys(rows, yield)
	}
}

// KeysRange decodes the keys selected by [DB.KeysRange] in ascending or
// [WithReverse] descending encoded-key order without reading DATA segments.
func (db *View[K, V]) KeysRange(ctx context.Context, lower, upper []byte, opts ...ScanOption) iter.Seq2[K, error] {
	view := *db
	rows := view.core.KeysRange(ctx, lower, upper, opts...)
	return func(yield func(K, error) bool) {
		view.decodeKeys(rows, yield)
	}
}

func (db *View[K, V]) decodeKeys(seq iter.Seq2[[]byte, error], yield func(K, error) bool) {
	var zero K
	for data, err := range seq {
		if err != nil {
			yield(zero, err)
			return
		}
		key, err := db.keyCodec.Decode(data)
		if err != nil {
			yield(zero, fmt.Errorf("tarp: decode key: %w", err))
			return
		}
		if !yield(key, nil) {
			return
		}
	}
}
