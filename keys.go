package tarp

import (
	"context"
	"fmt"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
)

// Keys yields the live keys in unspecified order. Each iteration captures a
// fresh point-in-time snapshot of the in-memory index. Capture takes O(live
// keys) time and memory. It performs no DATA reads and pins no files.
// Each returned key belongs to the caller. The loop body may call any DB method,
// including Close. An error is yielded once with a nil key and ends iteration.
func (db *DB) Keys(ctx context.Context) iter.Seq2[[]byte, error] {
	return (*engine.DB)(db).Keys(ctx)
}

// Keys decodes each key in a DB.Keys snapshot with the key codec. A DB or key
// codec error yields the zero value of K and ends iteration.
func (db *View[K, V]) Keys(ctx context.Context) iter.Seq2[K, error] {
	return func(yield func(K, error) bool) {
		var zero K
		for data, err := range db.core.Keys(ctx) {
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
}
