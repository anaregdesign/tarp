package engine

import (
	"context"
	"iter"
)

// Keys streams a fresh index snapshot without opening or pinning DATA files.
func (db *DB) Keys(ctx context.Context) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		err := db.walkSnapshot(ctx, false, func(k string, _ []byte) bool { return yield([]byte(k), nil) })
		if err != nil {
			yield(nil, err)
		}
	}
}
