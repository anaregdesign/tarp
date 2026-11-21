package engine

import (
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/index"
)

// Keys streams a fresh index snapshot without opening or pinning DATA files.
func (db *DB) Keys(ctx context.Context) iter.Seq2[[]byte, error] {
	return db.KeysPrefix(ctx, nil)
}

// KeysPrefix streams matching encoded keys without reading DATA.
func (db *DB) KeysPrefix(ctx context.Context, prefix []byte) iter.Seq2[[]byte, error] {
	bounds := index.PrefixBounds(prefix)
	return func(yield func([]byte, error) bool) {
		err := db.walkSnapshot(ctx, false, bounds, func(k string, _ []byte) bool { return yield([]byte(k), nil) })
		if err != nil {
			yield(nil, err)
		}
	}
}
