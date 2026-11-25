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
func (db *DB) KeysPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[[]byte, error] {
	return db.scanKeys(ctx, nil, index.PrefixScan(encodedPrefix))
}

// KeysRange streams encoded keys in [lower, upper) in strictly ascending or
// descending bytewise lexicographic order without reading DATA.
func (db *DB) KeysRange(ctx context.Context, lower, upper []byte, reverse bool) iter.Seq2[[]byte, error] {
	return db.scanKeys(ctx, nil, index.OrderedScan(lower, upper, reverse))
}

func (db *DB) scanKeys(ctx context.Context, snapshot *Snapshot, query index.Scan) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		err := db.walkSnapshot(ctx, snapshot, query, "keys", func(k, _ []byte) bool { return yield(k, nil) })
		if err != nil {
			yield(nil, err)
		}
	}
}

// A bounded scan with several short matches packs its mutable key copies into
// disjoint slices. The unused tail never overlaps any previously returned key.
type prefixKeyCopies struct {
	tail  []byte
	small int
}

func (c *prefixKeyCopies) copyKey(key string) []byte {
	const keyCopyBufferBytes = 512
	n := len(key)
	if n <= keyCopyBufferBytes/4 {
		if c.small >= 4 {
			if len(c.tail) < n {
				// Earlier results may be changing on another goroutine. Never
				// grow a previous key copy buffer by copying its published contents.
				c.tail = make([]byte, keyCopyBufferBytes)
			}
			result := c.tail[:n:n]
			copy(result, key)
			c.tail = c.tail[n:]
			return result
		}
		c.small++
	}
	return []byte(key)[:n:n]
}
