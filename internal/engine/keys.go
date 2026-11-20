package engine

import (
	"context"
	"iter"
)

// Keys captures the live key strings at the start of each iteration. Their
// immutable bytes remain valid across writes and compaction without file pins.
func (db *DB) Keys(ctx context.Context) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		keys, err := db.captureKeys(ctx)
		if err != nil {
			yield(nil, err)
			return
		}
		for _, key := range keys {
			// A consumer may close the DB or cancel between yields.
			db.life.Lock()
			closing := db.closing
			db.life.Unlock()
			if closing {
				yield(nil, ErrClosed)
				return
			}
			if err := db.health(); err != nil {
				yield(nil, err)
				return
			}
			if err := ctx.Err(); err != nil {
				yield(nil, operationError("keys", "context", err))
				return
			}
			if !yield([]byte(key), nil) {
				return
			}
		}
	}
}

func (db *DB) captureKeys(ctx context.Context) ([]string, error) {
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()
	if ctx == nil {
		return nil, validContextError("keys")
	}
	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, operationError("keys", "context", err)
	}
	keys := make([]string, 0, len(db.index))
	for key := range db.index {
		keys = append(keys, key)
		if len(keys)%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, operationError("keys", "context", err)
			}
		}
	}
	return keys, nil
}
