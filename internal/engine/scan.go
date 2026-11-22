package engine

import (
	"context"
	"errors"
	"iter"
	"os"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Entry is a snapshot key and value. Both byte slices belong to the caller.
type Entry struct {
	Key   []byte
	Value []byte
}

// Scan captures a new point-in-time view each time iteration begins.
func (db *DB) Scan(ctx context.Context) iter.Seq2[Entry, error] {
	return db.ScanPrefix(ctx, nil)
}

// ScanPrefix reads matching records from the same index and DATA snapshot.
func (db *DB) ScanPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[Entry, error] {
	bounds := index.PrefixBounds(encodedPrefix)
	return func(yield func(Entry, error) bool) {
		err := db.walkSnapshot(ctx, true, bounds, func(k, value []byte) bool { return yield(Entry{Key: k, Value: value}, nil) })
		if err != nil {
			yield(Entry{}, err)
		}
	}
}

// walkSnapshot pins the selected DATA segments for a value scan. Index cursors never
// hold a DB lock or admission across user code, which may close or mutate it.
func (db *DB) walkSnapshot(ctx context.Context, values bool, bounds index.Bounds, yield func([]byte, []byte) bool) error {
	op := "keys"
	if values {
		op = "scan"
	}
	if err := db.admit(); err != nil {
		return err
	}
	if ctx == nil {
		db.done()
		return validContextError(op)
	}
	if err := ctx.Err(); err != nil {
		db.done()
		return operationError(op, "context", err)
	}
	db.indexGate.RLock()
	v, err := bounds.Capture(ctx, db.keys)
	if err != nil {
		db.indexGate.RUnlock()
		db.done()
		return operationError(op, "context", err)
	}
	var pinnedSegments map[uint64]*storage.DataSegment
	if values {
		pinnedSegments = make(map[uint64]*storage.DataSegment, len(db.files))
		for id, f := range db.files {
			f.Pin()
			pinnedSegments[id] = f
		}
	}
	db.indexGate.RUnlock()
	db.done()
	defer v.Close()
	defer func() {
		for _, f := range pinnedSegments {
			db.releaseScanPin(f)
		}
	}()

	check := func() error {
		db.life.Lock()
		closing := db.closing
		db.life.Unlock()
		if closing {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return operationError(op, "context", err)
		}
		return db.health()
	}
	if err := check(); err != nil {
		return err
	}
	var result error
	var stopped bool
	var copies *prefixKeyCopies
	if bounds.Lower != "" {
		copies = new(prefixKeyCopies)
	}
	err = v.Walk(ctx, func(k string, a index.Address) bool {
		if result = check(); result != nil {
			return false
		}
		var value []byte
		if values {
			f := pinnedSegments[a.FileID]
			if f == nil {
				err := missingIndexFile(a.FileID)
				db.poison(err)
				result = operationError(op, "index", err)
				return false
			}
			if result = db.admit(); result != nil {
				return false
			}
			value, result = db.store.ReadContext(ctx, storage.At(f, a), k)
			if result != nil {
				if result == ctx.Err() {
					db.done()
					result = operationError(op, "context", result)
					return false
				}
				db.readFailure(result)
				db.done()
				result = operationError(op, "read", result)
				return false
			}
			db.done()
			if result = check(); result != nil {
				return false
			}
		}
		var key []byte
		if copies == nil {
			key = []byte(k)
		} else {
			key = copies.copyKey(k)
		}
		stopped = !yield(key, value)
		return !stopped
	})
	if result != nil {
		return result
	}
	if err != nil {
		if errors.Is(err, os.ErrClosed) || err == ctx.Err() {
			if lifeErr := check(); lifeErr != nil {
				return lifeErr
			}
		}
		db.poison(err)
		return operationError(op, "index", err)
	}
	// A bounded walk can reach its upper bound without another callback.
	if !stopped && bounds.Lower != "" {
		return check()
	}
	return nil
}

func (db *DB) releaseScanPin(file *storage.DataSegment) {
	file.Unpin()
	if file.References() == 0 {
		db.notifyCleaner()
	}
}
