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
	return func(yield func(Entry, error) bool) {
		err := db.walkSnapshot(ctx, true, func(k string, value []byte) bool { return yield(Entry{Key: []byte(k), Value: value}, nil) })
		if err != nil {
			yield(Entry{}, err)
		}
	}
}

// walkSnapshot pins the DATA layout only for a value scan. Index cursors never
// hold a DB lock or admission across user code, which may close or mutate it.
func (db *DB) walkSnapshot(ctx context.Context, values bool, yield func(string, []byte) bool) error {
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
	v := db.keys.View()
	var layout map[uint64]*storage.File
	if values {
		layout = make(map[uint64]*storage.File, len(db.files))
		for id, f := range db.files {
			f.Pin()
			layout[id] = f
		}
	}
	db.indexGate.RUnlock()
	db.done()
	defer v.Close()
	defer func() {
		for _, f := range layout {
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
	err := v.All(func(k string, a index.Address) bool {
		if result = check(); result != nil {
			return false
		}
		var value []byte
		if values {
			f := layout[a.FileID]
			if f == nil {
				err := missingIndexFile(a.FileID)
				db.poison(err)
				result = operationError(op, "index", err)
				return false
			}
			if result = db.admit(); result != nil {
				return false
			}
			value, result = db.store.Read(storage.At(f, a), k)
			if result != nil {
				db.poison(result)
				db.done()
				result = operationError(op, "read", result)
				return false
			}
			db.done()
			if result = check(); result != nil {
				return false
			}
		}
		return yield(k, value)
	})
	if result != nil {
		return result
	}
	if err != nil {
		if errors.Is(err, os.ErrClosed) {
			if lifeErr := check(); lifeErr != nil {
				return lifeErr
			}
		}
		db.poison(err)
		return operationError(op, "index", err)
	}
	return nil
}

func (db *DB) releaseScanPin(file *storage.File) {
	file.Unpin()
	if file.References() == 0 {
		db.notifyCleaner()
	}
}
