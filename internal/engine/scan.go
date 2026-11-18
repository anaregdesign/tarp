package engine

import (
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/storage"
)

// Entry is a snapshot key and value. Both byte slices belong to the caller.
type Entry struct {
	Key   []byte
	Value []byte
}

type scanRecord struct {
	key string
	loc storage.Location
}

// Scan captures a new point-in-time view each time iteration begins.
func (db *DB) Scan(ctx context.Context) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		if err := db.scan(ctx, yield); err != nil {
			yield(Entry{}, err)
		}
	}
}

func (db *DB) scan(ctx context.Context, yield func(Entry, error) bool) error {
	files := make(map[*storage.File][]scanRecord)
	defer func() {
		for file := range files {
			db.releaseScanPin(file)
		}
	}()
	if err := db.captureScan(ctx, files); err != nil {
		return err
	}
	for file, records := range files {
		for i, record := range records {
			value, err := db.readScan(ctx, record)
			if err != nil {
				return err
			}
			if i == len(records)-1 {
				delete(files, file)
				db.releaseScanPin(file)
			}
			if !yield(Entry{Key: []byte(record.key), Value: value}, nil) {
				return nil
			}
		}
	}
	return nil
}

func (db *DB) captureScan(ctx context.Context, files map[*storage.File][]scanRecord) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if ctx == nil {
		return validContextError("scan")
	}
	if err := ctx.Err(); err != nil {
		return operationError("scan", "context", err)
	}
	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		return err
	}
	for key, loc := range db.index {
		if err := ctx.Err(); err != nil {
			return operationError("scan", "context", err)
		}
		records := files[loc.File]
		if len(records) == 0 {
			loc.File.Pin()
		}
		files[loc.File] = append(records, scanRecord{key, loc})
	}
	if err := ctx.Err(); err != nil {
		return operationError("scan", "context", err)
	}
	return nil
}

func (db *DB) readScan(ctx context.Context, record scanRecord) ([]byte, error) {
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()
	if err := ctx.Err(); err != nil {
		return nil, operationError("scan", "context", err)
	}
	value, err := db.store.Read(record.loc, record.key)
	if err != nil {
		db.poison(err)
		return nil, operationError("scan", "read", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, operationError("scan", "context", err)
	}
	return value, db.health()
}

func (db *DB) releaseScanPin(file *storage.File) {
	file.Unpin()
	if file.References() == 0 {
		db.notifyCleaner()
	}
}
