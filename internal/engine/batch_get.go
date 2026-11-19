package engine

import (
	"bytes"

	"github.com/anaregdesign/tarp/internal/storage"
)

// BatchResult holds one requested key's value. Found distinguishes an empty
// value from a missing key. The caller owns Value.
type BatchResult struct {
	Value []byte
	Found bool
}

// BatchGet returns a point-in-time view in input order, preserving cache
// membership and LRU recency. Misses read pinned, checked records. A failed
// read returns no results. It accepts at most MaxBatchEntries keys.
func (db *DB) BatchGet(keys [][]byte) ([]BatchResult, error) {
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()
	if err := CheckBatchCount(len(keys)); err != nil {
		return nil, err
	}
	for _, key := range keys {
		if len(key) == 0 {
			return nil, ErrEmptyKey
		}
	}

	locations := make([]storage.Location, len(keys))
	results := make([]BatchResult, len(keys))
	db.state.Lock()
	for i, key := range keys {
		if value, ok := db.cache.Peek(key); ok {
			results[i] = BatchResult{Value: value, Found: true}
		} else if loc, ok := db.index[string(key)]; ok {
			loc.File.Pin()
			locations[i] = loc
		}
	}
	db.state.Unlock()
	defer func() {
		for _, loc := range locations {
			if loc.File != nil {
				db.releaseScanPin(loc.File)
			}
		}
	}()

	read := make(map[storage.Location][]byte)
	for i, loc := range locations {
		if results[i].Found {
			results[i].Value = bytes.Clone(results[i].Value)
			continue
		}
		if loc.File == nil {
			continue
		}
		if value, seen := read[loc]; seen {
			results[i] = BatchResult{Value: bytes.Clone(value), Found: true}
			continue
		}
		value, err := db.store.Read(loc, string(keys[i]))
		if err != nil {
			db.poison(err)
			return nil, operationError("batch-get", "read", err)
		}
		read[loc] = value
		results[i] = BatchResult{Value: value, Found: true}
	}
	if err := db.health(); err != nil {
		return nil, err
	}
	return results, nil
}
