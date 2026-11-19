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

	results := make([]BatchResult, len(keys))
	var locations []storage.Location
	var firstMiss int
	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		return nil, err
	}
	for i, key := range keys {
		if value, ok := db.cache.Peek(key); ok {
			countLookup(&db.stats.Cache.Hits)
			results[i] = BatchResult{Value: value, Found: true}
		} else if loc, ok := db.index[string(key)]; ok {
			countLookup(&db.stats.Cache.Misses)
			if locations == nil {
				firstMiss = i
				locations = make([]storage.Location, len(keys)-i)
			}
			loc.File.Pin()
			locations[i-firstMiss] = loc
		} else {
			countLookup(&db.stats.MissingKeys)
		}
	}
	db.state.Unlock()

	// Cached values are immutable. Clone after releasing state so each slot
	// owns its result. Batches served entirely here need no pin bookkeeping.
	for i := range results {
		results[i].Value = bytes.Clone(results[i].Value)
	}
	if locations != nil {
		if err := db.readBatchMisses(keys[firstMiss:], results[firstMiss:], locations); err != nil {
			return nil, err
		}
	}
	if err := db.health(); err != nil {
		return nil, err
	}
	return results, nil
}

func (db *DB) readBatchMisses(keys [][]byte, results []BatchResult, locations []storage.Location) error {
	defer func() {
		for _, loc := range locations {
			if loc.File != nil {
				db.releaseScanPin(loc.File)
			}
		}
	}()

	read := make(map[storage.Location][]byte)
	for i, loc := range locations {
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
			return operationError("batch-get", "read", err)
		}
		read[loc] = value
		results[i] = BatchResult{Value: value, Found: true}
	}
	return nil
}
