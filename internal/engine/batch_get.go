package engine

import (
	"bytes"
	"hash/maphash"
	"math"
	"math/bits"

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
	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		return nil, err
	}
	db.cache.BeginBatch()
	var firstUncached, uncached int
	var hits uint64
	for i, key := range keys {
		value, found, duplicate := db.cache.PeekBatch(key)
		if !found {
			if uncached == 0 {
				firstUncached = i
			}
			uncached++
			continue
		}
		if !duplicate {
			hits++
		}
		results[i] = BatchResult{Value: value, Found: true}
	}
	duplicates := uint64(len(keys)-uncached) - hits
	db.stats.Cache.Hits += min(hits, math.MaxUint64-db.stats.Cache.Hits)
	db.stats.BatchGetDuplicateInputs += min(duplicates, math.MaxUint64-db.stats.BatchGetDuplicateInputs)
	var reads []batchRead
	if uncached > 0 {
		reads = db.captureBatchMisses(keys, results, firstUncached, uncached)
	}
	db.state.Unlock()

	// Cached values are immutable. Clone after releasing state so each slot
	// owns its result. Batches served entirely here need no pin bookkeeping.
	for i := range results {
		results[i].Value = bytes.Clone(results[i].Value)
	}
	if len(reads) > 0 {
		if err := db.readBatchMisses(keys, results, reads); err != nil {
			return nil, err
		}
	}
	if err := db.health(); err != nil {
		return nil, err
	}
	return results, nil
}

type batchRead struct {
	slot   int
	loc    storage.Location
	source int // Earlier result to clone when loc.File is nil.
}

// captureBatchMisses classifies uncached keys under state. Each live key
// pins one location and schedules one checked read in input order.
func (db *DB) captureBatchMisses(keys [][]byte, results []BatchResult, first, uncached int) []batchRead {
	var reads []batchRead
	var small [64]uint16
	var slots []uint16
	for i := first; i < len(keys); i++ {
		if results[i].Found {
			continue // Already served by the LRU.
		}
		source := first
		if slots == nil && i != first && !bytes.Equal(keys[i], keys[first]) {
			// A single repeated key needs no table. For more keys, keep
			// the table at most half full, using inline space for small batches.
			size := 1 << bits.Len(uint(2*uncached-1))
			if size <= len(small) {
				slots = small[:size]
			} else {
				slots = make([]uint16, size)
			}
			batchKeySlot(slots, keys, first)
		}
		if slots != nil {
			source = batchKeySlot(slots, keys, i)
		}
		if source != i {
			countLookup(&db.stats.BatchGetDuplicateInputs)
			if results[source].Found {
				reads = append(reads, batchRead{slot: i, source: source})
			}
			continue
		}
		loc, ok := db.index[string(keys[i])]
		if !ok {
			countLookup(&db.stats.MissingKeys)
			continue
		}
		countLookup(&db.stats.Cache.Misses)
		loc.File.Pin()
		results[i].Found = true
		if reads == nil {
			reads = make([]batchRead, 0, min(uncached, len(keys)-i))
		}
		reads = append(reads, batchRead{slot: i, loc: loc})
	}
	return reads
}

var batchKeySeed = maphash.MakeSeed()

// The table stores input indices plus one. This bounds scratch memory by
// MaxBatchEntries even when absent keys contain arbitrarily many bytes.
func batchKeySlot(slots []uint16, keys [][]byte, i int) int {
	mask := len(slots) - 1
	bucket := int(maphash.Bytes(batchKeySeed, keys[i])) & mask
	for slots[bucket] != 0 {
		previous := int(slots[bucket]) - 1
		if bytes.Equal(keys[previous], keys[i]) {
			return previous
		}
		bucket = (bucket + 1) & mask
	}
	slots[bucket] = uint16(i + 1)
	return i
}

func (db *DB) readBatchMisses(keys [][]byte, results []BatchResult, reads []batchRead) error {
	defer func() {
		for _, r := range reads {
			if r.loc.File != nil {
				db.releaseScanPin(r.loc.File)
			}
		}
	}()

	for _, r := range reads {
		if r.loc.File == nil {
			results[r.slot] = BatchResult{Value: bytes.Clone(results[r.source].Value), Found: true}
			continue
		}
		value, err := db.store.Read(r.loc, string(keys[r.slot]))
		if err != nil {
			db.poison(err)
			return operationError("batch-get", "read", err)
		}
		results[r.slot].Value = value
	}
	return nil
}
