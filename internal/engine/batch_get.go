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
	if err := db.checkBatchResultSize(keys); err != nil {
		db.state.Unlock()
		return nil, err
	}
	reads := db.captureBatch(keys, results)
	db.state.Unlock()

	// Cached values are immutable. Clone after releasing state so each slot
	// owns its result. Batches served entirely here need no pin bookkeeping.
	for i := range results {
		if results[i].Value != nil {
			results[i].Value = bytes.Clone(results[i].Value)
		}
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

// checkBatchResultSize runs under the snapshot lock, before pinning files,
// changing counters or copying values. Skip the index walk when even a batch
// repeating the largest observed value would fit.
func (db *DB) checkBatchResultSize(keys [][]byte) error {
	limit := uint64(db.opts.MaxBatchResultBytes)
	high, upper := bits.Mul64(uint64(len(keys)), uint64(db.maxSeenValueBytes))
	if high == 0 && upper <= limit {
		return nil
	}
	var actual uint64
	for i, key := range keys {
		// Before adding, actual is at most the int64 limit. Adding another
		// nonnegative int64 therefore cannot overflow uint64.
		actual += uint64(db.index[string(key)].ValueBytes)
		if actual > limit {
			return &BatchLimitError{Kind: BatchResultBytesLimit, Index: i, Actual: actual, Limit: limit}
		}
	}
	return nil
}

func (db *DB) captureBatch(keys [][]byte, results []BatchResult) []batchRead {
	if len(keys) == 1 {
		if value, found := db.cache.Peek(keys[0]); found {
			countLookup(&db.stats.Cache.Hits)
			results[0] = BatchResult{Value: value, Found: true}
			return nil
		}
		return db.captureBatchKeys(keys, results, 0, 1, false)
	}
	db.cache.BeginBatch()
	var firstUncached, uncached int
	var hits uint64
	for i, key := range keys {
		value, found, duplicate := db.cache.PeekBatch(key)
		if !found {
			if i == 0 {
				if _, live := db.index[string(key)]; !live {
					// Lead with the index on missing-key requests: absent
					// keys then need no further LRU probes.
					return db.captureBatchKeys(keys, results, 0, len(keys), true)
				}
			}
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
	if uncached > 0 {
		return db.captureBatchKeys(keys, results, firstUncached, uncached, false)
	}
	return nil
}

type batchRead struct {
	loc    storage.Location
	slot   uint16
	source uint16 // Earlier result to clone when loc.File is nil.
}

// captureBatchKeys classifies the remaining keys under state. Each cold live
// key pins one location and schedules one checked read in input order.
func (db *DB) captureBatchKeys(keys [][]byte, results []BatchResult, first, remaining int, checkCache bool) []batchRead {
	var reads []batchRead
	var missing, duplicates uint64
	var small [64]uint16
	var slots []uint16
	var sample int
	for i := first; i < len(keys); i++ {
		if results[i].Found {
			continue // Already served by the LRU.
		}
		source := first
		if slots == nil && i != first && !bytes.Equal(keys[i], keys[first]) {
			// A single repeated key needs no table. For more keys, keep
			// the table at most half full, using inline space for small batches.
			size := 1 << bits.Len(uint(2*remaining-1))
			if size <= len(small) {
				slots = small[:size]
			} else {
				slots = make([]uint16, size)
			}
			// Look for a differing byte near the end of the shorter key.
			// A shared suffix can then still use the inexpensive first probe.
			sample = min(len(keys[i]), len(keys[first])) - 1
			limit := max(0, sample-31)
			for sample > limit && keys[i][sample] == keys[first][sample] {
				sample--
			}
			batchKeySlot(slots, keys, first, sample)
		}
		if slots != nil {
			source = batchKeySlot(slots, keys, i, sample)
		}
		if source != i {
			duplicates++
			if results[source].Value != nil {
				results[i] = results[source]
			} else if results[source].Found {
				reads = append(reads, batchRead{slot: uint16(i), source: uint16(source)})
			}
			continue
		}
		loc, ok := db.index[string(keys[i])]
		if !ok {
			missing++
			continue
		}
		if checkCache {
			if value, found, _ := db.cache.PeekBatch(keys[i]); found {
				countLookup(&db.stats.Cache.Hits)
				results[i] = BatchResult{Value: value, Found: true}
				continue
			}
		}
		countLookup(&db.stats.Cache.Misses)
		loc.File.Pin()
		results[i].Found = true
		if reads == nil {
			reads = make([]batchRead, 0, min(remaining, len(keys)-i))
		}
		reads = append(reads, batchRead{slot: uint16(i), loc: loc})
	}
	db.stats.MissingKeys += min(missing, math.MaxUint64-db.stats.MissingKeys)
	db.stats.BatchGetDuplicateInputs += min(duplicates, math.MaxUint64-db.stats.BatchGetDuplicateInputs)
	return reads
}

var batchKeySeed = maphash.MakeSeed()

const _ uint16 = MaxBatchEntries // One-based input positions must fit in slots.

// The table stores input indices plus one. This bounds scratch memory by
// MaxBatchEntries even when absent keys contain arbitrarily many bytes.
func batchKeySlot(slots []uint16, keys [][]byte, i, sample int) int {
	key := keys[i]
	mask := len(slots) - 1
	// Try one inexpensive bucket, then use a seeded probe sequence on a collision.
	pos := min(sample, len(key)-1)
	bucket := int(key[pos])
	if pos > 0 {
		bucket += 31 * int(key[pos-1])
	}
	bucket &= mask
	if prior := slots[bucket]; prior != 0 {
		previous := int(prior) - 1
		if bytes.Equal(keys[previous], key) {
			return previous
		}
		hash := maphash.Bytes(batchKeySeed, key)
		bucket = int(hash) & mask
		step := int(hash>>32) | 1 // An odd stride visits every power-of-two bucket.
		for slots[bucket] != 0 {
			previous = int(slots[bucket]) - 1
			if bytes.Equal(keys[previous], key) {
				return previous
			}
			bucket = (bucket + step) & mask
		}
	}
	slots[bucket] = uint16(i + 1)
	return i
}
