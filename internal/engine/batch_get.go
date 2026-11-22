package engine

import (
	"bytes"
	"hash/maphash"
	"math"
	"math/bits"

	"github.com/anaregdesign/tarp/internal/index"
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
	for i, key := range keys {
		if err := CheckBatchKey("BatchGet", i, key); err != nil {
			return nil, err
		}
	}
	return db.readBatch(keys)
}

// BatchGetPrepared reads encoded keys after the typed caller has checked the
// input count and each key. Admission follows all application codec calls.
func (db *DB) BatchGetPrepared(keys [][]byte) ([]BatchResult, error) {
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()
	return db.readBatch(keys)
}

func (db *DB) readBatch(keys [][]byte) ([]BatchResult, error) {
	results := make([]BatchResult, len(keys))
	reads, err := db.captureBatch(keys, results)
	if err != nil {
		return nil, err
	}

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

type batchRead struct {
	loc    storage.Location
	slot   uint16
	source uint16 // Earlier result to clone when loc.File is nil.
}

// peekBatch only borrows immutable values. It does not change LRU order or
// user-visible counters, so a rejected or retried capture remains invisible.
func (db *DB) peekBatch(keys [][]byte, results []BatchResult, stopOnMiss bool) (hits, uncached int) {
	db.cache.BeginBatch()
	for i, key := range keys {
		value, found, duplicate := db.cache.PeekBatch(key)
		results[i] = BatchResult{Value: value, Found: found}
		if !found {
			uncached++
			if stopOnMiss {
				return hits, uncached
			}
		} else if !duplicate {
			hits++
		}
	}
	return hits, uncached
}

func (db *DB) batchCounts(hits, misses, absent, duplicate int) {
	add := func(p *uint64, n int) { *p += min(uint64(n), math.MaxUint64-*p) }
	add(&db.stats.ValueCache.Hits, hits)
	add(&db.stats.ValueCache.Misses, misses)
	add(&db.stats.MissingKeyLookups, absent)
	add(&db.stats.BatchGetDuplicateInputs, duplicate)
}

func checkBatchGetValueBytes(count int, limit, maximum int64, size func(int) int64) error {
	high, upper := bits.Mul64(uint64(count), uint64(maximum))
	if high == 0 && upper <= uint64(limit) {
		return nil
	}
	var used uint64
	for i := range count {
		used += uint64(size(i))
		if used > uint64(limit) {
			return &BatchLimitError{Kind: BatchGetValueBytesLimit, Index: i, Actual: used, Limit: uint64(limit)}
		}
	}
	return nil
}

func (db *DB) captureBatch(keys [][]byte, results []BatchResult) ([]batchRead, error) {
	// All-hit requests avoid the index and its publication gate entirely.
	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		return nil, err
	}
	hits, uncached := db.peekBatch(keys, results, true)
	if uncached == 0 {
		err := checkBatchGetValueBytes(len(keys), db.opts.MaxBatchGetValueBytes, db.maxSeenValueBytes, func(i int) int64 { return int64(len(results[i].Value)) })
		if err == nil {
			db.batchCounts(hits, 0, 0, len(keys)-hits)
		}
		db.state.Unlock()
		return nil, err
	}
	if db.resident != nil {
		reads, err := db.captureResidentMisses(keys, results)
		db.state.Unlock()
		return reads, err
	}
	db.state.Unlock()

	db.indexGate.RLock()
	defer db.indexGate.RUnlock()
	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		return nil, err
	}
	hits, uncached = db.peekBatch(keys, results, false)
	maximum := db.maxSeenValueBytes
	db.state.Unlock()

	// Use the caller's stable key slices for deduplication. Only copy keys
	// that can represent live records, in bounded chunks for grouped lookup.
	source := make([]uint16, len(keys))
	addresses := make([]index.Address, len(keys))
	var distinctKeys batchKeySet
	var group []string
	var positions []int
	groupBytes := 0
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
		if many, ok := db.keys.(index.BatchView); ok {
			var invalid error
			if err := many.LookupMany(group, func(i int, loc index.Address, found bool) {
				if found {
					if loc.FileID == 0 {
						invalid = missingIndexFile(0)
						return
					}
					addresses[positions[i]] = loc
				}
			}); err != nil {
				return err
			}
			if invalid != nil {
				return invalid
			}
		} else {
			for i, k := range group {
				a, ok, err := db.keys.Lookup(k)
				if err != nil {
					return err
				}
				if ok {
					if a.FileID == 0 {
						return missingIndexFile(0)
					}
					addresses[positions[i]] = a
				}
			}
		}
		clear(group)
		group = group[:0]
		positions = positions[:0]
		groupBytes = 0
		return nil
	}
	var distinct int
	for i, k := range keys {
		if results[i].Found {
			continue
		}
		first := distinctKeys.source(keys, i, uncached)
		source[i] = uint16(first)
		if first != i {
			continue
		}
		distinct++
		if int64(len(k)) > db.opts.MaxRecordBytes {
			continue
		}
		if groupBytes > 0 && len(k) > 256<<10-groupBytes {
			if err := flush(); err != nil {
				db.poison(err)
				return nil, operationError("batch-get", "index", err)
			}
		}
		group = append(group, string(k))
		positions = append(positions, i)
		groupBytes += len(k)
	}
	if err := flush(); err != nil {
		db.poison(err)
		return nil, operationError("batch-get", "index", err)
	}
	err := checkBatchGetValueBytes(len(keys), db.opts.MaxBatchGetValueBytes, maximum, func(i int) int64 {
		if results[i].Found {
			return int64(len(results[i].Value))
		}
		return addresses[source[i]].ValueBytes
	})
	if err != nil {
		return nil, err
	}
	var reads []batchRead
	var misses int
	for i := range keys {
		if results[i].Found {
			continue
		}
		a := addresses[source[i]]
		if a.FileID == 0 {
			continue
		}
		results[i].Found = true
		r := batchRead{slot: uint16(i), source: source[i]}
		if int(source[i]) == i {
			file := db.files[a.FileID]
			if file == nil {
				return nil, db.rejectBatchFile(reads, a.FileID)
			}
			file.Pin()
			r.loc = storage.At(file, a)
			misses++
		}
		reads = append(reads, r)
	}
	db.state.Lock()
	db.batchCounts(hits, misses, distinct-misses, len(keys)-distinct-hits)
	db.state.Unlock()
	return reads, nil
}

func (db *DB) rejectBatchFile(reads []batchRead, id uint64) error {
	db.releaseBatchPins(reads)
	err := missingIndexFile(id)
	db.poison(err)
	return operationError("batch-get", "index", err)
}

// captureResidentMisses runs entirely under state. A missing first key favors
// probing the hash index first, avoiding an LRU lookup for remaining absent keys.
func (db *DB) captureResidentMisses(keys [][]byte, results []BatchResult) ([]batchRead, error) {
	if err := checkBatchGetValueBytes(len(keys), db.opts.MaxBatchGetValueBytes, db.maxSeenValueBytes, func(i int) int64 {
		a, _ := db.resident.LookupResident(keys[i])
		return a.ValueBytes
	}); err != nil {
		return nil, err
	}
	indexFirst := false
	if !results[0].Found {
		_, live := db.resident.LookupResident(keys[0])
		indexFirst = !live
	}
	var reads []batchRead
	var distinctKeys batchKeySet
	var hits, missing, misses, duplicates int
	for i, key := range keys {
		source := distinctKeys.source(keys, i, len(keys))
		if i != source {
			duplicates++
			results[i] = results[source]
			if results[i].Found && results[i].Value == nil {
				// A cached empty value can also be nil. A duplicate read of
				// its empty result is safe and performs no DATA I/O.
				reads = append(reads, batchRead{slot: uint16(i), source: uint16(source)})
			}
			continue
		}
		if !indexFirst {
			if v, ok := db.cache.Peek(key); ok {
				results[i] = BatchResult{Value: v, Found: true}
				hits++
				continue
			}
		}
		addr, found := db.resident.LookupResident(key)
		if !found {
			missing++
			continue
		}
		if indexFirst {
			if v, ok := db.cache.Peek(key); ok {
				results[i] = BatchResult{Value: v, Found: true}
				hits++
				continue
			}
		}
		misses++
		file := db.files[addr.FileID]
		if file == nil {
			return nil, db.rejectBatchFile(reads, addr.FileID)
		}
		file.Pin()
		results[i] = BatchResult{Found: true}
		if reads == nil {
			reads = make([]batchRead, 0, len(keys)-i)
		}
		reads = append(reads, batchRead{slot: uint16(i), loc: storage.At(file, addr)})
	}
	db.batchCounts(hits, misses, missing, duplicates)
	return reads, nil
}

// Most batches of one repeated key need no hash table. Size the table for the
// remaining inputs on the first distinct key. Sampling an unequal suffix byte
// keeps common-prefix and common-suffix workloads on the cheap first probe.
type batchKeySet struct {
	inline                     [64]uint16
	spill                      []uint16
	firstPlusOne, sample, size int
}

func (s *batchKeySet) source(keys [][]byte, i, count int) int {
	if s.firstPlusOne == 0 {
		s.firstPlusOne = i + 1
		return i
	}
	first := s.firstPlusOne - 1
	slots := s.inline[:]
	if s.size == 0 {
		if bytes.Equal(keys[i], keys[first]) {
			return first
		}
		s.size = 1 << bits.Len(uint(2*count-1))
		if s.size > len(slots) {
			s.spill = make([]uint16, s.size)
			slots = s.spill
		}
		s.sample = min(len(keys[i]), len(keys[first])) - 1
		for limit := max(0, s.sample-31); s.sample > limit && keys[i][s.sample] == keys[first][s.sample]; s.sample-- {
		}
		batchKeySlot(slots[:s.size], keys, first, s.sample)
	} else if s.spill != nil {
		slots = s.spill
	}
	return batchKeySlot(slots[:s.size], keys, i, s.sample)
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
