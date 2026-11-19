package engine

import (
	"bytes"
	"hash/maphash"
	"math"
	"math/bits"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// MaxBatchEntries limits the number of inputs, including duplicate keys.
const MaxBatchEntries = 4096

func CheckBatchCount(count int) error {
	if count > MaxBatchEntries {
		return &BatchLimitError{Kind: BatchInputCountLimit, Index: -1, Actual: uint64(count), Limit: MaxBatchEntries}
	}
	return nil
}

func (db *DB) addBatchBytes(index int, used, recordBytes uint64) (uint64, error) {
	limit := uint64(min(db.opts.TargetSegmentBytes-format.SegmentHeaderBytes, int64(math.MaxInt)))
	// Used fits a segment minus its header; recordBytes is either a validated
	// Put or the record header plus one native-length delete key.
	actual := used + recordBytes
	if actual > limit {
		return used, &BatchLimitError{Kind: BatchPayloadLimit, Index: index, Actual: actual, Limit: limit}
	}
	return actual, nil
}

// A PreparedBatch has passed the count and encoded-size checks for one DB.
// Its encoded input bytes must remain immutable until Commit returns.
type PreparedBatch struct {
	db      *DB
	kind    uint8
	records []storage.Entry
}

// PreparePuts checks entries as the caller produces them. A failure stops
// encoding immediately. No database lock is held while calling entryAt.
func (db *DB) PreparePuts(count int, entryAt func(int) (Entry, error)) (PreparedBatch, error) {
	if err := CheckBatchCount(count); err != nil {
		return PreparedBatch{}, err
	}
	records := make([]storage.Entry, count)
	var used uint64
	for i := range count {
		entry, err := entryAt(i)
		if err != nil {
			return PreparedBatch{}, err
		}
		key, value := uint64(len(entry.Key)), uint64(len(entry.Value))
		if err := db.opts.checkSize(key, value); err != nil {
			if size, ok := err.(*SizeError); ok {
				return PreparedBatch{}, &BatchLimitError{Kind: BatchEntrySizeLimit, Index: i, Entry: size}
			}
			return PreparedBatch{}, err
		}
		used, err = db.addBatchBytes(i, used, entrysize.RecordBytes(key, value))
		if err != nil {
			return PreparedBatch{}, err
		}
		records[i] = storage.Entry(entry)
	}
	return PreparedBatch{db, format.Put, records}, nil
}

// PrepareDeletes counts every input but charges only the first occurrence
// of each encoded key. Existence is checked under the writer gate at commit.
func (db *DB) PrepareDeletes(count int, keyAt func(int) ([]byte, error)) (PreparedBatch, error) {
	if err := CheckBatchCount(count); err != nil {
		return PreparedBatch{}, err
	}
	// Slots index the original immutable key bytes in records. Small sets
	// need neither a heap-allocated table nor an owned string per distinct key.
	const small = 16
	var inline [2 * small]uint16
	slots := inline[:]
	records := make([]storage.Entry, 0, min(count, small))
	var used uint64
	for i := range count {
		key, err := keyAt(i)
		if err != nil {
			return PreparedBatch{}, err
		}
		if len(key) == 0 {
			return PreparedBatch{}, ErrEmptyKey
		}
		slot := deleteKeySlot(slots, records, key)
		if slots[slot] != 0 {
			continue
		}
		used, err = db.addBatchBytes(i, used, entrysize.RecordBytes(uint64(len(key)), 0))
		if err != nil {
			return PreparedBatch{}, err
		}
		if len(records) == cap(records) {
			// A dense, all-distinct prefix predicts the full request well. If
			// duplicates already occurred, grow with the distinct set instead.
			capacity := min(count, 2*len(records))
			if i == len(records) {
				capacity = count
			}
			grown := make([]storage.Entry, len(records), capacity)
			copy(grown, records)
			records = grown
			slots = make([]uint16, 1<<bits.Len(uint(2*capacity-1)))
			for j, record := range records {
				slots[deleteKeySlot(slots, records, record.Key)] = uint16(j + 1)
			}
			slot = deleteKeySlot(slots, records, key)
		}
		slots[slot] = uint16(len(records) + 1)
		records = append(records, storage.Entry{Key: key})
	}
	return PreparedBatch{db, format.Delete, records}, nil
}

// deleteKeySlot returns an empty bucket or the bucket of an equal key.
// A zero slot is empty; other slots hold one-based indices into records.
// The table is a power of two with at least twice as many slots as records.
func deleteKeySlot(slots []uint16, records []storage.Entry, key []byte) int {
	mask := len(slots) - 1
	bucket := int(maphash.Bytes(batchKeySeed, key)) & mask
	for slots[bucket] != 0 && !bytes.Equal(records[slots[bucket]-1].Key, key) {
		bucket = (bucket + 1) & mask
	}
	return bucket
}

// Commit admits a typed call after all application codec calls have returned.
func (b PreparedBatch) Commit() error {
	if err := b.db.admit(); err != nil {
		return err
	}
	defer b.db.done()
	return b.apply()
}

func (b PreparedBatch) apply() error {
	op := "batch-put"
	if b.kind == format.Delete {
		op = "batch-delete"
	}
	return b.db.mutateBatch(op, b.kind, b.records)
}

// BatchPut commits all entries in input order. A repeated key retains its last value.
func (db *DB) BatchPut(entries []Entry) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	batch, err := db.PreparePuts(len(entries), func(i int) (Entry, error) { return entries[i], nil })
	if err != nil {
		return err
	}
	return batch.apply()
}

// BatchDelete requires every distinct key to exist before appending any records.
func (db *DB) BatchDelete(keys [][]byte) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	batch, err := db.PrepareDeletes(len(keys), func(i int) ([]byte, error) { return keys[i], nil })
	if err != nil {
		return err
	}
	return batch.apply()
}

func (db *DB) mutateBatch(op string, kind uint8, records []storage.Entry) error {
	if len(records) == 0 {
		return nil
	}
	previousActive := db.lockMutation()
	invalidatedSealed := false
	defer func() { db.unlockMutation(previousActive, invalidatedSealed) }()
	if err := db.health(); err != nil {
		return err
	}
	if kind == format.Delete {
		db.state.Lock()
		for _, record := range records {
			if _, ok := db.index[string(record.Key)]; !ok {
				db.state.Unlock()
				return ErrNotFound
			}
		}
		db.state.Unlock()
	}
	locations, err := db.store.AppendBatch(kind, records, db.health)
	if err != nil {
		db.storageFailure(err)
		return operationError(op, "commit", err)
	}
	owned := make([]ownedMutation, len(records))
	for i, record := range records {
		owned[i] = db.ownMutation(kind, record.Key, record.Value)
	}

	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		return &OpError{Operation: op, Phase: "memory-publication", Outcome: Committed, Err: err}
	}
	for i, entry := range owned {
		if db.applyMutation(kind, entry, locations[i]) {
			invalidatedSealed = true
		}
	}
	db.countWriteCommit(previousActive)
	return nil
}
