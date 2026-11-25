package engine

import (
	"bytes"
	"errors"
	"hash/maphash"
	"math"
	"math/bits"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// MaxWriteBatchEntries caps the one-based uint16 slots used by write batches.
const MaxWriteBatchEntries = math.MaxUint16

func CheckBatchCount(count, limit int) error {
	if count > limit {
		return &BatchLimitError{Kind: BatchInputCountLimit, Index: -1, Actual: uint64(count), Limit: uint64(limit)}
	}
	return nil
}

// CheckBatchKey reports the original input position of an empty encoded key.
func CheckBatchKey(operation string, i int, key []byte) error {
	if len(key) == 0 {
		return &BatchKeyError{Operation: operation, Index: i, Err: ErrEmptyKey}
	}
	return nil
}

func (db *DB) addBatchRecordBytes(index int, used, recordBytes uint64) (uint64, error) {
	limit := uint64(min(db.opts.TargetSegmentBytes-format.SegmentHeaderBytes, int64(math.MaxInt)))
	// Used fits a segment minus its header; recordBytes is either a validated
	// Put or the record header plus one native-length delete key.
	actual := used + recordBytes
	if actual > limit {
		return used, &BatchLimitError{Kind: BatchRecordBytesLimit, Index: index, Actual: actual, Limit: limit}
	}
	return actual, nil
}

// A PreparedBatch has passed the count and encoded-size checks for one DB.
// Its encoded input bytes must remain immutable until Commit returns.
type PreparedBatch struct {
	db      *DB
	kind    uint8
	records []storage.Entry
	// Original indices of distinct delete records following the first duplicate.
	// Earlier records have the same positions in the input and the commit.
	deleteInputs []uint16
}

// PreparePuts checks entry and total encoded record byte limits as entryAt supplies each put.
// The caller must check each key with CheckBatchKey before supplying its value.
// No database lock is held while calling entryAt.
func (db *DB) PreparePuts(count int, entryAt func(int) (Entry, error)) (PreparedBatch, error) {
	if err := CheckBatchCount(count, MaxWriteBatchEntries); err != nil {
		return PreparedBatch{}, err
	}
	records := make([]storage.Entry, count)
	var used uint64
	for i := range count {
		entry, err := entryAt(i)
		if err != nil {
			return PreparedBatch{}, err
		}
		keyBytes, valueBytes := uint64(len(entry.Key)), uint64(len(entry.Value))
		if size := db.opts.checkEntryLimits(keyBytes, valueBytes); size != nil {
			return PreparedBatch{}, &BatchLimitError{Kind: BatchEntrySizeLimit, Index: i, Entry: size}
		}
		used, err = db.addBatchRecordBytes(i, used, entrysize.RecordBytes(keyBytes, valueBytes))
		if err != nil {
			return PreparedBatch{}, err
		}
		records[i] = storage.Entry(entry)
	}
	return PreparedBatch{db: db, kind: format.Put, records: records}, nil
}

// PrepareDeletes counts every input but charges only the first occurrence
// of each encoded key. The caller must check each key with CheckBatchKey.
// Existence is checked under the writer gate at commit.
func (db *DB) PrepareDeletes(count int, keyAt func(int) ([]byte, error)) (PreparedBatch, error) {
	if err := CheckBatchCount(count, MaxWriteBatchEntries); err != nil {
		return PreparedBatch{}, err
	}
	// Slots index the original immutable key bytes in records. Small sets
	// need neither a heap-allocated table nor an owned string per distinct key.
	const small = 16
	var inline [2 * small]uint16
	slots := inline[:]
	records := make([]storage.Entry, 0, min(count, small))
	var deleteInputs []uint16
	var used uint64
	for i := range count {
		key, err := keyAt(i)
		if err != nil {
			return PreparedBatch{}, err
		}
		slot := deleteKeySlot(slots, records, key)
		if slots[slot] != 0 {
			continue
		}
		used, err = db.addBatchRecordBytes(i, used, entrysize.RecordBytes(uint64(len(key)), 0))
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
		if i != len(records) {
			deleteInputs = append(deleteInputs, uint16(i))
		}
		records = append(records, storage.Entry{Key: key})
	}
	return PreparedBatch{db: db, kind: format.Delete, records: records, deleteInputs: deleteInputs}, nil
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

// BatchPut commits all entries in input order. A repeated key retains its last value.
func (db *DB) BatchPut(entries []Entry) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	batch, err := db.PreparePuts(len(entries), func(i int) (Entry, error) {
		entry := entries[i]
		return entry, CheckBatchKey("BatchPut", i, entry.Key)
	})
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
	batch, err := db.PrepareDeletes(len(keys), func(i int) ([]byte, error) {
		return keys[i], CheckBatchKey("BatchDelete", i, keys[i])
	})
	if err != nil {
		return err
	}
	return batch.apply()
}

func (b PreparedBatch) apply() error {
	db, kind, records := b.db, b.kind, b.records
	if len(records) == 0 {
		return nil
	}
	previousActive := db.lockMutation()
	invalidatedSealed := false
	defer func() { db.unlockMutation(previousActive, invalidatedSealed) }()
	if err := db.health(); err != nil {
		return err
	}
	op := "batch-put"
	if kind == format.Delete {
		op = "batch-delete"
	}
	tx := db.keys.Begin()
	finished := false
	defer func() {
		if !finished {
			tx.Abort()
		}
	}()
	previous := make([]storage.Location, len(records))
	owned := make([]ownedMutation, len(records))
	prepare := func(i int, loc storage.Location) error {
		owned[i].key = string(records[i].Key)
		old, err := db.stage(tx, kind, owned[i].key, loc)
		previous[i] = old
		return b.indexMutationError(i, err)
	}
	if kind == format.Delete {
		for i := range records {
			if err := prepare(i, storage.Location{}); err != nil {
				if errors.Is(err, ErrNotFound) {
					return err
				}
				return operationError(op, "index-prepare", err)
			}
		}
		prepare = nil
	}
	locations, err := db.store.AppendBatchPrepared(kind, records, db.health, prepare)
	if err != nil {
		db.storageFailure(err)
		return operationError(op, "commit", err)
	}

	for i, record := range records {
		db.copyPutValue(kind, &owned[i], record.Value)
	}

	db.indexGate.Lock()
	defer db.indexGate.Unlock()
	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		return &OpError{Operation: op, Phase: "memory-publication", Outcome: Committed, Err: err}
	}
	tx.Commit()
	finished = true
	for i, entry := range owned {
		if db.applyMutation(kind, entry, locations[i], previous[i]) {
			invalidatedSealed = true
		}
	}
	db.countWriteCommit(previousActive)
	return nil
}
