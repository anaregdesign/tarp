package engine

import (
	"fmt"
	"math"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// MaxBatchEntries limits the number of inputs, including duplicate keys.
const MaxBatchEntries = 4096

func CheckBatchCount(count int) error {
	if count > MaxBatchEntries {
		return fmt.Errorf("%w: batch has %d inputs, limit %d", ErrTooLarge, count, MaxBatchEntries)
	}
	return nil
}

// CheckBatchPutEntry lets typed callers validate entries as their codecs produce them.
func (db *DB) CheckBatchPutEntry(entry Entry, used int64) (int64, error) {
	key, value := uint64(len(entry.Key)), uint64(len(entry.Value))
	if err := db.opts.checkSize(key, value); err != nil {
		return used, err
	}
	return db.addBatchBytes(used, format.RecordHeaderBytes+key+value)
}

// CheckBatchDeleteKey charges a single distinct key to the encoded batch.
func (db *DB) CheckBatchDeleteKey(key []byte, used int64) (int64, error) {
	if len(key) == 0 {
		return used, ErrEmptyKey
	}
	return db.addBatchBytes(used, format.RecordHeaderBytes+uint64(len(key)))
}

func (db *DB) addBatchBytes(used int64, recordBytes uint64) (int64, error) {
	limit := min(db.opts.TargetSegmentBytes-format.SegmentHeaderBytes, int64(math.MaxInt))
	if recordBytes > uint64(limit-used) {
		return used, fmt.Errorf("%w: batch exceeds %d encoded bytes", ErrTooLarge, limit)
	}
	return used + int64(recordBytes), nil
}

// BatchPut commits all entries in input order. A repeated key retains its last value.
func (db *DB) BatchPut(entries []Entry) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if err := CheckBatchCount(len(entries)); err != nil {
		return err
	}
	records := make([]storage.Entry, len(entries))
	var used int64
	for i, entry := range entries {
		var err error
		used, err = db.CheckBatchPutEntry(entry, used)
		if err != nil {
			return err
		}
		records[i] = storage.Entry(entry)
	}
	return db.mutateBatch("batch-put", format.Put, records)
}

// BatchDelete requires every distinct key to exist before appending any records.
func (db *DB) BatchDelete(keys [][]byte) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if err := CheckBatchCount(len(keys)); err != nil {
		return err
	}
	records := make([]storage.Entry, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	var used int64
	for _, key := range keys {
		if _, ok := seen[string(key)]; ok {
			continue
		}
		var err error
		used, err = db.CheckBatchDeleteKey(key, used)
		if err != nil {
			return err
		}
		seen[string(key)] = struct{}{}
		records = append(records, storage.Entry{Key: key})
	}
	return db.mutateBatch("batch-delete", format.Delete, records)
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
	return nil
}
