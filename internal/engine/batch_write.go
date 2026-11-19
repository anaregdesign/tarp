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

func (db *DB) addBatchBytes(used int64, recordBytes uint64) (int64, error) {
	limit := min(db.opts.TargetSegmentBytes-format.SegmentHeaderBytes, int64(math.MaxInt))
	if recordBytes > uint64(limit-used) {
		return used, fmt.Errorf("%w: batch exceeds %d encoded bytes", ErrTooLarge, limit)
	}
	return used + int64(recordBytes), nil
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
	var used int64
	for i := range count {
		entry, err := entryAt(i)
		if err != nil {
			return PreparedBatch{}, err
		}
		key, value := uint64(len(entry.Key)), uint64(len(entry.Value))
		if err := db.opts.checkSize(key, value); err != nil {
			return PreparedBatch{}, err
		}
		used, err = db.addBatchBytes(used, format.RecordHeaderBytes+key+value)
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
	records := make([]storage.Entry, 0, count)
	seen := make(map[string]struct{}, count)
	var used int64
	for i := range count {
		key, err := keyAt(i)
		if err != nil {
			return PreparedBatch{}, err
		}
		if _, ok := seen[string(key)]; ok {
			continue
		}
		if len(key) == 0 {
			return PreparedBatch{}, ErrEmptyKey
		}
		used, err = db.addBatchBytes(used, format.RecordHeaderBytes+uint64(len(key)))
		if err != nil {
			return PreparedBatch{}, err
		}
		seen[string(key)] = struct{}{}
		records = append(records, storage.Entry{Key: key})
	}
	return PreparedBatch{db, format.Delete, records}, nil
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
