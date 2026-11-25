package engine

import (
	"bytes"
	"errors"
	"hash/maphash"
	"math/bits"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// MixedOp represents one operation supplied to PrepareMixed.
type MixedOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type coalescedMixedItem struct {
	record      storage.MixedEntry
	recordBytes uint64
	inputIndex  int
	superseded  bool
}

// A PreparedMixedBatch holds validated, intra-batch-coalesced Put and Delete
// operations ready for atomic publication on one DB.
type PreparedMixedBatch struct {
	db      *DB
	records []storage.MixedEntry
}

// PrepareMixed validates input count, per-operation key/entry limits in input
// order, folds duplicate encoded keys so the last operation per distinct key
// wins, and checks the combined encoded record byte limit of the coalesced set.
// No database lock is held while calling opAt.
func (db *DB) PrepareMixed(count int, opAt func(int) (MixedOp, error)) (PreparedMixedBatch, error) {
	if err := CheckBatchCount(count); err != nil {
		return PreparedMixedBatch{}, err
	}
	if count == 0 {
		return PreparedMixedBatch{db: db}, nil
	}

	const small = 16
	var inline [2 * small]uint16
	slots := inline[:]
	items := make([]coalescedMixedItem, 0, min(count, small))
	distinct := 0

	for i := range count {
		op, err := opAt(i)
		if err != nil {
			return PreparedMixedBatch{}, err
		}
		kind := uint8(format.Put)
		val := op.Value
		if op.Delete {
			kind = format.Delete
			val = nil
		}
		keyBytes, valBytes := uint64(len(op.Key)), uint64(len(val))
		if size := db.opts.checkEntryLimits(keyBytes, valBytes); size != nil {
			return PreparedMixedBatch{}, &BatchLimitError{Kind: BatchEntrySizeLimit, Index: i, Entry: size}
		}
		recBytes := entrysize.RecordBytes(keyBytes, valBytes)

		slot := mixedKeySlot(slots, items, op.Key)
		if prev := slots[slot]; prev != 0 {
			items[prev-1].superseded = true
			items = append(items, coalescedMixedItem{
				record:      storage.MixedEntry{Kind: kind, Key: op.Key, Value: val},
				recordBytes: recBytes,
				inputIndex:  i,
			})
			slots[slot] = uint16(len(items))
			continue
		}

		distinct++
		if distinct > len(slots)/2 {
			slots = make([]uint16, 1<<bits.Len(uint(2*min(count, max(2*distinct, small))-1)))
			for idx := range items {
				if !items[idx].superseded {
					slots[mixedKeySlot(slots, items, items[idx].record.Key)] = uint16(idx + 1)
				}
			}
			slot = mixedKeySlot(slots, items, op.Key)
		}
		items = append(items, coalescedMixedItem{
			record:      storage.MixedEntry{Kind: kind, Key: op.Key, Value: val},
			recordBytes: recBytes,
			inputIndex:  i,
		})
		slots[slot] = uint16(len(items))
	}

	records := make([]storage.MixedEntry, 0, distinct)
	var used uint64
	for i := range items {
		if items[i].superseded {
			continue
		}
		var err error
		used, err = db.addBatchRecordBytes(items[i].inputIndex, used, items[i].recordBytes)
		if err != nil {
			return PreparedMixedBatch{}, err
		}
		records = append(records, items[i].record)
	}
	return PreparedMixedBatch{db: db, records: records}, nil
}

func mixedKeySlot(slots []uint16, items []coalescedMixedItem, key []byte) int {
	mask := len(slots) - 1
	bucket := int(maphash.Bytes(batchKeySeed, key)) & mask
	for slots[bucket] != 0 && !bytes.Equal(items[slots[bucket]-1].record.Key, key) {
		bucket = (bucket + 1) & mask
	}
	return bucket
}

// Commit admits a prepared mixed batch after all application codec calls return.
func (b PreparedMixedBatch) Commit() error {
	if err := b.db.admit(); err != nil {
		return err
	}
	defer b.db.done()
	if err := b.db.health(); err != nil {
		return err
	}
	return b.apply()
}

// ApplyMixed validates, coalesces, and durably commits a mixed WriteBatch.
func (db *DB) ApplyMixed(count int, opAt func(int) MixedOp) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if err := db.health(); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	batch, err := db.PrepareMixed(count, func(i int) (MixedOp, error) {
		op := opAt(i)
		return op, CheckBatchKey("Apply", i, op.Key)
	})
	if err != nil {
		return err
	}
	return batch.apply()
}

func (b PreparedMixedBatch) apply() error {
	db, records := b.db, b.records
	if len(records) == 0 {
		return nil
	}
	previousActive := db.lockMutation()
	invalidatedSealed := false
	defer func() { db.unlockMutation(previousActive, invalidatedSealed) }()
	if err := db.health(); err != nil {
		return err
	}

	const op = "apply"
	tx := db.keys.Begin()
	finished := false
	defer func() {
		if !finished {
			tx.Abort()
		}
	}()

	// Elide Delete operations on keys that are not currently live so recovery
	// never encounters an unpruned tombstone without a preceding live key.
	surviving := make([]storage.MixedEntry, 0, len(records))
	owned := make([]ownedMutation, 0, len(records))
	previous := make([]storage.Location, 0, len(records))
	hasPut := false

	for _, rec := range records {
		keyStr := string(rec.Key)
		if rec.Kind == format.Delete {
			old, err := db.stage(tx, format.Delete, keyStr, storage.Location{})
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return operationError(op, "index-prepare", err)
			}
			surviving = append(surviving, rec)
			owned = append(owned, ownedMutation{key: keyStr})
			previous = append(previous, old)
			continue
		}
		hasPut = true
		surviving = append(surviving, rec)
		owned = append(owned, ownedMutation{key: keyStr})
		previous = append(previous, storage.Location{})
	}

	if len(surviving) == 0 {
		return nil
	}

	var prepare func(int, storage.Location) error
	if hasPut {
		prepare = func(i int, loc storage.Location) error {
			if surviving[i].Kind == format.Delete {
				return nil
			}
			old, err := db.stage(tx, format.Put, owned[i].key, loc)
			previous[i] = old
			return err
		}
	}

	locations, err := db.store.AppendMixedBatchPrepared(surviving, db.health, prepare)
	if err != nil {
		db.storageFailure(err)
		return operationError(op, "commit", err)
	}

	for i, rec := range surviving {
		db.copyPutValue(rec.Kind, &owned[i], rec.Value)
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
		if db.applyMutation(surviving[i].Kind, entry, locations[i], previous[i]) {
			invalidatedSealed = true
		}
	}
	db.countWriteCommit(previousActive)
	return nil
}
