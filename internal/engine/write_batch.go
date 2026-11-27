package engine

import (
	"errors"
	"slices"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// WriteBatchSizer estimates the space needed by an atomic WriteBatch using
// only the operation lengths and the DB's effective options.
type WriteBatchSizer struct {
	opts        Options
	count       int
	recordBytes uint64
}

func (db *DB) WriteBatchSizer() WriteBatchSizer {
	return WriteBatchSizer{opts: db.opts}
}

// Add charges an encoded key and value length. For Delete, pass zero value
// bytes. Add checks each entry and counts every input at its full encoded
// record size. If the updated plan fits, Add records the operation and returns
// nil. On error the plan is unchanged. Apply checks actual space after
// folding duplicate keys and eliding absent Deletes.
func (s *WriteBatchSizer) Add(keyBytes, valueBytes uint64) error {
	if s.opts.MaxRecordBytes == 0 {
		return errors.New("tarp: initialize WriteBatchSizer with DB.WriteBatchSizer")
	}
	if err := CheckBatchCount(s.count+1, MaxWriteBatchEntries); err != nil {
		return err
	}
	if keyBytes == 0 {
		return &BatchKeyError{Operation: "WriteBatchSizer.Add", Index: s.count, Err: ErrEmptyKey}
	}
	if size := s.opts.checkEntryLimits(keyBytes, valueBytes); size != nil {
		return &BatchLimitError{Kind: BatchEntrySizeLimit, Index: s.count, Entry: size}
	}
	used, err := s.opts.addBatchRecordBytes(s.count, s.recordBytes, entrysize.RecordBytes(keyBytes, valueBytes))
	if err != nil {
		return err
	}
	s.count++
	s.recordBytes = used
	return nil
}

// MixedOp represents one operation supplied to PrepareMixed.
type MixedOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type mixedBatchItem struct {
	record     storage.MixedEntry
	key        string // Owned once per distinct key, shared by the index and cache on commit.
	inputIndex int
}

// A PreparedMixedBatch holds validated, intra-batch-coalesced Put and Delete
// operations ready for atomic publication on one DB.
type PreparedMixedBatch struct {
	db      *DB
	records []mixedBatchItem
	order   []uint16 // One-based record slots in final input order, when reordering is needed.
}

// PrepareMixed validates input count, per-operation key/entry limits in input
// order, folds duplicate encoded keys so the last operation per distinct key
// wins. Commit checks the combined record bytes after resolving absent Deletes.
// No database lock is held while calling opAt.
func (db *DB) PrepareMixed(count int, opAt func(int) (MixedOp, error)) (PreparedMixedBatch, error) {
	if err := CheckBatchCount(count, MaxWriteBatchEntries); err != nil {
		return PreparedMixedBatch{}, err
	}
	if count == 0 {
		return PreparedMixedBatch{db: db}, nil
	}

	positions := make(map[string]int)
	items := make([]mixedBatchItem, 0, min(count, 16))

	for i := range count {
		op, err := opAt(i)
		if err != nil {
			return PreparedMixedBatch{}, err
		}
		record := storage.MixedEntry{Kind: format.Put, Key: op.Key, Value: op.Value}
		if op.Delete {
			record.Kind, record.Value = format.Delete, nil
		}
		if size := db.opts.checkEntryLimits(uint64(len(record.Key)), uint64(len(record.Value))); size != nil {
			return PreparedMixedBatch{}, &BatchLimitError{Kind: BatchEntrySizeLimit, Index: i, Entry: size}
		}

		if pos, found := positions[string(op.Key)]; found {
			items[pos].record = record
			items[pos].inputIndex = i
			continue
		}
		key := string(op.Key)
		positions[key] = len(items)
		items = append(items, mixedBatchItem{record: record, key: key, inputIndex: i})
	}
	batch := PreparedMixedBatch{db: db, records: items}
	if len(items) > 1 && len(items) < count {
		// Sequence assignment and limit errors follow the last input for each
		// key. Compact the slot numbers without moving the records themselves.
		slots := make([]uint16, count)
		for pos := range items {
			slots[items[pos].inputIndex] = uint16(pos + 1)
		}
		batch.order = slices.DeleteFunc(slots, func(slot uint16) bool { return slot == 0 })
	}
	return batch, nil
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
	type change struct {
		entry    ownedMutation
		previous storage.Location
	}
	changes := make([]change, 0, len(records))
	hasPut := false

	var used uint64
	for i := range records {
		pos := i
		if b.order != nil {
			pos = int(b.order[i]) - 1
		}
		item := &records[pos]
		rec := item.record
		var old storage.Location
		var err error
		if rec.Kind == format.Delete {
			old, err = db.stage(tx, format.Delete, item.key, storage.Location{})
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return operationError(op, "index-prepare", err)
			}
		} else {
			hasPut = true
		}
		recordBytes := entrysize.RecordBytes(uint64(len(rec.Key)), uint64(len(rec.Value)))
		used, err = db.opts.addBatchRecordBytes(item.inputIndex, used, recordBytes)
		if err != nil {
			return err
		}
		surviving = append(surviving, rec)
		changes = append(changes, change{entry: ownedMutation{key: item.key}, previous: old})
	}

	if len(surviving) == 0 {
		return nil
	}
	if err := db.checkWriteReserve(op, used); err != nil {
		return err
	}

	var prepare func(int, storage.Location) error
	if hasPut {
		prepare = func(i int, loc storage.Location) error {
			if surviving[i].Kind == format.Delete {
				return nil
			}
			old, err := db.stage(tx, format.Put, changes[i].entry.key, loc)
			changes[i].previous = old
			return err
		}
	}

	locations, err := db.store.AppendMixedBatchPrepared(surviving, db.health, prepare)
	if err != nil {
		db.storageFailure(err)
		return operationError(op, "commit", err)
	}

	for i, rec := range surviving {
		db.copyPutValue(rec.Kind, &changes[i].entry, rec.Value)
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
	for i, c := range changes {
		if db.applyMutation(surviving[i].Kind, c.entry, locations[i], c.previous) {
			invalidatedSealed = true
		}
	}
	db.countWriteCommit(previousActive)
	return nil
}
