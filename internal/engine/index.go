package engine

import (
	"errors"
	"fmt"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
	"github.com/anaregdesign/tarp/internal/storage"
)

func openIndex(mode IndexMode, budget int64) (index.Backend, error) {
	if mode == IndexMemory {
		return memory.New(), nil
	}
	return btree.New(budget)
}

// Reserve one quarter for backup and another for compaction so that both can
// run concurrently with the foreground index.
func (db *DB) newIndex() (index.Backend, error) {
	return openIndex(db.opts.IndexMode, db.opts.BTreeMemoryBudgetBytes/4)
}

func (db *DB) indexView() index.View { return db.keys.View() }

func missingIndexFile(id uint64) error {
	return fmt.Errorf("%w: index references absent DATA file %d", format.ErrCorrupt, id)
}

func (db *DB) locationLocked(key string) (storage.Location, bool, error) {
	a, ok, err := db.keys.Lookup(key)
	if err == nil && ok && db.files[a.FileID] == nil {
		err = missingIndexFile(a.FileID)
	}
	if err != nil {
		db.poison(err)
		return storage.Location{}, false, operationError("index", "lookup", err)
	}
	if !ok {
		return storage.Location{}, false, nil
	}
	return storage.At(db.files[a.FileID], a), true, nil
}

func (db *DB) lookupLocation(key string) (storage.Location, bool, error) {
	db.indexGate.RLock()
	defer db.indexGate.RUnlock()
	return db.locationLocked(key)
}

// stage runs under the writer gate, before the first selected DATA byte can
// change. Earlier entries of a repeated-key batch are visible to this lookup.
func (db *DB) stage(tx index.Tx, kind uint8, key string, next storage.Location) (storage.Location, error) {
	if err := db.store.IO.Check("index.prepare:before"); err != nil {
		return storage.Location{}, err
	}
	a, found, err := tx.Lookup(key)
	if err != nil {
		db.poison(err)
		return storage.Location{}, err
	}
	var old storage.Location
	if found {
		file := next.File
		if file == nil || a.FileID != file.ID {
			file = db.files[a.FileID]
		}
		if file == nil {
			err := missingIndexFile(a.FileID)
			db.poison(err)
			return old, err
		}
		old = storage.At(file, a)
	}
	if kind == format.Delete {
		if !found {
			return old, ErrNotFound
		}
		err = tx.Delete(key)
	} else {
		err = tx.Put(key, next.Address())
	}
	return old, err
}

func (db *DB) dropLocation(file *storage.File) {
	if file.LiveKeys.Add(-1) == 0 {
		delete(db.files, file.ID)
	}
}

func (db *DB) addLocation(file *storage.File) {
	if file.LiveKeys.Add(1) == 1 {
		db.files[file.ID] = file
	}
}

// Relocate only records that still point at the copied source. A bounded batch
// amortizes publication and COW without holding the writer gate over spool I/O.
func (db *DB) relocateBatch(batch []storage.Relocation) error {
	db.writer.Lock()
	defer db.writer.Unlock()
	if err := db.health(); err != nil {
		return err
	}
	tx := db.keys.Begin()
	var changed []storage.Relocation
	for _, move := range batch {
		a, found, err := tx.Lookup(move.Key)
		if err != nil {
			tx.Abort()
			return err
		}
		if !found || a != move.Old.Address() {
			continue
		}
		if err := tx.Put(move.Key, move.Next.Address()); err != nil {
			tx.Abort()
			return err
		}
		changed = append(changed, move)
	}
	if len(changed) == 0 {
		tx.Abort()
		return nil
	}
	db.indexGate.Lock()
	defer db.indexGate.Unlock()
	db.state.Lock()
	defer db.state.Unlock()
	tx.Commit()
	for _, move := range changed {
		db.dropLocation(move.Old.File)
		db.addLocation(move.Next.File)
		move.Old.File.LiveBytes.Add(-move.Old.Bytes)
		move.Next.File.LiveBytes.Add(move.Next.Bytes)
	}
	return nil
}

// indexMutationError preserves the original input number in a failed batch delete.
func (b PreparedBatch) indexMutationError(i int, err error) error {
	if b.kind == format.Delete && errors.Is(err, ErrNotFound) {
		if prefix := len(b.records) - len(b.deleteInputs); i >= prefix {
			i = int(b.deleteInputs[i-prefix])
		}
		return &BatchKeyError{Operation: "BatchDelete", Index: i, Err: ErrNotFound}
	}
	return err
}
