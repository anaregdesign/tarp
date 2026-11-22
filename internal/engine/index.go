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

// During a memory checkpoint, staged holds the keys whose old location the
// active writer will use for file accounting at commit. It borrows the owned
// key strings from that single mutation or batch.
type memoryCheckpointRemap struct {
	index  index.ResidentRelocator
	staged map[string]struct{}
}

func openIndex(store *storage.Store, mode IndexMode, budget int64) (index.Backend, error) {
	if mode == IndexMemory {
		return memory.New(), nil
	}
	dir, err := store.WorkspaceDir()
	if err != nil {
		return nil, err
	}
	return btree.NewIn(dir, budget)
}

// Reserve one quarter for backup and another for compaction so that both can
// run concurrently with the foreground index.
func (db *DB) newIndex() (index.Backend, error) {
	return openIndex(db.store, db.opts.IndexMode, db.opts.BTreeMemoryBudgetBytes/4)
}

func (db *DB) indexView() index.View { return db.keys.View() }

func missingIndexFile(id uint64) error {
	return fmt.Errorf("%w: index references absent DATA file %d", format.ErrCorrupt, id)
}

func (db *DB) locationLocked(key string) (storage.Location, bool, error) {
	a, ok, err := db.keys.Lookup(key)
	if err == nil && ok && db.segments[a.FileID] == nil {
		err = missingIndexFile(a.FileID)
	}
	if err != nil {
		db.poison(err)
		return storage.Location{}, false, operationError("index", "lookup", err)
	}
	if !ok {
		return storage.Location{}, false, nil
	}
	return storage.At(db.segments[a.FileID], a), true, nil
}

func (db *DB) lookupLocation(key string) (storage.Location, bool, error) {
	db.indexGate.RLock()
	defer db.indexGate.RUnlock()
	return db.locationLocked(key)
}

// stage runs under the writer gate, before the first selected DATA byte can
// change. Apply returns the previous location, including earlier staged entries.
func (db *DB) stage(tx index.Tx, kind uint8, key string, next storage.Location) (storage.Location, error) {
	if err := db.store.IO.Check("index.prepare:before"); err != nil {
		return storage.Location{}, err
	}
	if db.memoryRemap != nil {
		// Keep the prepared file address stable until this writer publishes or aborts.
		db.indexGate.Lock()
		defer db.indexGate.Unlock()
		db.memoryRemap.staged[key] = struct{}{}
	}
	mutation := index.Mutation{Key: key, Delete: kind == format.Delete}
	if !mutation.Delete {
		mutation.Address = next.Address()
	}
	a, found, err := tx.Apply(mutation)
	if err != nil {
		var lookupErr *index.LookupError
		if errors.As(err, &lookupErr) {
			db.poison(lookupErr.Err)
			return storage.Location{}, err
		}
	}
	var old storage.Location
	if found {
		segment := next.Segment
		if segment == nil || a.FileID != segment.ID {
			segment = db.segments[a.FileID]
		}
		if segment == nil {
			err := missingIndexFile(a.FileID)
			db.poison(err)
			return old, err
		}
		old = storage.At(segment, a)
	}
	if err == nil && mutation.Delete && !found {
		return old, ErrNotFound
	}
	return old, err
}

func (db *DB) dropLocation(segment *storage.DataSegment) {
	if segment.LiveKeys.Add(-1) == 0 {
		delete(db.segments, segment.ID)
	}
}

func (db *DB) addLocation(segment *storage.DataSegment) {
	if segment.LiveKeys.Add(1) == 1 {
		db.segments[segment.ID] = segment
	}
}

// Relocate only records that still point at the copied source. A bounded batch
// amortizes publication and COW without holding the writer gate over spool I/O.
func (db *DB) relocateBatch(batch []storage.Relocation) error {
	if db.memoryRemap != nil {
		if done, err := db.relocateUnstaged(batch); done {
			return err
		}
	}
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
		db.accountRelocation(move)
	}
	return nil
}

func (db *DB) accountRelocation(move storage.Relocation) {
	db.dropLocation(move.Old.Segment)
	db.addLocation(move.Next.Segment)
	move.Old.Segment.LiveBytes.Add(-move.Old.RecordBytes)
	move.Next.Segment.LiveBytes.Add(move.Next.RecordBytes)
}

// A durable writer has already captured file addresses for its staged keys.
// If the batch overlaps, release indexGate before taking the writer lock.
// Otherwise publish under indexGate, then state, in the usual commit order.
func (db *DB) relocateUnstaged(batch []storage.Relocation) (bool, error) {
	db.indexGate.Lock()
	defer db.indexGate.Unlock()
	if len(db.memoryRemap.staged) != 0 {
		for _, move := range batch {
			if _, busy := db.memoryRemap.staged[move.Key]; busy {
				return false, nil
			}
		}
	}
	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		return true, err
	}
	for _, move := range batch {
		if db.memoryRemap.index.Relocate(move.Key, move.Old.Address(), move.Next.Address()) {
			db.accountRelocation(move)
		}
	}
	return true, nil
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
