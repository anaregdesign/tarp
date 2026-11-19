package engine

import (
	"bytes"
	"sync"
	"sync/atomic"
)

func (db *DB) readBatchMisses(keys [][]byte, results []BatchResult, reads []batchRead) error {
	defer func() {
		for _, r := range reads {
			if r.loc.File != nil {
				db.releaseScanPin(r.loc.File)
			}
		}
	}()

	workers := db.reserveBatchReaders(reads)
	if workers == 1 {
		if err := db.readBatchSerial(keys, results, reads); err != nil {
			return err
		}
	} else if workers > 1 {
		if err := db.readBatchParallel(keys, results, reads, workers); err != nil {
			return err
		}
	}
	// Source slots now contain their checked values. Each duplicate owns its
	// bytes, regardless of the order in which workers finished reading.
	for _, r := range reads {
		if r.loc.File == nil {
			results[r.slot] = BatchResult{Value: bytes.Clone(results[r.source].Value), Found: true}
		}
	}
	return nil
}

// Reserve one permit for the caller and take extra permits only if already
// available. Batches never wait for another permit while holding one.
func (db *DB) reserveBatchReaders(reads []batchRead) int {
	workers := 0
	for _, r := range reads {
		if r.loc.File == nil {
			continue
		}
		if workers == 0 {
			db.batchReadSlots <- struct{}{}
		} else {
			select {
			case db.batchReadSlots <- struct{}{}:
			default:
				return workers
			}
		}
		workers++
		if workers == cap(db.batchReadSlots) {
			break
		}
	}
	return workers
}

func (db *DB) readBatchRecord(key []byte, result *BatchResult, r batchRead) error {
	value, err := db.store.Read(r.loc, string(key))
	if err != nil {
		db.poison(err)
		return operationError("batch-get", "read", err)
	}
	result.Value = value
	return nil
}

func (db *DB) readBatchSerial(keys [][]byte, results []BatchResult, reads []batchRead) error {
	defer func() { <-db.batchReadSlots }()
	for _, r := range reads {
		if r.loc.File == nil {
			continue
		}
		if err := db.health(); err != nil {
			return err
		}
		if err := db.readBatchRecord(keys[r.slot], &results[r.slot], r); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) readBatchParallel(keys [][]byte, results []BatchResult, reads []batchRead, workers int) error {
	// The cold slots are private until all reads finish. Lend each worker
	// its expected key there; it replaces the key with the checked value.
	// This keeps the caller's slice of key headers out of the worker closures
	// without allocating a second job list.
	for _, r := range reads {
		if r.loc.File != nil {
			results[r.slot].Value = keys[r.slot]
		}
	}
	var next atomic.Int32
	var wg sync.WaitGroup
	var once sync.Once
	var readErr error
	work := func() {
		defer func() { <-db.batchReadSlots }()
		for db.failed.Load() == nil {
			i := int(next.Add(1)) - 1
			if i >= len(reads) {
				return
			}
			r := reads[i]
			if r.loc.File == nil {
				continue
			}
			result := &results[r.slot]
			if err := db.readBatchRecord(result.Value, result, r); err != nil {
				once.Do(func() { readErr = err })
				return
			}
		}
	}
	for i := 1; i < workers; i++ {
		wg.Go(work)
	}
	work()
	wg.Wait()
	if readErr != nil {
		return readErr
	}
	return db.health()
}
