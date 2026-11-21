package engine

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

const (
	smallBatchRecordBytes = 4 << 10
	fastBatchReadLimit    = 4 * time.Microsecond
)

func (db *DB) releaseBatchPins(reads []batchRead) {
	for _, r := range reads {
		if r.loc.File != nil {
			db.releaseScanPin(r.loc.File)
		}
	}
}

func (db *DB) readBatchMisses(keys [][]byte, results []BatchResult, reads []batchRead) error {
	defer db.releaseBatchPins(reads)
	if err := db.readBatchData(keys, results, reads); err != nil {
		return err
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

func (db *DB) readBatchData(keys [][]byte, results []BatchResult, reads []batchRead) error {
	// A copy-only plan can arise from duplicate cached nil values.
	for len(reads) > 0 && reads[0].loc.File == nil {
		reads = reads[1:]
	}
	if len(reads) == 0 {
		return nil
	}
	// Limit range planning to a small batch whose first reads are adjacent.
	// Larger plans keep the adaptive per-record scheduler.
	if len(reads) > 1 && len(reads) <= maxAdjacentRecords {
		if n := adjacentCount(reads); n > 1 {
			return db.readGroupedBatch(keys, results, reads, n)
		}
	}
	db.batchReadSlots <- struct{}{}
	if len(reads) == 1 || cap(db.batchReadSlots) == 1 {
		return db.readBatchSerial(keys, results, reads, false)
	}

	var count int
	small := true
	for _, r := range reads {
		if r.loc.File != nil {
			count++
			if r.loc.Bytes > smallBatchRecordBytes {
				small = false
			}
		}
	}
	sampleFirst := small && count > 1
	if sampleFirst && db.fastSmallBatchReads.Load() {
		var err error
		reads, count, err = db.readSmallBatchWhileFast(keys, results, reads, count)
		if err != nil || count == 0 {
			<-db.batchReadSlots
			return err
		}
		// The slow sample already updated the hint. Keep the caller's permit
		// and only schedule descriptors that still need a DATA read.
		sampleFirst = false
	}

	workers := db.reserveBatchWorkers(count)
	if workers == 1 {
		return db.readBatchSerial(keys, results, reads, sampleFirst)
	}
	return db.readBatchParallel(keys, results, reads, workers, sampleFirst)
}

// The caller already holds one permit. Additional workers only take free slots.
func (db *DB) reserveBatchWorkers(count int) int {
	workers := 1
reserve:
	for workers < min(count, cap(db.batchReadSlots)) {
		select {
		case db.batchReadSlots <- struct{}{}:
			workers++
		default:
			break reserve
		}
	}
	return workers
}

// Time the first actual read separately, then up to four reads per observation.
// Return the unread suffix as soon as the caller benefits from trying workers.
func (db *DB) readSmallBatchWhileFast(keys [][]byte, results []BatchResult, reads []batchRead, remaining int) ([]batchRead, int, error) {
	groupSize, inGroup := 1, 0
	var started time.Time
	for i, r := range reads {
		if r.loc.File == nil {
			continue
		}
		if err := db.health(); err != nil {
			return nil, 0, err
		}
		if inGroup == 0 {
			started = time.Now()
		}
		if err := db.readBatchRecord(keys[r.slot], &results[r.slot], r, false); err != nil {
			return nil, 0, err
		}
		inGroup++
		remaining--
		if inGroup == groupSize || remaining == 0 {
			if time.Since(started) > time.Duration(inGroup)*fastBatchReadLimit {
				db.fastSmallBatchReads.Store(false)
				return reads[i+1:], remaining, nil
			}
			groupSize, inGroup = 4, 0
		}
	}
	return nil, 0, nil
}

func (db *DB) readBatchRecord(key []byte, result *BatchResult, r batchRead, sample bool) error {
	var started time.Time
	if sample {
		started = time.Now()
	}
	value, err := db.store.Read(r.loc, string(key))
	if err != nil {
		db.readFailure(err)
		return operationError("batch-get", "read", err)
	}
	if sample {
		db.fastSmallBatchReads.Store(time.Since(started) <= fastBatchReadLimit)
	}
	result.Value = value
	return nil
}

func (db *DB) readBatchSerial(keys [][]byte, results []BatchResult, reads []batchRead, sampleFirst bool) error {
	defer func() { <-db.batchReadSlots }()
	for i, r := range reads {
		if r.loc.File == nil {
			continue
		}
		if err := db.health(); err != nil {
			return err
		}
		if err := db.readBatchRecord(keys[r.slot], &results[r.slot], r, sampleFirst && i == 0); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) readBatchParallel(keys [][]byte, results []BatchResult, reads []batchRead, workers int, sampleFirst bool) error {
	// The cold slots are private until all reads finish. Lend each worker
	// its expected key there; it replaces the key with the checked value.
	// This keeps the caller's slice of key headers out of the worker closures
	// without allocating a second job list.
	for _, r := range reads {
		if r.loc.File != nil {
			results[r.slot].Value = keys[r.slot]
		}
	}
	return db.runBatchWorkers(workers, len(reads), func(i int) error {
		r := reads[i]
		if r.loc.File == nil {
			return nil
		}
		result := &results[r.slot]
		return db.readBatchRecord(result.Value, result, r, sampleFirst && i == 0)
	})
}

func (db *DB) runBatchWorkers(workers, count int, read func(int) error) error {
	var next atomic.Int32
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var readErr error
	work := func() {
		defer func() { <-db.batchReadSlots }()
		for db.failed.Load() == nil {
			i := int(next.Add(1)) - 1
			if i >= count {
				return
			}
			if err := read(i); err != nil {
				next.Store(int32(count))
				errMu.Lock()
				// A sibling already in flight may report a DB-wide failure.
				if readErr == nil || storage.IsDataUnavailable(readErr) && !storage.IsDataUnavailable(err) {
					readErr = err
				}
				errMu.Unlock()
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
