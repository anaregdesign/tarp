package engine

import (
	"math"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

const maxAdjacentRecords = 32

// adjacentCount finds a bounded, gapless physical run in the captured order.
// Duplicate result slots and segment boundaries end a run.
func adjacentCount(reads []batchRead) int {
	first := reads[0].loc
	if first.File == nil || first.Bytes <= 0 || first.Bytes >= storage.MaxAdjacentReadBytes || first.Offset < 0 {
		return 1
	}
	total := first.Bytes
	for i := 1; i < min(len(reads), maxAdjacentRecords); i++ {
		next := reads[i].loc
		if next.File != first.File || first.Offset > math.MaxInt64-total || next.Offset != first.Offset+total ||
			next.Bytes <= 0 || next.Bytes > storage.MaxAdjacentReadBytes-total {
			return i
		}
		total += next.Bytes
	}
	return min(len(reads), maxAdjacentRecords)
}

// A bounded run is one I/O job. Multiple runs and nonadjacent singletons share
// the same worker and permit limits as individual reads.
func (db *DB) readGroupedBatch(keys [][]byte, results []BatchResult, reads []batchRead, firstWidth int) error {
	db.batchReadSlots <- struct{}{}
	// Workers borrow expected keys through private output slots just as they do
	// for individual reads. The caller's slice of key headers stays on its stack.
	for _, r := range reads {
		if r.loc.File != nil {
			results[r.slot].Value = keys[r.slot]
		}
	}
	if firstWidth == len(reads) {
		defer func() { <-db.batchReadSlots }()
		if err := db.health(); err != nil {
			return err
		}
		return db.readAdjacent(results, reads)
	}
	type job struct{ start, end uint16 }
	var local [maxAdjacentRecords]job
	jobs := append(local[:0], job{0, uint16(firstWidth)})
	for i := firstWidth; i < len(reads); {
		if reads[i].loc.File == nil {
			i++
			continue
		}
		width := adjacentCount(reads[i:])
		jobs = append(jobs, job{uint16(i), uint16(i + width)})
		i += width
	}
	read := func(j job) error {
		if j.end-j.start == 1 {
			r := reads[j.start]
			return db.readBatchRecord(results[r.slot].Value, &results[r.slot], r, false)
		}
		return db.readAdjacent(results, reads[j.start:j.end])
	}
	// Warm contiguous runs often finish faster than starting more workers.
	// After a slow observation, keep the permit and parallelize the remainder.
	for len(jobs) > 0 && db.fastSmallBatchReads.Load() {
		if err := db.health(); err != nil {
			<-db.batchReadSlots
			return err
		}
		j := jobs[0]
		start := time.Now()
		err := read(j)
		if err != nil {
			<-db.batchReadSlots
			return err
		}
		db.fastSmallBatchReads.Store(time.Since(start) <= fastBatchReadLimit)
		jobs = jobs[1:]
	}
	if len(jobs) == 0 {
		<-db.batchReadSlots
		return nil
	}
	workers := db.reserveBatchWorkers(len(jobs))
	return db.runBatchWorkers(workers, len(jobs), func(i int) error {
		if i != 0 {
			return read(jobs[i])
		}
		start := time.Now()
		err := read(jobs[i])
		if err == nil {
			db.fastSmallBatchReads.Store(time.Since(start) <= fastBatchReadLimit)
		}
		return err
	})
}

func (db *DB) readAdjacent(results []BatchResult, reads []batchRead) error {
	var group [maxAdjacentRecords]storage.AdjacentRead
	for i, read := range reads {
		group[i] = storage.AdjacentRead{Location: read.loc, Key: results[read.slot].Value}
	}
	if err := db.store.ReadAdjacent(group[:len(reads)]); err != nil {
		db.poison(err)
		return operationError("batch-get", "read", err)
	}
	for i, read := range reads {
		results[read.slot].Value = group[i].Value
	}
	return nil
}
