package engine

import (
	"math"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

const maxContiguousRecords = 32

// contiguousRecordCount returns the number of consecutive captured reads that
// cover a bounded, contiguous byte range in one DATA segment.
// A duplicate result slot or a gap in the byte offsets ends the range.
func contiguousRecordCount(reads []batchRead) int {
	first := reads[0].loc
	if first.File == nil || first.RecordBytes <= 0 || first.RecordBytes >= storage.MaxContiguousReadBytes || first.RecordOffset < 0 {
		return 1
	}
	total := first.RecordBytes
	for i := 1; i < min(len(reads), maxContiguousRecords); i++ {
		next := reads[i].loc
		if next.File != first.File || first.RecordOffset > math.MaxInt64-total || next.RecordOffset != first.RecordOffset+total ||
			next.RecordBytes <= 0 || next.RecordBytes > storage.MaxContiguousReadBytes-total {
			return i
		}
		total += next.RecordBytes
	}
	return min(len(reads), maxContiguousRecords)
}

// Each contiguous DATA byte range is one I/O job. Range reads and individual
// record reads share worker and permit limits.
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
		return db.readContiguous(results, reads)
	}
	type job struct{ start, end uint16 }
	var local [maxContiguousRecords]job
	jobs := append(local[:0], job{0, uint16(firstWidth)})
	for i := firstWidth; i < len(reads); {
		if reads[i].loc.File == nil {
			i++
			continue
		}
		width := contiguousRecordCount(reads[i:])
		jobs = append(jobs, job{uint16(i), uint16(i + width)})
		i += width
	}
	read := func(j job) error {
		if j.end-j.start == 1 {
			r := reads[j.start]
			return db.readBatchRecord(results[r.slot].Value, &results[r.slot], r, false)
		}
		return db.readContiguous(results, reads[j.start:j.end])
	}
	// Warm contiguous DATA reads often finish faster than starting more workers.
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

func (db *DB) readContiguous(results []BatchResult, reads []batchRead) error {
	var group [maxContiguousRecords]storage.ContiguousRead
	for i, read := range reads {
		group[i] = storage.ContiguousRead{Location: read.loc, Key: results[read.slot].Value}
	}
	if err := db.store.ReadContiguous(group[:len(reads)]); err != nil {
		db.readFailure(err)
		return operationError("batch-get", "read", err)
	}
	for i, read := range reads {
		results[read.slot].Value = group[i].Value
	}
	return nil
}
