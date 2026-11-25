package tarp

import "github.com/anaregdesign/tarp/internal/engine"

// MaxBatchGetEntries limits the number of input keys per BatchGet, including duplicates.
const MaxBatchGetEntries = engine.MaxBatchGetEntries

// MaxWriteBatchEntries limits the number of inputs per BatchPut, BatchDelete,
// or Apply, including duplicate keys. Each write batch must also fit one DATA segment.
const MaxWriteBatchEntries = engine.MaxWriteBatchEntries

// BatchResult holds a value from BatchGet. Found distinguishes a stored empty
// value from a missing key. The caller owns the returned bytes.
type BatchResult = engine.BatchResult

// BatchGet reads all keys at one point in time and returns one result per input,
// in order. Repeated keys receive independent value slices. Like Scan, it
// preserves cache recency. Distinct uncached records can be read concurrently,
// up to Options.BatchGetReadConcurrency across the DB. MaxBatchGetValueBytes
// limits the combined value bytes, counting duplicates. Exceeding it returns
// a BatchLimitError before reading or copying values. An error discards the entire result.
func (db *DB) BatchGet(keys [][]byte) ([]BatchResult, error) {
	return (*engine.DB)(db).BatchGet(keys)
}

// BatchPut durably upserts entries in one atomic commit. Entries are applied
// in input order, so the last value for a repeated key wins. Encoded records
// must collectively fit in one DATA segment after its header.
func (db *DB) BatchPut(entries []Entry) error {
	return (*engine.DB)(db).BatchPut(entries)
}

// BatchDelete durably removes the requested keys in one atomic commit.
// Repeated keys are removed once. A missing key returns a BatchKeyError matching
// ErrNotFound before publication. The distinct Delete records must fit
// in one DATA segment after its header. For idempotent deletes, queue them in
// a [WriteBatch] and call [DB.Apply].
func (db *DB) BatchDelete(keys [][]byte) error {
	return (*engine.DB)(db).BatchDelete(keys)
}
