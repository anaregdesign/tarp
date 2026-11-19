package tarp

import "github.com/anaregdesign/tarp/internal/engine"

// MaxBatchEntries limits the number of input items per batch, including duplicates.
const MaxBatchEntries = engine.MaxBatchEntries

// BatchResult holds a value from BatchGet. Found distinguishes a stored empty
// value from a missing key. The caller owns the returned bytes.
type BatchResult = engine.BatchResult

// BatchGet reads all keys at one point in time and returns one result per input,
// in order. Repeated keys receive independent value slices. Like Scan, it
// preserves cache recency. An error discards the entire result.
func (db *DB) BatchGet(keys [][]byte) ([]BatchResult, error) {
	return (*engine.DB)(db).BatchGet(keys)
}

// BatchPut durably upserts entries in one atomic commit. Entries are applied
// in input order, so the last value for a repeated key wins. Encoded records
// must collectively fit within one effective segment payload.
func (db *DB) BatchPut(entries []Entry) error {
	return (*engine.DB)(db).BatchPut(entries)
}

// BatchDelete durably removes the requested keys in one atomic commit.
// Repeated keys are removed once. A missing key returns ErrNotFound before
// publication. The distinct deletion records must fit one segment payload.
func (db *DB) BatchDelete(keys [][]byte) error {
	return (*engine.DB)(db).BatchDelete(keys)
}
