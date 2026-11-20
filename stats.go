package tarp

import "github.com/anaregdesign/tarp/internal/engine"

// CacheStats reports the effective LRU budget and usage in logical bytes,
// and cumulative hit, miss and capacity-eviction counts since Open.
// Hits and misses count each distinct key once per Get or BatchGet call.
type CacheStats struct {
	UsedBytes  uint64 // Logical bytes charged for cached keys, values and entry metadata.
	LimitBytes uint64 // Effective logical cache budget after applying defaults.
	Hits       uint64 // Distinct keys found in the LRU.
	Misses     uint64 // Distinct live keys absent from the LRU at lookup.
	Evictions  uint64 // Entries evicted to stay within LimitBytes.
}

// Stats contains current storage and cache gauges, and cumulative counters
// for this Open. BatchGet counts each distinct encoded key once in Cache.Hits,
// Cache.Misses or MissingKeys. BatchGetDuplicateInputs counts each additional
// occurrence of that key in the same call.
// See docs/stats.md for metric definitions and Prometheus integration.
type Stats struct {
	LiveKeys                uint64     // Keys present in the live index.
	LiveRecordBytes         uint64     // Encoded bytes of the latest Put for each live key, including headers.
	SelectedSegments        uint64     // Selected physical DATA files, including the active log.
	SelectedDataBytes       uint64     // Committed lengths of selected DATA files, including segment headers.
	ObsoletePutBytes        uint64     // Superseded or deleted Put record bytes still present in selected DATA.
	TombstoneBytes          uint64     // Encoded Delete record bytes still present in selected DATA.
	Cache                   CacheStats // Current LRU usage and its counters.
	MissingKeys             uint64     // Distinct keys absent from the live index per Get or BatchGet call.
	BatchGetDuplicateInputs uint64     // Input positions beyond the first for each key in a captured BatchGet.
	WriteCommits            uint64     // Nonempty foreground write calls published since Open.
	WrittenRecords          uint64     // Put and Delete records published by those calls.
}

// Stats returns an independent snapshot in constant time without file I/O.
// It is safe during concurrent operations and retains the final snapshot after Close.
func (db *DB) Stats() (s Stats) {
	var snapshot engine.Stats
	(*engine.DB)(db).ReadStats(&snapshot)
	s.LiveKeys = snapshot.LiveKeys
	s.LiveRecordBytes = snapshot.LiveRecordBytes
	s.SelectedSegments = snapshot.SelectedSegments
	s.SelectedDataBytes = snapshot.SelectedDataBytes
	s.ObsoletePutBytes = snapshot.ObsoletePutBytes
	s.TombstoneBytes = snapshot.TombstoneBytes
	s.Cache = CacheStats(snapshot.Cache)
	s.MissingKeys = snapshot.MissingKeys
	s.BatchGetDuplicateInputs = snapshot.BatchGetDuplicateInputs
	s.WriteCommits = snapshot.WriteCommits
	s.WrittenRecords = snapshot.WrittenRecords
	return s
}

// Stats returns statistics for the database owned by this typed handle.
func (db *TypedDB[K, V]) Stats() Stats {
	return db.core.Stats()
}
