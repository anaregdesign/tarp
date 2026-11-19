package tarp

import "github.com/anaregdesign/tarp/internal/engine"

// CacheStats reports the effective LRU budget and usage in logical bytes,
// and cumulative hit, miss and capacity-eviction counts since Open.
// Hits and misses count each distinct key once per Get or BatchGet call.
type CacheStats = engine.CacheStats

// Stats contains current storage and cache gauges, and cumulative counters
// for this Open. BatchGet counts each distinct encoded key once in Cache.Hits,
// Cache.Misses or MissingKeys. BatchGetDuplicateInputs counts each additional
// occurrence of that key in the same call.
// See docs/stats.md for metric definitions and Prometheus integration.
type Stats = engine.Stats

// Stats returns an independent snapshot in constant time without file I/O.
// It is safe during concurrent operations and retains the final snapshot after Close.
func (db *DB) Stats() Stats {
	return (*engine.DB)(db).Stats()
}

// Stats returns statistics for the database owned by this typed handle.
func (db *TypedDB[K, V]) Stats() Stats {
	return db.core.Stats()
}
