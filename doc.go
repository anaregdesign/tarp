// Package tarp implements a file-backed key-value store with selectable key indexes.
//
// Keys and values are bytes. Empty keys are invalid and empty values are valid.
// Retained inputs and returned values are copied. Keep input backing arrays
// stable until all calls using them have returned.
//
// The default key index is a paged copy-on-write B+tree with a 16 MiB
// working-memory budget. Set Options.IndexMode to IndexMemory for an in-memory
// hash index. Both can reuse a persistent index checkpoint and replay
// later commits. Full (default) verifies all committed DATA. Indexed skips the
// covered payloads on Open. Both modes share the Value LRU.
//
// Use OpenTyped with a Codec for each of the key and value types to work with
// Go values. NewCodec derives a codec for scalar types and their combinations
// in structs, slices, arrays, and maps. Bind lends typed access to an open DB.
// The owner closes the DB. Use compatible codecs across reopens and restores.
//
// One process owns a directory. Its DB supports concurrent goroutines and
// linearizable single-key operations. BatchPut and BatchDelete commit atomically.
// BatchGet reads the requested keys at one point in time and bounds their total
// encoded value bytes. Keys iterates the live keys without reading values.
// Successful writes sync the data and
// commit frontier before acknowledgment. Inspect OpError for the outcome of
// failed writes. See the durability contract for storage assumptions.
//
// OpenExisting requires an initialized directory. OpenContext also creates a
// new DB if needed. Both accept a context to cancel recovery during startup.
// Open starts one incremental compaction worker on each new or reopened DB.
// It reclaims obsolete records, repacks small files and yields for writers
// and backup. Compact explicitly builds a full checkpoint. Compaction uses
// PARITY to recover sealed DATA and can discard a proven irrecoverable live key.
// CompactResult and AutoCompactionStatus report the number of dropped keys. Provide temporary
// space for both maintenance and backup. Close joins the worker. Stats returns
// a constant-time snapshot of storage, cache and foreground activity.
//
// During v0 the public API and the database and backup formats may change
// between releases. Each revision reads and writes its current formats.
// Backup captures a consistent image. VerifyBackup verifies that image and
// returns its metadata. Restore publishes a new closed DB. See README.md
// and docs/ for limits, recovery and qualification evidence.
package tarp
