// Package tarp implements a file-backed key-value store with selectable key indexes.
//
// Keys and values are bytes. Empty keys are invalid and empty values are valid.
// Retained inputs and returned values are copied. Keep input backing arrays
// stable until all calls using them have returned.
//
// The default key index is a paged copy-on-write B+tree with a 16 MiB
// working-memory budget. Set Options.IndexMode to IndexMemory for an in-memory
// hash index. Both can reuse a persistent index checkpoint and replay
// the DATA log suffix after the checkpoint. Full (default) verifies all
// committed DATA during Open. Indexed validates the covered segment headers
// and lengths during Open and verifies individual DATA records on read.
// Both modes use the same value cache.
//
// Use OpenTyped with a Codec for each of the key and value types to work with
// Go values. NewCodec derives a codec for scalar types and their combinations
// in structs, slices, arrays, and maps. NewOrderedCodec and the Ordered*Codec
// types produce byte-comparable key encodings that preserve Go's natural order
// under bytes.Compare, with EncodeOrderedStringPrefix, EncodeOrderedBytesPrefix,
// and EncodeOrderedTuplePrefix for prefix bounds. Bind lends typed access to an
// open DB. The owner closes the DB. Use compatible codecs across reopens and
// restores.
//
// One process owns a directory. Its DB supports concurrent goroutines and
// linearizable single-key operations. BatchPut, BatchDelete, and Apply (taking
// a mixed WriteBatch or TypedWriteBatch of Put and idempotent Delete
// operations) commit atomically. BatchGet reads the requested keys at one point
// in time and bounds their total encoded value bytes. [DB.Snapshot] and
// [View.Snapshot] capture a multi-operation consistent read point with
// Sequence, Get, BatchGet, Scan*, Keys*, and Close without blocking concurrent
// writers or compaction. [DB.Keys], [DB.KeysPrefix], and [DB.KeysRange]
// enumerate the index without reading DATA. [DB.Scan], [DB.ScanPrefix], and
// [DB.ScanRange] also read the matching values. [DB.ScanRange] and
// [DB.KeysRange] iterate half-open [lower, upper) key bounds in ascending
// bytewise lexicographic order (or descending with [WithReverse]) across both
// index modes; [PrefixRange] converts a byte prefix into [lower, upper) bounds.
// [DB.Scan], [DB.ScanPrefix], [DB.Keys], and [DB.KeysPrefix] capture a fresh
// snapshot in unspecified order. Successful writes sync the data and commit
// frontier before acknowledgment. Inspect OpError for the outcome of failed
// writes. See the durability contract for storage assumptions.
//
// OpenExisting requires an initialized directory. OpenContext also creates a
// new DB if needed. Both accept a context to cancel recovery during startup.
// Open starts one incremental compaction worker on each new or reopened DB.
// It reclaims obsolete records, repacks small files and yields for writers
// and backup. [DB.Compact] explicitly builds a full DATA checkpoint; pass
// [WithKeyOrder] to order its snapshot records by encoded key. Compaction uses
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
