// Package tarp implements a file-backed, hash-indexed key-value store.
//
// Keys and values are bytes. Empty keys are invalid and empty values are valid.
// Retained inputs and returned values are copied. Keep input backing arrays
// stable until all calls using them have returned.
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
// and backup. Compact explicitly builds a full checkpoint. MaintenanceStatus reports worker
// activity, deferred retirement and logical bytes unlinked. Provide temporary
// space for both maintenance and backup. Close joins the worker. Stats returns
// a constant-time snapshot of storage, cache and foreground activity.
//
// Open and Restore accept one unversioned format. A database or backup from
// the previous implementation is unsupported. Use Backup for a consistent
// image. VerifyBackup returns metadata for a fully verified immutable backup. Restore publishes
// a new closed DB. See README.md and docs/ for limits, recovery and qualification evidence.
package tarp
