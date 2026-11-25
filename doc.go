// Package tarp implements a file-backed key-value store. One process owns the
// database directory, and goroutines can share the open [DB]. Single-key
// operations are linearizable. A call that publishes new records syncs the
// DATA and commit frontier before returning success.
//
// # Opening a database and choosing codecs
//
// Use [Open] with Options{} to create or reopen a database with the defaults.
// Use [OpenContext] to bound startup recovery or [OpenExisting] when a missing
// database should fail startup. The owner calls [DB.Close] to release the
// directory. The default index is a paged B+tree with a 16 MiB working-memory
// budget. [IndexMemory] keeps all key locations in an in-memory hash index.
// Both modes use the value cache.
//
// The byte API accepts nonempty keys and arbitrary values, including empty
// values. Callers own the returned slices and keep input slices stable until
// each call returns. For Go values, [OpenTyped] returns an owning handle.
// [Bind] lends a typed view of a DB that is already open. Use [NewCodec] to
// store supported Go values such as structs, slices and maps. Use [NewOrderedCodec] for keys
// whose field order should determine range scan order, such as jobs ordered
// by queue and priority. Use the same encodings across reopens and restores.
//
// # Reading consistent data
//
// For one key, use [DB.Get] and check for [ErrNotFound] with [errors.Is].
// For a known list of keys, [DB.BatchGet] returns one result per input at one
// point in time. Check each result's Found field. For a report that combines
// several lookups or scans while writes continue, capture one [DB.Snapshot],
// use it for every read, and close it when finished to release its segment pins.
//
// To enumerate keys, use [DB.Keys] or [DB.KeysPrefix]. These read the index
// alone. To export values as well, use [DB.Scan] or [DB.ScanPrefix]. For
// sorted results, [DB.KeysRange] and [DB.ScanRange] traverse half-open bounds
// in encoded-key order. Use [PrefixRange] to restrict an ordered scan to a
// prefix and [WithReverse] for descending order. Each iteration on a DB
// captures a fresh view. Check the error yielded by the iterator.
//
// # Writing related changes
//
// [DB.Put] upserts one key and [DB.Delete] removes one existing key. To commit
// several upserts together, use [DB.BatchPut]. [DB.BatchDelete] removes a set
// of keys atomically and fails if any key is missing. To publish a result and
// remove its pending job in one commit, queue both operations in a [WriteBatch]
// and call [DB.Apply]. A delete in this batch succeeds even if the key is absent.
//
// If a write fails, use [errors.As] to inspect the [OpError] when present.
// An [Unknown] database commit requires closing the handle, reopening the
// database and checking its state before deciding whether to retry.
//
// # Backup and maintenance
//
// Use [DB.Backup] to capture a consistent copy while an application keeps
// writing. [VerifyBackup] checks the backup and reports its size requirements.
// [Restore] publishes a new, closed database at a fresh destination. During
// v0 each revision reads and writes its current database and backup formats.
//
// Open starts an incremental compaction worker. [DB.Stats] reports storage
// and cache activity, and [DB.AutoCompactionStatus] reports the worker's
// progress. For a full DATA checkpoint, call [DB.Compact]. [WithKeyOrder]
// clusters retained records by encoded key. Compaction uses PARITY to recover
// sealed DATA. Set Options.PreserveUnavailableKeys on each open to refuse a
// checkpoint that would discard an irrecoverable live key. By default,
// compaction reports the number of live keys it drops.
// Allow temporary space for compaction and backups.
//
// To reduce index rebuilding on restart, [DB.CheckpointIndex] persists an
// index checkpoint. The default [Full] validation still checks all committed
// DATA during Open. [Indexed] checks the headers and lengths of covered DATA
// segments, replays the log suffix, and verifies covered records when read.
// See the repository's durability contract for storage assumptions.
package tarp
