// Package tarp implements a file-backed, hash-indexed key-value store.
//
// Keys and values are bytes. Empty keys are invalid and empty values are valid.
// Retained inputs and returned values are copied. Keep input backing arrays
// stable until all calls using them have returned.
//
// One process owns a directory. Its DB supports concurrent goroutines and
// linearizable single-key operations. Successful writes sync the data and
// commit frontier before acknowledgment. Inspect OpError for the outcome of
// failed writes. See the durability contract for storage assumptions.
//
// Open starts one incremental compaction worker on each new or reopened DB.
// It reclaims eligible sealed files and yields for writers and backup. Compact
// explicitly builds a full checkpoint. MaintenanceStatus reports worker
// activity, deferred retirement and logical bytes unlinked. Provide temporary
// space for both maintenance and backup. Close joins the worker.
//
// Open and Restore accept one unversioned format. A database or backup from
// the previous implementation is unsupported. Use Backup for a consistent
// image. See README.md and docs/ for limits, recovery and qualification evidence.
package tarp
