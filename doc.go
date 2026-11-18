// Package tarp implements a file-backed, hash-indexed key-value store.
//
// Keys and values are bytes. Empty keys are invalid; empty values are valid.
// Retained inputs and returned values are copied. Callers must not mutate
// input backing arrays while a call using them is in progress.
//
// A DB supports concurrent goroutines and linearizable individual key
// operations, not multi-key transactions. Nil write errors acknowledge durable
// commits on the documented, qualified Linux storage configuration. An error
// does not always imply rollback; inspect OpError. Close is not a commit barrier.
//
// One process exclusively owns a directory. Use Backup, not a live directory
// copy, for a consistent image. Applications schedule Compact and provision
// maintenance headroom. The value-cache limit is logical, not a process-memory
// or full-index limit. See README.md and docs/ for the durability/support policy.
package tarp
