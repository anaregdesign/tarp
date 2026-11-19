package tarp

import (
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
)

// DB owns its directory until Close finishes. Do not copy a DB.
type DB engine.DB

// Options configures one database. A zero field selects its default.
// CacheBytes bounds the logical LRU charge (64 MiB), MaxRecordBytes bounds
// one encoded record (1 MiB), TargetSegmentBytes sizes new log and checkpoint
// files and bounds repack inputs (4 MiB), and ReadFileHandles sizes the cache-miss
// read pool (64).
type Options = engine.Options

// Open creates or reopens a database and starts automatic incremental compaction.
// The parent directory must exist. Incompatible options leave existing data intact.
func Open(dir string, opts Options) (*DB, error) {
	db, err := engine.Open(dir, opts)
	return (*DB)(db), err
}

// Get returns an independently owned value, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	return (*engine.DB)(db).Get(key)
}

// Entry pairs a key and value for BatchPut and Scan. Both slices belong to the caller.
type Entry = engine.Entry

// Scan yields every live entry once in unspecified order. Each iteration captures
// a fresh snapshot when it starts. Writes and compaction can continue during the
// scan. An error is yielded once with a zero Entry and ends iteration.
// Breaking out of the loop releases the snapshot. With iter.Pull2, defer stop().
// The loop body may call any DB method, including Close.
func (db *DB) Scan(ctx context.Context) iter.Seq2[Entry, error] {
	return (*engine.DB)(db).Scan(ctx)
}

// Put durably inserts or replaces a value. An oversized entry returns ErrTooLarge
// before changing stored data or cache recency.
func (db *DB) Put(key, value []byte) error {
	return (*engine.DB)(db).Put(key, value)
}

// Delete durably removes a key. A missing key returns ErrNotFound.
func (db *DB) Delete(key []byte) error {
	return (*engine.DB)(db).Delete(key)
}

// Backup publishes a consistent point-in-time copy while writes continue.
// The destination must be new and its parent must already exist.
func (db *DB) Backup(ctx context.Context, destination string) error {
	return (*engine.DB)(db).Backup(ctx, destination)
}

// Restore verifies a completed backup and publishes a new, closed database.
// The source must remain immutable and the destination must be new.
func Restore(ctx context.Context, backup, destination string, opts Options) error {
	return engine.Restore(ctx, backup, destination, opts)
}

// CompactResult counts reclaimed file bytes and retired bytes still held by leases.
type CompactResult = engine.CompactResult

// Compact replaces the complete committed prefix with an online checkpoint.
// It requires temporary disk space.
func (db *DB) Compact(ctx context.Context) (CompactResult, error) {
	return (*engine.DB)(db).Compact(ctx)
}

// MaintenanceStatus reports automatic maintenance activity since Open.
type MaintenanceStatus = engine.MaintenanceStatus

// MaintenanceStatus returns the worker's latest status, including after Close.
func (db *DB) MaintenanceStatus() MaintenanceStatus {
	return (*engine.DB)(db).MaintenanceStatus()
}

// Close rejects new calls and waits for admitted operations before releasing
// directory ownership. Concurrent or repeated calls return the same result.
func (db *DB) Close() error {
	return (*engine.DB)(db).Close()
}
