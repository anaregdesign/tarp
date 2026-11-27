package tarp

import (
	"context"
	"iter"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

// DB stores byte keys and values. Share one *DB across goroutines and call
// [DB.Close] to release its directory. Use [Bind] to lend typed access to an
// existing DB. Do not copy a DB.
type DB engine.DB

// IndexMode selects the runtime key index.
type IndexMode = engine.IndexMode

const (
	// IndexBTree is the zero value. It keeps DATA record addresses in B+tree
	// pages. A clean Close persists or reuses an index checkpoint.
	IndexBTree = engine.IndexBTree
	// IndexMemory keeps all key locations in memory.
	IndexMemory = engine.IndexMemory
)

// OpenValidation controls verification of DATA covered by a persisted index.
type OpenValidation = engine.OpenValidation

const (
	// Full verifies all committed DATA during Open. This is the default.
	Full = engine.Full
	// Indexed validates an available index checkpoint and replays the DATA log
	// suffix it does not cover. It checks covered DATA records on read.
	Indexed = engine.Indexed
)

// Options configures one database. Zero fields select their defaults.
type Options struct {
	// IndexMode selects a paged B+tree (the default) or an in-memory hash
	// index for this handle.
	IndexMode IndexMode
	// OpenValidation selects Full (the default) or Indexed recovery.
	OpenValidation OpenValidation
	// BTreeMemoryBudgetBytes sets the working-memory budget for IndexBTree,
	// shared by the foreground index and its maintenance workspaces.
	// Zero selects 16 MiB for the B+tree. A nonzero budget must be at least
	// 1 MiB and is valid only with IndexBTree.
	BTreeMemoryBudgetBytes int64
	// ValueCacheBudgetBytes sets the logical budget for the value LRU in
	// either index mode. Each live key, value and its entry metadata must fit
	// together within the budget. Zero selects 64 MiB.
	ValueCacheBudgetBytes int64
	// MaxRecordBytes limits the encoded header, key and value of one record. Zero selects 1 MiB.
	MaxRecordBytes int64
	// TargetSegmentBytes sets the target for each new log or snapshot segment.
	// It caps small-segment repacking input and grouped log cleaning output.
	// The default is 4 MiB. The target grows to fit one maximum record and a segment header.
	TargetSegmentBytes int64
	// ReadFileHandles bounds the pool of reusable DATA handles. Zero selects 64.
	ReadFileHandles int
	// BatchGetReadConcurrency limits simultaneous DATA reads across BatchGet calls on this DB. Zero selects 8.
	BatchGetReadConcurrency int
	// MaxBatchGetValueBytes limits the sum of encoded value bytes in one BatchGet
	// result, including duplicates. Missing keys and empty values cost zero.
	// It excludes result metadata and allocations by application codecs. Zero selects 64 MiB.
	MaxBatchGetValueBytes int64
}

// RestoreLimits returns the limits used to verify live records for restore.
func (o Options) RestoreLimits() RestoreLimits {
	return RestoreLimits(engine.Options(o).RestoreLimits())
}

// Open creates or reopens a database and starts automatic incremental compaction.
// Use [OpenExisting] when a missing or empty directory should be an error and
// [OpenContext] when startup must honor a deadline. The parent directory must
// exist. Incompatible options leave existing data intact.
// Opening a backup directory returns an error matching [ErrBackupDirectory].
func Open(dir string, opts Options) (*DB, error) {
	return OpenContext(context.Background(), dir, opts)
}

// OpenContext creates or reopens a database like Open. The context controls
// recovery and has no effect on a successfully returned DB. Once initialization
// of a new database begins, it finishes before OpenContext returns.
func OpenContext(ctx context.Context, dir string, opts Options) (*DB, error) {
	db, err := engine.OpenContext(ctx, dir, engine.Options(opts), false)
	return (*DB)(db), err
}

// OpenExisting reopens an initialized database with cancellable recovery.
// An absent or empty directory returns an error matching fs.ErrNotExist.
// It accepts a database just published by [Restore]. Opening the source backup
// returns an error matching [ErrBackupDirectory].
func OpenExisting(ctx context.Context, dir string, opts Options) (*DB, error) {
	db, err := engine.OpenContext(ctx, dir, engine.Options(opts), true)
	return (*DB)(db), err
}

// Get returns an independently owned value. A missing key returns [ErrNotFound].
// If its sealed DATA cannot supply the value, Get may return [ErrDataUnavailable].
func (db *DB) Get(key []byte) ([]byte, error) {
	return (*engine.DB)(db).Get(key)
}

// GetContext reads an independently owned value like [DB.Get]. A context
// canceled before lookup returns its error. Cancellation can also interrupt
// DATA repair or a wait for another caller's repair. An in-progress filesystem
// call may finish before GetContext returns.
func (db *DB) GetContext(ctx context.Context, key []byte) ([]byte, error) {
	return (*engine.DB)(db).GetContext(ctx, key)
}

// Entry pairs a key and value for BatchPut and Scan. Both slices belong to the caller.
type Entry = engine.Entry

// Scan yields every live entry once in unspecified order. Use it to export
// the current key-value state. [DB.Keys] enumerates just keys without reading
// DATA, and [DB.ScanRange] yields entries in encoded-key order. Each iteration captures
// a fresh snapshot when it starts. Writes and compaction can continue during the
// scan. An error is yielded once with a zero Entry and ends iteration.
// Breaking out of the loop releases the snapshot. With iter.Pull2, defer stop().
// The loop body may call any DB method, including Close.
func (db *DB) Scan(ctx context.Context) iter.Seq2[Entry, error] {
	return (*engine.DB)(db).Scan(ctx)
}

// ScanPrefix yields entries whose encoded keys start with encodedPrefix.
// A nil or empty prefix matches every key. ScanPrefix copies the prefix
// before returning. The iterator has the same snapshot, order, and ownership
// guarantees as [DB.Scan].
func (db *DB) ScanPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[Entry, error] {
	return (*engine.DB)(db).ScanPrefix(ctx, encodedPrefix)
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

// Backup publishes a consistent file backup while writes continue. Use
// [VerifyBackup] to check the backup and [Restore] to create a DB from it.
// The destination must be new and its parent must already exist.
// A destination collision matches fs.ErrExist and has outcome [NotApplied].
func (db *DB) Backup(ctx context.Context, destination string) error {
	return (*engine.DB)(db).Backup(ctx, destination)
}

// ProtectParity seals a nonempty active DATA segment, then verifies or builds
// PARITY sidecars for the selected sealed DATA segments. Use it while the DB
// is open to prepare coverage for later [RepairDataFromParity]. Writers can
// continue appending to the new active segment.
func (db *DB) ProtectParity(ctx context.Context) error {
	return (*engine.DB)(db).ProtectParity(ctx)
}

// RestoreLimits bounds the logical value-cache charge and encoded size of each live
// record in a restored image. Zero fields select the same defaults as Open.
type RestoreLimits struct {
	// ValueCacheBudgetBytes must cover the largest live entry's value-cache
	// charge (key, value and entry metadata). Zero selects 64 MiB.
	ValueCacheBudgetBytes int64
	// MaxRecordBytes bounds the largest live encoded record, including its header. Zero selects 1 MiB.
	MaxRecordBytes int64
}

// RestoreRequest names the immutable Source backup, a new Destination directory
// and the Limits to verify before publishing the restored database.
type RestoreRequest struct {
	// Source is the immutable backup directory.
	Source string
	// Destination is the new database directory. Its parent must already exist.
	Destination string
	// Limits specifies the cache and record sizes to validate before publication.
	Limits RestoreLimits
}

// Restore verifies a completed backup and publishes a new, closed database.
// The source must remain immutable and the destination must be new. Pass the
// destination to [OpenExisting] after Restore succeeds.
// After validating Limits, Restore checks for cancellation before resolving either path.
// A destination collision matches fs.ErrExist and has outcome [NotApplied].
func Restore(ctx context.Context, req RestoreRequest) error {
	return engine.Restore(ctx, engine.RestoreRequest{
		Source: req.Source, Destination: req.Destination, Limits: engine.RestoreLimits(req.Limits),
	})
}

// BackupInfo describes a verified file backup at its last committed user sequence.
// An empty backup has zero live keys and zero size measurements.
type BackupInfo struct {
	DatabaseID       [16]byte // Database UUID, shared by its backups and restored copies.
	LastUserSequence uint64   // Last committed user sequence in the backup.
	LiveKeys         uint64   // Number of keys present at LastUserSequence.
	// LargestLiveCacheChargeBytes is the largest logical value-cache charge for one live entry.
	LargestLiveCacheChargeBytes int64
	// LargestLiveRecordBytes is the largest encoded live record, including its header.
	LargestLiveRecordBytes int64
}

// CheckLimits checks whether a target configuration can restore this backup,
// using the measured live record sizes without reading the backup again. Zero fields
// select the same defaults as Open. Invalid limits match ErrInvalidOptions.
// Insufficient limits match ErrIncompatibleOptions and expose *CompatibilityError
// through errors.As.
func (info BackupInfo) CheckLimits(limits RestoreLimits) error {
	return engine.RestoreLimits(limits).Check(storage.LiveRecordSizes{
		LargestLiveCacheChargeBytes: info.LargestLiveCacheChargeBytes,
		LargestLiveRecordBytes:      info.LargestLiveRecordBytes,
	})
}

// VerifyBackup checks an immutable backup's file set, checksums and committed
// records, and reports the limits its live records require. It does not write
// files. Use [BackupInfo.CheckLimits] with [Options.RestoreLimits] to check a
// planned restore configuration. Any error returns a zero BackupInfo.
func VerifyBackup(ctx context.Context, source string) (BackupInfo, error) {
	return publicBackupInfo(engine.VerifyBackup(ctx, source))
}

// VerifyBackupWithScratch verifies an immutable backup using temporary sort
// files in an existing directory outside the backup. Use it for a large backup
// when scratch space is available to avoid repeated DATA passes.
func VerifyBackupWithScratch(ctx context.Context, source, scratch string) (BackupInfo, error) {
	return publicBackupInfo(engine.VerifyBackupWithScratch(ctx, source, scratch))
}

func publicBackupInfo(info storage.BackupInfo, err error) (BackupInfo, error) {
	if err != nil {
		return BackupInfo{}, err
	}
	return BackupInfo{
		DatabaseID:                  info.DatabaseID,
		LastUserSequence:            info.LastUserSequence,
		LiveKeys:                    info.LiveKeys,
		LargestLiveCacheChargeBytes: info.LargestLiveCacheChargeBytes,
		LargestLiveRecordBytes:      info.LargestLiveRecordBytes,
	}, nil
}

// CheckpointIndex persists an index checkpoint covering the captured catalog prefix
// while normal writes continue. Use it on a long-running DB to reduce the index
// rebuilding needed on a later open. A healthy B+tree DB also checkpoints on
// [DB.Close]. To reclaim obsolete DATA records, use [DB.Compact].
func (db *DB) CheckpointIndex(ctx context.Context) error {
	return (*engine.DB)(db).CheckpointIndex(ctx)
}

// CompactResult reports unlinked bytes and retired DATA bytes
// still retained by segment pins or live index references at cleanup.
type CompactResult = engine.CompactResult

// CompactOption configures one call to Compact. Its zero value keeps the
// default behavior.
type CompactOption struct {
	keyOrder bool
}

// WithKeyOrder writes the retained snapshot records in encoded-key order.
// Concurrent writes after the checkpoint cutoff form the next log suffix.
// On the same handle, later ordered calls reuse unchanged snapshot segments.
// A healthy ordered checkpoint with no later writes or pending cleanup is a no-op.
func WithKeyOrder() CompactOption {
	return CompactOption{keyOrder: true}
}

// Compact builds a full DATA checkpoint through the captured user sequence.
// Use it to reclaim obsolete DATA history in addition to automatic incremental cleaning.
// By default it copies live records in source order. Pass [WithKeyOrder] to
// cluster the snapshot by encoded key.
// It uses PARITY to recover damaged sealed DATA. If a live version remains
// unreadable, the checkpoint returns [ErrDataUnavailable] and keeps its source
// selected. Recovery and protected snapshot files require temporary disk space.
func (db *DB) Compact(ctx context.Context, opts ...CompactOption) (CompactResult, error) {
	for _, opt := range opts {
		if opt.keyOrder {
			return (*engine.DB)(db).CompactByKey(ctx)
		}
	}
	return (*engine.DB)(db).Compact(ctx)
}

// AutoCompactionStatus reports the background compaction worker's activity since Open.
type AutoCompactionStatus = engine.AutoCompactionStatus

// AutoCompactionStatus returns the worker's latest status, including after Close.
func (db *DB) AutoCompactionStatus() AutoCompactionStatus {
	return (*engine.DB)(db).AutoCompactionStatus()
}

// Close rejects new calls and waits for admitted operations before releasing
// directory ownership. A healthy B+tree handle persists or reuses an index
// checkpoint. Concurrent or repeated calls return the same result.
func (db *DB) Close() error {
	return (*engine.DB)(db).Close()
}
