package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
)

// IncrementalBackupRequest identifies a new backup generation within an
// existing, caller-controlled repository directory on a filesystem supporting
// hardlinks. Place the repository on independent storage for device recovery.
type IncrementalBackupRequest struct {
	// Repository is the existing directory that owns the generations and shared
	// objects. Place it outside the source database directory and its ancestors.
	Repository string
	// Name is a new single path component, excluding LOCK and dot-prefixed names.
	Name string
	// VerifyReusedData rehashes old sealed DATA on the repository filesystem.
	// The default checks the digest-derived object name and expected file size.
	VerifyReusedData bool
}

// IncrementalBackupResult reports the commit sequence and DATA file bytes
// copied from the live database or reused from verified repository objects.
// Metadata bytes are excluded. An error returns a zero result.
type IncrementalBackupResult = engine.IncrementalBackupResult

// BackupIncremental publishes a native backup directory at Repository/Name.
// [VerifyBackupWithScratch] and [Restore] accept that directory directly.
// It reads the live source for missing sealed DATA and the exact committed
// prefix of the active segment. New objects are verified before reuse.
// VerifyBackupWithScratch and Restore rehash every object. Use [DB.Backup]
// to independently replay the live source during capture. The repository
// and its generations must remain immutable except for these backup
// operations and deletion of generations that are no longer used. A failure
// after publication can report [Unknown].
func (db *DB) BackupIncremental(ctx context.Context, req IncrementalBackupRequest) (IncrementalBackupResult, error) {
	return (*engine.DB)(db).BackupIncremental(ctx, engine.IncrementalBackupRequest(req))
}

// BackupIncremental publishes a generation of the encoded database.
// See [DB.BackupIncremental] for the repository and consistency contract.
func (db *TypedDB[K, V]) BackupIncremental(ctx context.Context, req IncrementalBackupRequest) (IncrementalBackupResult, error) {
	return db.core.BackupIncremental(ctx, req)
}

// PruneIncrementalBackupObjects removes cached DATA files whose published
// generations have all been deleted. It leaves all remaining backup
// generations intact. An interrupted pruning operation can be retried.
func PruneIncrementalBackupObjects(ctx context.Context, repository string) error {
	return engine.PruneIncrementalBackupObjects(ctx, repository)
}
