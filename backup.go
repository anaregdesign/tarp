package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
)

// BackupWithSnapshot publishes a backup as in [DB.Backup] and invokes inspect
// with a read view of the exact committed state in the image. After validating
// the staged copy, it calls inspect once before publishing the destination.
// A callback error cancels publication and is returned to the caller.
//
// Writes can proceed during the copy and callback. The snapshot is borrowed
// for the duration of inspect and is closed when the operation returns.
// Supply a non-nil callback and keep the source DB open until it returns. Use ctx for
// reads and scans in inspect when they should honor cancellation.
func (db *DB) BackupWithSnapshot(ctx context.Context, destination string, inspect func(*Snapshot) error) error {
	if inspect == nil {
		return (*engine.DB)(db).BackupWithSnapshot(ctx, destination, nil)
	}
	return (*engine.DB)(db).BackupWithSnapshot(ctx, destination, func(snapshot *engine.Snapshot) error {
		return inspect((*Snapshot)(snapshot))
	})
}

// BackupWithSnapshot publishes a backup and supplies a typed read view of
// the exact cut. See [DB.BackupWithSnapshot] for callback and lifetime rules.
func (db *TypedDB[K, V]) BackupWithSnapshot(ctx context.Context, destination string, inspect func(*TypedSnapshot[K, V]) error) error {
	if inspect == nil {
		return db.core.BackupWithSnapshot(ctx, destination, nil)
	}
	return db.core.BackupWithSnapshot(ctx, destination, func(snapshot *Snapshot) error {
		return inspect(&TypedSnapshot[K, V]{snap: snapshot, codecs: db.View()})
	})
}
