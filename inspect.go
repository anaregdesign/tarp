package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
)

// InspectKey reads the committed value of key from a stopped database without
// changing or creating any files. It validates committed DATA, including the
// histories of other keys, using bounded replay that may require multiple
// passes. The returned value belongs to the caller. A validated absence
// returns [ErrNotFound].
//
// InspectKey requires an existing LOCK file and returns [ErrLocked] if another
// handle owns the directory. A missing directory or LOCK file matches
// fs.ErrNotExist. A freshly restored database acquires its LOCK file on first
// [OpenExisting]. A backup directory returns [ErrBackupDirectory].
func InspectKey(ctx context.Context, dir string, key []byte) ([]byte, error) {
	return engine.InspectKey(ctx, dir, key)
}
