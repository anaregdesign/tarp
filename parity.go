package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
)

// ErrInvalidParity identifies a damaged or mismatched PARITY sidecar.
var ErrInvalidParity = format.ErrInvalidParity

// RepairDataFromParity exclusively locks an existing database and repairs sealed DATA
// using its PARITY sidecars. A successful call verifies the selected
// image, including the committed active log. Healthy files retain their inodes.
// Each replacement must match the sealed DATA digest in MANIFEST and is
// independently durable. Earlier repairs remain durable if a later file fails.
// An [OpError] with outcome [Committed] reports partial durable progress.
// After [ErrCommitUncertain], retry RepairDataFromParity to verify the selected
// bytes under the database lock.
func RepairDataFromParity(ctx context.Context, dir string) error {
	return engine.RepairDataFromParity(ctx, dir)
}
