package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
)

// ErrInvalidParity identifies a damaged or mismatched recovery file.
var ErrInvalidParity = format.ErrInvalidParity

// RepairParity exclusively locks an existing database and restores recoverable
// sealed DATA from its parity files. A successful call verifies the selected
// image, including the committed active log. Healthy files retain their inodes.
// Each replacement must match its original MANIFEST digest and is independently
// durable. If another file cannot be repaired, earlier successful repairs remain.
// An [OpError] with outcome [Committed] reports partial durable progress.
// [ErrCommitUncertain] means a replacement may have reached disk; retry
// RepairParity to verify the selected bytes under the database lock.
func RepairParity(ctx context.Context, dir string) error {
	return engine.RepairParity(ctx, dir)
}
