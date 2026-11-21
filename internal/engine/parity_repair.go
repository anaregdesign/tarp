package engine

import (
	"context"

	"github.com/anaregdesign/tarp/internal/storage"
)

func RepairParity(ctx context.Context, dir string) error {
	if ctx == nil {
		return validContextError("repair-parity")
	}
	if err := storage.RepairParity(ctx, dir, nil); err != nil {
		return operationError("repair-parity", "validation/repair", err)
	}
	return nil
}
