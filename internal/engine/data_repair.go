package engine

import (
	"context"

	"github.com/anaregdesign/tarp/internal/storage"
)

func RepairDataFromParity(ctx context.Context, dir string) error {
	if ctx == nil {
		return validContextError("repair-data-from-parity")
	}
	if err := storage.RepairDataFromParity(ctx, dir, nil); err != nil {
		return operationError("repair-data-from-parity", "validation/repair", err)
	}
	return nil
}
