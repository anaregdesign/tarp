package engine

import (
	"context"
	"fmt"

	"github.com/anaregdesign/tarp/internal/storage"
)

func SalvageData(ctx context.Context, dir string, id uint64, limit int64, visit func(storage.SalvagedRecord) error) (storage.SalvageReport, error) {
	var report storage.SalvageReport
	if ctx == nil {
		return report, validContextError("salvage")
	}
	if id == 0 || visit == nil {
		return report, operationError("salvage", "options", fmt.Errorf("%w: require a sealed DATA ID and visitor", ErrInvalidOptions))
	}
	limits, err := (RestoreLimits{MaxRecordBytes: limit}).normalized()
	if err != nil {
		return report, operationError("salvage", "options", err)
	}
	report, err = storage.SalvageData(ctx, dir, id, limits.MaxRecordBytes, visit)
	if err != nil {
		return report, operationError("salvage", "sealed DATA", err)
	}
	return report, nil
}
