package engine

import (
	"context"

	"github.com/anaregdesign/tarp/internal/storage"
)

func InspectKey(ctx context.Context, dir string, key []byte) ([]byte, error) {
	if ctx == nil {
		return nil, validContextError("inspect key")
	}
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	value, found, err := storage.InspectKey(ctx, dir, string(key))
	if err != nil {
		return nil, operationError("inspect key", "validation", err)
	}
	if !found {
		return nil, ErrNotFound
	}
	return value, nil
}
