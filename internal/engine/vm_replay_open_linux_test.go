package engine_test

import (
	"context"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
)

// Exercise the root public API with a genuinely zero-valued Options.
func init() {
	engine.VMReplayOpenExisting = func(ctx context.Context, dir string) (*engine.DB, error) {
		db, err := tarp.OpenExisting(ctx, dir, tarp.Options{})
		return (*engine.DB)(db), err
	}
}
