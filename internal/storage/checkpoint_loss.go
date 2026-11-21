package storage

import (
	"context"
	"errors"
)

// ErrLossSourceRepaired asks the engine to capture the available DATA again.
var ErrLossSourceRepaired = errors.New("a checkpoint loss source was repaired")

type lossSource struct {
	repair     *sealedRepair
	generation uint64
}

// GuardLosses waits outside the writer gate for attempts that could restore
// discarded records. Hold the affected files' repair locks in catalog order
// through the CURRENT switch so a successful repair cannot race publication.
func (c *Checkpoint) GuardLosses(ctx context.Context) (func(), error) {
	if len(c.lossSources) == 0 {
		return func() {}, nil
	}
	for {
		var running <-chan struct{}
		held := 0
		release := func() {
			for i := held - 1; i >= 0; i-- {
				c.lossSources[i].repair.mu.Unlock()
			}
		}
		for _, src := range c.lossSources {
			r := src.repair
			r.mu.Lock()
			held++
			switch {
			case r.uncertain != nil:
				err := &UncertainReadRepair{Err: r.uncertain}
				release()
				return nil, err
			case r.generation.Load() != src.generation || r.unavailable.Load() == nil:
				release()
				return nil, ErrLossSourceRepaired
			case r.running != nil:
				running = r.running.done
			}
			if running != nil {
				break
			}
		}
		if running == nil {
			return release, nil
		}
		release()
		select {
		case <-running:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
