package storage

import (
	"fmt"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

// Each ordered checkpoint has one staging allowance. Temporary sort and
// relocation files return their charges on close. New DATA and its anticipated
// PARITY stay charged through publication; carried-forward files cost nothing.
type orderedScratchBudget struct{ remaining uint64 }

func (c *DataCheckpoint) prepareOrderedScratch(moves *Relocations) error {
	if c.scratch == nil {
		available, err := platform.AvailableBytes(c.store.Dir)
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		target := uint64(c.store.Config.TargetSegmentBytes)
		// Keep space for the writer's next segment and sidecar, the current
		// catalog-sized publication, selector, and directory allocation blocks.
		for _, reserve := range []uint64{target, format.ParitySidecarBytes(target), format.CatalogEncodedBytes(len(c.capture.Catalog.Entries)), format.FrontierBytes, 4 * 4096} {
			available -= min(available, reserve)
		}
		c.scratch = &orderedScratchBudget{remaining: available}
	}
	moves.budget = c.scratch
	return nil
}

func (b *orderedScratchBudget) grow(n uint64) error {
	if b == nil {
		return nil
	}
	if n > b.remaining {
		return fmt.Errorf("ordered checkpoint staging allowance: %w", syscall.ENOSPC)
	}
	b.remaining -= n
	return nil
}

func (b *orderedScratchBudget) release(n uint64) {
	if b != nil {
		b.remaining += n
	}
}

func (b *orderedScratchBudget) growOutput(oldEnd, newEnd int64) error {
	if b == nil {
		return nil
	}
	oldBytes, newBytes := uint64(oldEnd), uint64(newEnd)
	bytes := newBytes - oldBytes + (format.ParitySidecarBytes(newBytes) - format.ParitySidecarBytes(oldBytes))
	return b.grow(bytes)
}
