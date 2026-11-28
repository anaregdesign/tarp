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

// CheckInitialOrderedSpace checks a proven lower bound on new DATA before the
// active log is sealed. The caller holds the writer gate and has established
// that there are no snapshot segments to reuse.
func (s *Store) CheckInitialOrderedSpace(liveRecordBytes uint64) error {
	if liveRecordBytes == 0 {
		return nil
	}
	entries := s.catalogEntries.Len()
	if s.Current.ActiveCommittedEndOffset != format.SegmentHeaderBytes {
		entries++
	}
	available, err := s.orderedScratchAllowance(entries)
	if err != nil {
		return err
	}
	if liveRecordBytes > available {
		return fmt.Errorf("ordered checkpoint requires at least %d bytes for live DATA records with %d staging bytes available: %w", liveRecordBytes, available, syscall.ENOSPC)
	}
	return nil
}

func (c *DataCheckpoint) prepareOrderedScratch(moves *Relocations) error {
	if c.scratch == nil {
		available, err := c.store.orderedScratchAllowance(len(c.capture.Catalog.Entries))
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		c.scratch = &orderedScratchBudget{remaining: available}
	}
	moves.budget = c.scratch
	return nil
}

func (s *Store) orderedScratchAllowance(entries int) (uint64, error) {
	availableBytes := platform.AvailableBytes
	if s.IO != nil && s.IO.AvailableBytes != nil {
		availableBytes = s.IO.AvailableBytes
	}
	available, err := availableBytes(s.Dir)
	if err != nil {
		return 0, err
	}
	target := uint64(s.Config.TargetSegmentBytes)
	// Keep space for the writer's next segment and sidecar, the current
	// catalog-sized publication, selector, and directory allocation blocks.
	for _, reserve := range []uint64{target, format.ParitySidecarBytes(target), format.CatalogEncodedBytes(entries), format.FrontierBytes, 4 * 4096} {
		available -= min(available, reserve)
	}
	return available, nil
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
