package storage

import (
	"context"
	"errors"
	"testing"
)

func TestGuardLossesWaitsForRunningRepairBeforePublication(t *testing.T) {
	r := &sealedRepair{running: &readRepair{done: make(chan struct{})}}
	r.unavailable.Store(&UnavailableDataError{FileID: 7, Err: ErrCorrupt})
	c := &DataCheckpoint{lossSources: []lossSource{{r, 0}}}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// The in-flight repair can only be awaited outside the writer gate.
	if release, err := c.GuardLosses(ctx); !errors.Is(err, context.Canceled) || release != nil {
		t.Fatalf("pending repair: %v", err)
	}
	close(r.running.done)
	r.running = nil
	// Another failed attempt leaves the same confirmed loss eligible.
	release, err := c.GuardLosses(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if r.mu.TryLock() {
		r.mu.Unlock()
		release()
		t.Fatal("published loss was not fenced against concurrent repair")
	}
	release()
	// Even if another failure follows a successful repair, evidence from the
	// captured repair generation can no longer authorize a deletion.
	r.repairGeneration.Add(1)
	if release, err := c.GuardLosses(t.Context()); !errors.Is(err, ErrLossSourceRepaired) || release != nil {
		t.Fatalf("obsolete loss evidence: %v", err)
	}
}
