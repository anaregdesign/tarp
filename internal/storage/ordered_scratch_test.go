package storage

import (
	"errors"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestOrderedScratchCountsSimultaneousMergeInputsAndOutput(t *testing.T) {
	const bytesPerEvent = replayHeaderBytes + 1
	b := &orderedScratchBudget{remaining: 2 * bytesPerEvent}
	s := sortedReplay{ctx: t.Context(), dir: t.TempDir(), limit: replayKeyCharge + 1, budget: b}
	for i, k := range []string{"z", "a"} {
		if err := s.apply(&DataSegment{ID: 11, Kind: format.RawLogKind}, int64(64+50*i), format.RecordHeader{Kind: format.Put, Sequence: uint64(i + 1), KeyBytes: 1}, k); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.each(func(replayEvent) error { return nil }); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("merge exceeded simultaneous allowance: %v", err)
	}
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	if b.remaining != 2*bytesPerEvent {
		t.Fatalf("scratch allowance after discard = %d", b.remaining)
	}
}
