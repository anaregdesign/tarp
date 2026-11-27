package storage

import (
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestRecoveryInfoBoundsReplayAtFirstRejectedFile(t *testing.T) {
	const changed = maxIndexedReplayBytes + 1
	image := &Image{
		Current: format.Frontier{ActiveID: 1, LastUserSequence: 1, ActiveCommittedEndOffset: changed},
		Catalog: format.Catalog{ActiveID: 1, Index: format.IndexCheckpoint{ID: 2}},
		Segments: map[uint64]*DataSegment{
			1: {ID: 1, Kind: format.RawLogKind},
		},
	}
	_, _, err := prepareIndexReplay(image, &selectedIndexFile{})
	if !IsIndexCheckpointUnusable(err) {
		t.Fatalf("replay over the fast-path budget = %v", err)
	}
	info := RecoveryInfo{}
	info.noteCheckpointFailure(err)
	if info.Reason != RecoveryReplayBudget || info.ChangedDataBytes != changed || info.ChangedDataBytesExact {
		t.Fatalf("budget fallback = %+v; want a %d-byte lower bound", info, changed)
	}
}
