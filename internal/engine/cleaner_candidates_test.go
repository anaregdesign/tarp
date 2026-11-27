package engine

import (
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestCleanerCandidateQueueBoundsWorkAndRetriesReleasedPins(t *testing.T) {
	s := &storage.Store{Segments: make(map[uint64]*storage.DataSegment)}
	var q cleanerCandidates
	const size, files = 2048, 3 * cleanerCandidateWindow
	for i := range files {
		id := uint64(i + 1)
		entry := format.CatalogEntry{ID: id, Kind: format.RawLogKind, Bytes: size}
		segment := &storage.DataSegment{}
		s.Segments[id] = segment
		segment.LiveBytes.Store(size - format.SegmentHeaderBytes)
		q.track(s, entry, i, 4096)
		if q.pending.Len() != i {
			t.Fatal("full segment became a candidate")
		}
		segment.LiveBytes.Store(128)
		q.changed(q.files[id], segment, 4096)
		q.changed(q.files[id], segment, 4096)
	}
	if q.pending.Len() != files || q.debt != files*(size-format.SegmentHeaderBytes-128) {
		t.Fatalf("queue length %d, projected DATA savings %d", q.pending.Len(), q.debt)
	}
	taken := q.take()
	if len(taken) != cleanerCandidateWindow || q.pending.Len() != files-cleanerCandidateWindow {
		t.Fatal("candidate work exceeded the pass limit")
	}
	for _, file := range taken {
		q.deferPin(file, 1)
	}
	q.resumePins(1, true)
	if q.pinned.Len() != cleanerCandidateWindow {
		t.Fatal("pin-blocked work retried without a pin release")
	}
	q.resumePins(2, true)
	if q.pinned.Len() != 0 || q.pending.Len() != files {
		t.Fatal("pin release did not return candidates to the work queue")
	}
	q.remove(taken[0].entry.ID)
	if q.pending.Len() != files-1 || q.debt != (files-1)*(size-format.SegmentHeaderBytes-128) {
		t.Fatal("retired work remained in the queue or DATA savings")
	}
}
