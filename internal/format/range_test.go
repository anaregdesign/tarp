package format

import (
	"errors"
	"math"
	"testing"
)

func TestRecordSequenceValidator(t *testing.T) {
	raw := CatalogEntry{ID: 1, Kind: RawLogKind, FirstSequence: 10, LastSequence: 12}
	sparse := CatalogEntry{ID: 2, Kind: CleanedLogKind, FirstSequence: 10, LastSequence: 15}
	snapshot := CatalogEntry{ID: 3, Kind: SnapshotKind}
	empty := CatalogEntry{Kind: CleanedLogKind, FirstSequence: 10, LastSequence: 12}
	for _, tc := range []struct {
		entry   CatalogEntry
		records []uint64
		ok      bool
	}{
		{raw, []uint64{10, 11, 12}, true}, {raw, []uint64{10, 12}, false}, {raw, []uint64{10, 11}, false},
		{sparse, []uint64{10, 13}, true}, {sparse, []uint64{13, 13}, false}, {sparse, []uint64{14, 12}, false},
		{sparse, []uint64{9}, false}, {sparse, []uint64{16}, false}, {sparse, nil, false},
		{snapshot, []uint64{8, 4, 2}, true}, {snapshot, []uint64{10}, false},
		{empty, nil, true}, {empty, []uint64{10}, false},
		{CatalogEntry{ID: 1, Kind: RawLogKind, FirstSequence: math.MaxUint64, LastSequence: math.MaxUint64}, []uint64{math.MaxUint64}, true},
	} {
		span := NewRecordSequenceValidator(tc.entry, 9)
		var err error
		for _, seq := range tc.records {
			if err = span.Accept(RecordHeader{Kind: Put, Sequence: seq}); err != nil {
				break
			}
		}
		if err == nil {
			err = span.Finish()
		}
		if (err == nil) != tc.ok || err != nil && !errors.Is(err, ErrCorrupt) {
			t.Errorf("entry %+v, records %v: %v", tc.entry, tc.records, err)
		}
	}
	span := NewRecordSequenceValidator(snapshot, 9)
	if err := span.Accept(RecordHeader{Kind: Delete, Sequence: 2}); !errors.Is(err, ErrCorrupt) {
		t.Fatal("accepted snapshot tombstone")
	}
}

func TestFrontierDefinesFinalRawInterval(t *testing.T) {
	c := Catalog{ActiveID: 2, CheckpointCutoffSequence: 1, PrunedThroughSequence: 5, Entries: []CatalogEntry{{Kind: CleanedLogKind, FirstSequence: 2, LastSequence: 5}}}
	f := Frontier{ActiveID: 2, LastUserSequence: 7, ActiveCommittedEndOffset: SegmentHeaderBytes + 2*(RecordHeaderBytes+1)}
	got, err := c.ActiveEntry(f)
	if err != nil || got.FirstSequence != 6 || got.LastSequence != 7 {
		t.Fatalf("active: %+v, %v", got, err)
	}
	for _, bad := range []Frontier{
		{ActiveID: 2, LastUserSequence: 4, ActiveCommittedEndOffset: SegmentHeaderBytes},
		{ActiveID: 2, LastUserSequence: 5, ActiveCommittedEndOffset: SegmentHeaderBytes + RecordHeaderBytes + 1},
		{ActiveID: 2, LastUserSequence: 6, ActiveCommittedEndOffset: SegmentHeaderBytes},
		{ActiveID: 1, LastUserSequence: 5, ActiveCommittedEndOffset: SegmentHeaderBytes},
	} {
		if _, err := c.ActiveEntry(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted frontier %+v", bad)
		}
	}
	c.CheckpointCutoffSequence, c.PrunedThroughSequence, c.Entries = math.MaxUint64, math.MaxUint64, nil
	got, err = c.ActiveEntry(Frontier{ActiveID: 2, LastUserSequence: math.MaxUint64, ActiveCommittedEndOffset: SegmentHeaderBytes})
	if err != nil || got.FirstSequence != 0 || got.LastSequence != 0 {
		t.Fatalf("empty final log: %+v, %v", got, err)
	}
}

func TestRecordSequenceValidatorSkipsOnlyAcrossReportedGaps(t *testing.T) {
	r := NewRecordSequenceValidator(CatalogEntry{ID: 1, Kind: RawLogKind, FirstSequence: 10, LastSequence: 18}, 0)
	for _, seq := range []uint64{8, 19} {
		if !errors.Is(r.AcceptAfterGap(RecordHeader{Kind: Put, Sequence: seq}), ErrCorrupt) {
			t.Fatalf("accepted out-of-range sequence %d", seq)
		}
	}
	if err := r.AcceptAfterGap(RecordHeader{Kind: Put, Sequence: 14}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(r.AcceptAfterGap(RecordHeader{Kind: Put, Sequence: 13}), ErrCorrupt) {
		t.Fatal("accepted decreasing sequence after a gap")
	}
	if !errors.Is(r.Accept(RecordHeader{Kind: Put, Sequence: 16}), ErrCorrupt) {
		t.Fatal("accepted unexplained gap")
	}
	if err := r.Accept(RecordHeader{Kind: Put, Sequence: 15}); err != nil {
		t.Fatal(err)
	}
}
