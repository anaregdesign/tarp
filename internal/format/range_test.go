package format

import (
	"errors"
	"math"
	"testing"
)

func TestRecordRanges(t *testing.T) {
	raw := File{ID: 1, Kind: Log, First: 10, Last: 12}
	sparse := File{ID: 2, Kind: Cleaned, First: 10, Last: 15}
	snapshot := File{ID: 3, Kind: Snapshot}
	empty := File{Kind: Cleaned, First: 10, Last: 12}
	for _, tc := range []struct {
		entry   File
		records []uint64
		ok      bool
	}{
		{raw, []uint64{10, 11, 12}, true}, {raw, []uint64{10, 12}, false}, {raw, []uint64{10, 11}, false},
		{sparse, []uint64{10, 13}, true}, {sparse, []uint64{13, 13}, false}, {sparse, []uint64{14, 12}, false},
		{sparse, []uint64{9}, false}, {sparse, []uint64{16}, false}, {sparse, nil, false},
		{snapshot, []uint64{8, 4, 2}, true}, {snapshot, []uint64{10}, false},
		{empty, nil, true}, {empty, []uint64{10}, false},
		{File{ID: 1, Kind: Log, First: math.MaxUint64, Last: math.MaxUint64}, []uint64{math.MaxUint64}, true},
	} {
		span := NewRecordRange(tc.entry, 9)
		var err error
		for _, seq := range tc.records {
			if err = span.Accept(Record{Kind: Put, Sequence: seq}); err != nil {
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
	span := NewRecordRange(snapshot, 9)
	if err := span.Accept(Record{Kind: Delete, Sequence: 2}); !errors.Is(err, ErrCorrupt) {
		t.Fatal("accepted snapshot tombstone")
	}
}

func TestFrontierDefinesFinalRawInterval(t *testing.T) {
	c := Catalog{ActiveID: 2, Cutoff: 1, PrunedThrough: 5, Files: []File{{Kind: Cleaned, First: 2, Last: 5}}}
	f := Frontier{ActiveID: 2, Sequence: 7, ActiveEnd: SegmentHeaderBytes + 2*(RecordHeaderBytes+1)}
	got, err := c.ActiveFile(f)
	if err != nil || got.First != 6 || got.Last != 7 {
		t.Fatalf("active: %+v, %v", got, err)
	}
	for _, bad := range []Frontier{
		{ActiveID: 2, Sequence: 4, ActiveEnd: SegmentHeaderBytes},
		{ActiveID: 2, Sequence: 5, ActiveEnd: SegmentHeaderBytes + RecordHeaderBytes + 1},
		{ActiveID: 2, Sequence: 6, ActiveEnd: SegmentHeaderBytes},
		{ActiveID: 1, Sequence: 5, ActiveEnd: SegmentHeaderBytes},
	} {
		if _, err := c.ActiveFile(bad); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted frontier %+v", bad)
		}
	}
	c.Cutoff, c.PrunedThrough, c.Files = math.MaxUint64, math.MaxUint64, nil
	got, err = c.ActiveFile(Frontier{ActiveID: 2, Sequence: math.MaxUint64, ActiveEnd: SegmentHeaderBytes})
	if err != nil || got.First != 0 || got.Last != 0 {
		t.Fatalf("empty final log: %+v, %v", got, err)
	}
}
