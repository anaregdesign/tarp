package format

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"testing"
)

func TestCatalogRejectsAmbiguousCoverage(t *testing.T) {
	const size = SegmentHeaderBytes + RecordHeaderBytes + 1
	raw := File{ID: 2, Bytes: size, Kind: Log, First: 4, Last: 4}
	snapshot := File{ID: 3, Bytes: size, Kind: Snapshot}
	valid := Catalog{Cutoff: 3, PrunedThrough: 6, ActiveID: 7}
	for _, tc := range []struct {
		name  string
		files []File
	}{
		{"start gap", []File{{ID: 2, Bytes: size, Kind: Log, First: 5, Last: 5}}},
		{"start overlap", []File{{ID: 2, Bytes: size, Kind: Log, First: 3, Last: 4}}},
		{"duplicate ID", []File{raw, {ID: 2, Bytes: size, Kind: Cleaned, First: 5, Last: 6}}},
		{"overlapping intervals", []File{raw, {Kind: Cleaned, First: 4, Last: 6}}},
		{"reversed interval", []File{raw, {Kind: Cleaned, First: 5, Last: 4}}},
		{"empty raw interval", []File{{Kind: Log, First: 4, Last: 4}}},
		{"missing physical length", []File{{ID: 2, Kind: Cleaned, First: 4, Last: 4}}},
		{"empty interval has bytes", []File{{Bytes: size, Kind: Cleaned, First: 4, Last: 4}}},
		{"past prune boundary", []File{{Kind: Cleaned, First: 4, Last: 7}}},
		{"snapshot after log", []File{raw, snapshot}},
		{"snapshot with interval", []File{{ID: 3, Bytes: size, Kind: Snapshot, First: 1, Last: 2}}},
		{"active ID reused", []File{{ID: 7, Bytes: size, Kind: Snapshot}}},
		{"raw records exceed physical length", []File{{ID: 2, Bytes: size, Kind: Log, First: 4, Last: 5}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			c.Files = tc.files
			if _, err := DecodeCatalog(EncodeCatalog(c)); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted invalid coverage: %v", err)
			}
		})
	}
	for _, c := range []Catalog{
		{ActiveID: 7, Cutoff: 2, PrunedThrough: 1},
		{ActiveID: 7, Cutoff: 0, Files: []File{snapshot}},
		{ActiveID: 7, Cutoff: math.MaxUint64, PrunedThrough: math.MaxUint64, Files: []File{{Kind: Cleaned, First: 0, Last: 0}}},
	} {
		if _, err := DecodeCatalog(EncodeCatalog(c)); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted invalid catalog %+v", c)
		}
	}
}

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

type boundedReader struct {
	*bytes.Reader
	max int
}

func (r *boundedReader) ReadAt(b []byte, off int64) (int, error) {
	if len(b) > r.max {
		return 0, errors.New("unbounded read")
	}
	return r.Reader.ReadAt(b, off)
}

type boundedWriter struct {
	bytes.Buffer
	max int
}

func (w *boundedWriter) Write(b []byte) (int, error) {
	if len(b) > w.max {
		return 0, errors.New("unbounded write")
	}
	return w.Buffer.Write(b)
}

func TestSelectRecordChecksDiscardedValuesAndBoundsKeyIO(t *testing.T) {
	key := bytes.Repeat([]byte{0, 0xff}, streamChunk+1)
	value := bytes.Repeat([]byte{1}, 3*streamChunk)
	b := EncodeRecord(Put, 6, key, value)
	run := func(dst io.Writer) error {
		reader := &boundedReader{bytes.NewReader(b), streamChunk}
		_, err := SelectRecord(context.Background(), reader, 0, int64(len(b)), make([]byte, 4*streamChunk), func(r Record, k string) (io.Writer, error) {
			if k != string(key) || r.Sequence != 6 {
				t.Fatal("incorrect key or sequence")
			}
			return dst, nil
		})
		return err
	}
	w := &boundedWriter{max: streamChunk}
	if err := run(w); err != nil || !bytes.Equal(b, w.Bytes()) {
		t.Fatalf("retained copy: %v", err)
	}
	if err := run(nil); err != nil {
		t.Fatalf("discarded record: %v", err)
	}
	b[len(b)-1] ^= 1
	if err := run(nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("discarded corrupt value: %v", err)
	}
}

func TestOnlyUnknownOuterSignaturesAreUnsupported(t *testing.T) {
	outer := []func([]byte) error{
		func(b []byte) error { _, err := DecodeFrontier(b); return err },
		func(b []byte) error { _, err := DecodeBackup(b); return err },
	}
	for _, decode := range outer {
		if err := decode([]byte("unknown!")); !errors.Is(err, ErrUnsupported) {
			t.Fatal(err)
		}
		if err := decode([]byte("short")); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
	for _, fn := range []func([]byte) error{
		func(b []byte) error { _, err := DecodeSegment(b[:SegmentHeaderBytes]); return err },
		func(b []byte) error { _, err := DecodeCatalog(b); return err },
		func(b []byte) error { _, err := DecodeRecord(b[:RecordHeaderBytes], RecordHeaderBytes); return err },
	} {
		if err := fn(make([]byte, 128)); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
	}
}
