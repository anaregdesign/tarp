package format

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"testing"
)

func TestGoldenVectors(t *testing.T) {
	uuid := [16]byte{1, 2, 3}
	for _, tc := range []struct {
		name string
		got  []byte
		hex  string
	}{
		{"segment", EncodeSegment(Segment{UUID: uuid, ID: 7, Kind: Log}), "544152505345474d00004000010000000102030000000000000000000000000007000000000000000000000000000000000000000000000000000000b1343913"},
		{"record", EncodeRecord(Put, 1, []byte{0, 255}, []byte{1, 2, 3}), "5441525052454344000001003fce7e6b01000000000000000200000000000000030000000000000013cf25fc0000000000ff010203"},
		{"frontier", EncodeFrontier(Frontier{UUID: uuid, Generation: 9, ManifestID: 8, ActiveID: 7, ActiveEnd: 64, Sequence: 6}), "544152504355525200008000000000000102030000000000000000000000000009000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000007000000000000004000000000000000060000000000000000000000000000000000000000000000000000009543bbc6"},
		{"catalog-raw", EncodeCatalog(Catalog{UUID: uuid, Cutoff: 6, PrunedThrough: 15, ActiveID: 9, Files: []File{{ID: 7, Bytes: 115, Kind: Snapshot, First: 0, Last: 0}, {ID: 8, Bytes: 128, Kind: Log, First: 7, Last: 7}}}), "544152504341544c00000000800000000000000065b4654ac8eb83ea000000000102030000000000000000000000000006000000000000000f00000000000000090000000000000002000000000000000700000000000000730000000000000002000000000000000000000000000000000000000000000008000000000000008000000000000000010000000000000007000000000000000700000000000000"},
		{"catalog-cleaned", EncodeCatalog(Catalog{UUID: uuid, Cutoff: 6, PrunedThrough: 15, ActiveID: 9, Files: []File{{ID: 7, Bytes: 115, Kind: Snapshot, First: 0, Last: 0}, {ID: 8, Bytes: 128, Kind: Log, First: 7, Last: 7}, {ID: 10, Bytes: 162, Kind: Cleaned, First: 8, Last: 15}}}), "544152504341544c00000000a800000000000000c17e831794048e53000000000102030000000000000000000000000006000000000000000f000000000000000900000000000000030000000000000007000000000000007300000000000000020000000000000000000000000000000000000000000000080000000000000080000000000000000100000000000000070000000000000007000000000000000a00000000000000a200000000000000030000000000000008000000000000000f00000000000000"},
		{"catalog-recleaned", EncodeCatalog(Catalog{UUID: uuid, Cutoff: 6, PrunedThrough: 15, ActiveID: 9, Files: []File{{ID: 7, Bytes: 115, Kind: Snapshot, First: 0, Last: 0}, {ID: 8, Bytes: 128, Kind: Log, First: 7, Last: 7}, {ID: 11, Bytes: 113, Kind: Cleaned, First: 8, Last: 15}}}), "544152504341544c00000000a800000000000000b5c306b941905ee0000000000102030000000000000000000000000006000000000000000f000000000000000900000000000000030000000000000007000000000000007300000000000000020000000000000000000000000000000000000000000000080000000000000080000000000000000100000000000000070000000000000007000000000000000b000000000000007100000000000000030000000000000008000000000000000f00000000000000"},
		{"catalog-empty", EncodeCatalog(Catalog{UUID: uuid, Cutoff: 6, PrunedThrough: 15, ActiveID: 9, Files: []File{{ID: 7, Bytes: 115, Kind: Snapshot, First: 0, Last: 0}, {ID: 8, Bytes: 128, Kind: Log, First: 7, Last: 7}, {ID: 0, Bytes: 0, Kind: Cleaned, First: 8, Last: 15}}}), "544152504341544c00000000a80000000000000007ce3133369e1a16000000000102030000000000000000000000000006000000000000000f0000000000000009000000000000000300000000000000070000000000000073000000000000000200000000000000000000000000000000000000000000000800000000000000800000000000000001000000000000000700000000000000070000000000000000000000000000000000000000000000030000000000000008000000000000000f00000000000000"},
		{"backup", EncodeBackup(Backup{UUID: uuid, Sequence: 6, Cutoff: 3, SourceCacheBytes: 512, SourceRecordBytes: 128, Files: []BackupFile{{Name: "CURRENT", Bytes: 128}}}), "544152504241434b00000000710000000000000087968015af5a971e0000000001020300000000000000000000000000060000000000000003000000000000000002000000000000800000000000000000000000000000000100000000000000070043555252454e5480000000000000000000000000000000000000000000000000000000000000000000000000000000"},
	} {
		expected, err := hex.DecodeString(tc.hex)
		if err != nil || !bytes.Equal(tc.got, expected) {
			t.Fatalf("%s changed wire bytes: %x; %v", tc.name, tc.got, err)
		}
	}
}

func TestDecodeBackupWithFormerFileLimit(t *testing.T) {
	// A backup written with a 256-byte source file limit.
	wire, err := hex.DecodeString("544152504241434b00000000710000000000000085798e6197641f730000000001020300000000000000000000000000060000000000000003000000000000000002000000000000800000000000000000010000000000000100000000000000070043555252454e5480000000000000000000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	backup, err := DecodeBackup(wire)
	want := Backup{UUID: [16]byte{1, 2, 3}, Sequence: 6, Cutoff: 3, SourceCacheBytes: 512, SourceRecordBytes: 128, Files: []BackupFile{{Name: "CURRENT", Bytes: 128}}}
	if err != nil || !reflect.DeepEqual(backup, want) {
		t.Fatalf("backup: %#v, %v", backup, err)
	}
}

func TestWireImages(t *testing.T) {
	uuid := [16]byte{1, 2, 3}
	segment := Segment{UUID: uuid, ID: 7, Kind: Log}
	if got, err := DecodeSegment(EncodeSegment(segment)); err != nil || got != segment {
		t.Fatalf("segment: %#v %v", got, err)
	}
	frontier := Frontier{UUID: uuid, Generation: 9, ManifestID: 8, ActiveID: 7, ActiveEnd: 64, Sequence: 6}
	if got, err := DecodeFrontier(EncodeFrontier(frontier)); err != nil || got != frontier {
		t.Fatalf("frontier: %#v %v", got, err)
	}
	catalog := Catalog{UUID: uuid, ActiveID: 7, Cutoff: 3, PrunedThrough: 5, Files: []File{{ID: 1, Bytes: 113, Kind: Snapshot}, {ID: 4, Bytes: 113, Kind: Log, First: 4, Last: 4}, {Kind: Cleaned, First: 5, Last: 5}}}
	if got, err := DecodeCatalog(EncodeCatalog(catalog)); err != nil || !reflect.DeepEqual(got, catalog) {
		t.Fatalf("catalog: %#v %v", got, err)
	}
	backup := Backup{UUID: uuid, Sequence: 6, Cutoff: 3, SourceCacheBytes: 512, Files: []BackupFile{{Name: "CURRENT", Bytes: 128}}}
	if got, err := DecodeBackup(EncodeBackup(backup)); err != nil || !reflect.DeepEqual(got, backup) {
		t.Fatalf("backup: %#v %v", got, err)
	}
	for _, wire := range []struct {
		b      []byte
		decode func([]byte) error
	}{
		{EncodeSegment(segment), func(b []byte) error { _, e := DecodeSegment(b); return e }},
		{EncodeFrontier(frontier), func(b []byte) error { _, e := DecodeFrontier(b); return e }},
		{EncodeCatalog(catalog), func(b []byte) error { _, e := DecodeCatalog(b); return e }},
		{EncodeBackup(backup), func(b []byte) error { _, e := DecodeBackup(b); return e }},
	} {
		for i := range wire.b {
			b := bytes.Clone(wire.b)
			b[i] ^= 0x80
			if wire.decode(b) == nil {
				t.Fatalf("accepted bit flip at %d/%d", i, len(b))
			}
		}
		for i := range wire.b {
			if wire.decode(wire.b[:i]) == nil {
				t.Fatalf("accepted truncated image at %d", i)
			}
		}
	}
}

func TestDecodeRecordValidatesFieldsWithAnIntactChecksum(t *testing.T) {
	wire := EncodeRecord(Put, 1, []byte("k"), []byte("v"))
	const exactSize = RecordHeaderBytes + 2
	for _, tc := range []struct {
		name      string
		change    func([]byte)
		available int64
	}{
		{"unknown kind", func(b []byte) { b[10] = 3 }, exactSize},
		{"zero sequence", func(b []byte) { order.PutUint64(b[16:], 0) }, exactSize},
		{"empty key", func(b []byte) { order.PutUint64(b[24:], 0) }, exactSize},
		{"delete carrying a value", func(b []byte) { b[10] = Delete }, exactSize},
		{"reserved signature field", func(b []byte) { b[8] = 1 }, exactSize},
		{"unknown flags", func(b []byte) { b[11] = 1 }, exactSize},
		{"reserved trailing field", func(b []byte) { b[47] = 1 }, exactSize},
		{"key overflows record size", func(b []byte) { order.PutUint64(b[24:], math.MaxUint64) }, math.MaxInt64},
		{"value overflows record size", func(b []byte) { order.PutUint64(b[32:], math.MaxInt64-RecordHeaderBytes) }, math.MaxInt64},
		{"value past committed end", func(b []byte) { order.PutUint64(b[32:], 2) }, exactSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := bytes.Clone(wire[:RecordHeaderBytes])
			tc.change(header)
			clear(header[12:16])
			order.PutUint32(header[12:], crc32.Checksum(header, table))
			if _, err := DecodeRecord(header, tc.available); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("DecodeRecord returned %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestDecodeRecordAcceptsLargestRepresentableRecord(t *testing.T) {
	header := EncodeRecord(Put, math.MaxUint64, []byte("k"), nil)[:RecordHeaderBytes]
	order.PutUint64(header[32:], math.MaxInt64-RecordHeaderBytes-1)
	clear(header[12:16])
	order.PutUint32(header[12:], crc32.Checksum(header, table))
	record, err := DecodeRecord(header, math.MaxInt64)
	if err != nil || record.Bytes() != math.MaxInt64 || record.Sequence != math.MaxUint64 {
		t.Fatalf("maximum legal size and sequence: %+v, %v", record, err)
	}
}

func TestScanCommittedRanges(t *testing.T) {
	for _, value := range [][]byte{nil, {0, 1, 0xff}} {
		record := EncodeRecord(Put, 1, []byte{0, 0xff}, value)
		all := append(EncodeSegment(Segment{ID: 1, Kind: Log}), record...)
		found := 0
		if err := Scan(t.Context(), bytes.NewReader(all), int64(len(all)), func(off int64, r Record, k string) error {
			got, err := ReadValue(bytes.NewReader(all), off, r.Bytes(), k, r.Sequence)
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("read: %x %v", got, err)
			}
			found++
			return nil
		}); err != nil || found != 1 {
			t.Fatalf("scan: %d %v", found, err)
		}
		for end := 65; end < len(all); end++ {
			if err := Scan(t.Context(), bytes.NewReader(all), int64(end), func(int64, Record, string) error { return nil }); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("truncation %d: %v", end, err)
			}
		}
		all[len(all)-1] ^= 1
		if err := Scan(t.Context(), bytes.NewReader(all), int64(len(all)), func(int64, Record, string) error { return nil }); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("payload corruption: %v", err)
		}
	}
}

type readerAtFunc func([]byte, int64) (int, error)

func (f readerAtFunc) ReadAt(b []byte, off int64) (int, error) { return f(b, off) }

func TestScanStopsDuringLargeRecord(t *testing.T) {
	data := EncodeSegment(Segment{ID: 1, Kind: Log})
	data = append(data, EncodeRecord(Put, 1, []byte("key"), make([]byte, streamChunk*8))...)
	source := bytes.NewReader(data)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := readerAtFunc(func(b []byte, off int64) (int, error) {
		if ctx.Err() != nil {
			t.Fatal("read another chunk after cancellation")
		}
		if off > SegmentHeaderBytes {
			cancel()
		}
		return source.ReadAt(b, off)
	})
	err := Scan(ctx, input, int64(len(data)), func(int64, Record, string) error {
		t.Fatal("visited a partially checked record")
		return nil
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("interrupted scan returned %v", err)
	}
}

func TestScanBatchesOnlyCommittedBytes(t *testing.T) {
	data := EncodeSegment(Segment{ID: 1, Kind: Log})
	const smallRecords = 400
	for i := range smallRecords {
		data = append(data, EncodeRecord(Put, uint64(i+1), []byte("k"), []byte("v"))...)
	}
	largeKey := bytes.Repeat([]byte{0xff}, streamChunk+1)
	largeValue := bytes.Repeat([]byte{0xee}, 2*streamChunk+1)
	data = append(data, EncodeRecord(Put, smallRecords+1, largeKey, largeValue)...)
	end := int64(len(data))
	data = append(data, 0xff) // An active log can have an uncommitted tail.
	source := bytes.NewReader(data)
	pos, calls := int64(SegmentHeaderBytes), 0
	input := readerAtFunc(func(b []byte, off int64) (int, error) {
		if off != pos || off+int64(len(b)) > end || len(b) > streamChunk {
			t.Fatalf("out-of-range or repeated read at %d, length %d", off, len(b))
		}
		calls++
		pos += int64(len(b))
		return source.ReadAt(b, off)
	})
	seen, next := 0, int64(SegmentHeaderBytes)
	err := Scan(t.Context(), input, end, func(off int64, r Record, key string) error {
		seen++
		if off != next || r.Sequence != uint64(seen) {
			t.Fatalf("unexpected record %d at %d", r.Sequence, off)
		}
		next += r.Bytes()
		if seen <= smallRecords && key != "k" || seen > smallRecords && key != string(largeKey) {
			t.Fatal("key mismatch crossing the read buffer")
		}
		return nil
	})
	if err != nil || next != end || pos != end || seen != smallRecords+1 || calls > 8 {
		t.Fatalf("scan saw %d records in %d reads ending at %d/%d, want %d: %v", seen, calls, pos, next, end, err)
	}
	data[end-1] ^= 1
	err = Scan(t.Context(), bytes.NewReader(data), end, func(int64, Record, string) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt last byte of streamed value: %v", err)
	}
}

func TestScanHandlesReadAtResults(t *testing.T) {
	record := EncodeRecord(Put, 1, []byte("k"), nil)
	end := int64(SegmentHeaderBytes + len(record))
	failure := errors.New("data file read failed")
	for _, tc := range []struct {
		name       string
		n          int
		err        error
		expect     error
		wantVisits int
	}{
		{"full final buffer with EOF", len(record), io.EOF, nil, 1},
		{"full buffer with error", len(record), failure, failure, 0},
		{"short read without error", len(record) - 1, nil, ErrCorrupt, 0},
		{"truncated data", len(record) - 1, io.EOF, ErrCorrupt, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := readerAtFunc(func(dst []byte, _ int64) (int, error) {
				copy(dst, record[:tc.n])
				return tc.n, tc.err
			})
			var visits int
			err := Scan(t.Context(), src, end, func(int64, Record, string) error {
				visits++
				return nil
			})
			if !errors.Is(err, tc.expect) || visits != tc.wantVisits {
				t.Fatalf("visited %d records, got %v, want %v", visits, err, tc.expect)
			}
		})
	}
}

func FuzzDecoders(f *testing.F) {
	for _, b := range [][]byte{
		EncodeSegment(Segment{ID: 1, Kind: Cleaned}),
		EncodeCatalog(Catalog{ActiveID: 1}),
		EncodeCatalog(Catalog{ActiveID: 1, PrunedThrough: 5, Files: []File{
			{ID: 2, Bytes: 128, Kind: Log, First: 1, Last: 1},
			{Kind: Cleaned, First: 2, Last: 2},
			{ID: 3, Bytes: 128, Kind: Cleaned, First: 3, Last: 5},
		}}),
		EncodeRecord(Put, 1, []byte("k"), nil),
		EncodeFrontier(Frontier{Generation: 1, ManifestID: 2, ActiveID: 1, ActiveEnd: SegmentHeaderBytes}),
		EncodeBackup(Backup{}),
	} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = DecodeSegment(b)
		_, _ = DecodeFrontier(b)
		_, _ = DecodeCatalog(b)
		_, _ = DecodeBackup(b)
		if len(b) >= RecordHeaderBytes {
			_, _ = DecodeRecord(b[:RecordHeaderBytes], int64(len(b)))
		}
	})
}

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

type countedReaderAt struct {
	io.ReaderAt
	calls      int
	shortCount bool
}

func (r *countedReaderAt) ReadAt(b []byte, off int64) (int, error) {
	r.calls++
	n, err := r.ReaderAt.ReadAt(b, off)
	if r.shortCount && n > 0 {
		n--
	}
	return n, err
}

func TestIndexedReadBoundariesAndValidation(t *testing.T) {
	const key = "a\x00b"
	for _, tc := range []struct {
		name  string
		size  int
		reads int
	}{
		{"empty value", RecordHeaderBytes + len(key), 1},
		{"exact buffer", indexedReadBufferBytes, 1},
		{"above buffer", indexedReadBufferBytes + 1, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value := bytes.Repeat([]byte{0x71}, tc.size-RecordHeaderBytes-len(key))
			record := EncodeRecord(Put, 42, []byte(key), value)
			file := append([]byte{0xff}, record...)
			reader := &countedReaderAt{ReaderAt: bytes.NewReader(file)}
			got, err := ReadValue(reader, 1, int64(tc.size), key, 42)
			if err != nil || got == nil || !bytes.Equal(got, value) || reader.calls != tc.reads {
				t.Fatalf("decoded %d bytes in %d reads, err=%v", len(got), reader.calls, err)
			}
			for _, wrong := range []struct {
				key string
				seq uint64
			}{{"b\x00a", 42}, {key, 41}} {
				if _, err := ReadValue(reader, 1, int64(tc.size), wrong.key, wrong.seq); !errors.Is(err, ErrCorrupt) {
					t.Fatalf("accepted mismatched identity: %v", err)
				}
			}
			file[len(file)-1] ^= 1
			if _, err := ReadValue(reader, 1, int64(tc.size), key, 42); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted modified payload: %v", err)
			}
			if !bytes.Equal(got, value) {
				t.Fatal("a later indexed read changed the previously returned value")
			}
			reader.shortCount = true
			if _, err := ReadValue(reader, 1, int64(tc.size), key, 42); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("short read: %v", err)
			}
		})
	}
}

func TestIndexedReadRejectsInvalidBoundsBeforeIO(t *testing.T) {
	reader := &countedReaderAt{ReaderAt: bytes.NewReader(nil)}
	for _, tc := range []struct{ off, size int64 }{{0, -1}, {-1, 64}, {math.MaxInt64 - 32, 64}, {0, RecordHeaderBytes}} {
		if _, err := ReadValue(reader, tc.off, tc.size, "k", 1); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("bounds %+v: %v", tc, err)
		}
	}
	if reader.calls != 0 {
		t.Fatalf("read %d times for invalid indexed locations", reader.calls)
	}
}
