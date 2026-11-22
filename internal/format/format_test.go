package format

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
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
		{"segment", EncodeSegmentHeader(SegmentHeader{UUID: uuid, ID: 7, Kind: RawLogKind}), "544152505345474d00004000010000000102030000000000000000000000000007000000000000000000000000000000000000000000000000000000b1343913"},
		{"record", EncodeRecord(Put, 1, []byte{0, 255}, []byte{1, 2, 3}), "5441525052454344000001003fce7e6b01000000000000000200000000000000030000000000000013cf25fc0000000000ff010203"},
		{"frontier", EncodeFrontier(Frontier{UUID: uuid, PublicationGeneration: 9, ManifestID: 8, ActiveID: 7, ActiveCommittedEndOffset: 64, LastUserSequence: 6}), "544152504845414400008000000000000102030000000000000000000000000009000000000000000800000000000000000000000000000000000000000000000000000000000000000000000000000007000000000000004000000000000000060000000000000000000000000000000000000000000000000000001dd11936"},
		{"catalog-raw", EncodeCatalog(Catalog{UUID: uuid, CheckpointCutoffSequence: 6, PrunedThroughSequence: 15, ActiveID: 9, Entries: []CatalogEntry{{ID: 7, DataFileDigest: [32]byte{7}, Bytes: 115, Kind: SnapshotKind, FirstSequence: 0, LastSequence: 0}, {ID: 8, DataFileDigest: [32]byte{8}, Bytes: 128, Kind: RawLogKind, FirstSequence: 7, LastSequence: 7}}}), "544152504d4e4654000000002001000000000000760f67d09dd76084000000000102030000000000000000000000000006000000000000000f0000000000000009000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000070000000000000073000000000000000200000000000000000000000000000000000000000000000700000000000000000000000000000000000000000000000000000000000000080000000000000080000000000000000100000000000000070000000000000007000000000000000800000000000000000000000000000000000000000000000000000000000000"},
		{"catalog-cleaned", EncodeCatalog(Catalog{UUID: uuid, CheckpointCutoffSequence: 6, PrunedThroughSequence: 15, ActiveID: 9, Entries: []CatalogEntry{{ID: 7, DataFileDigest: [32]byte{7}, Bytes: 115, Kind: SnapshotKind, FirstSequence: 0, LastSequence: 0}, {ID: 8, DataFileDigest: [32]byte{8}, Bytes: 128, Kind: RawLogKind, FirstSequence: 7, LastSequence: 7}, {ID: 10, DataFileDigest: [32]byte{10}, Bytes: 162, Kind: CleanedLogKind, FirstSequence: 8, LastSequence: 15}}}), "544152504d4e4654000000006801000000000000c9e1e57a7d6b6d22000000000102030000000000000000000000000006000000000000000f00000000000000090000000000000003000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000700000000000000730000000000000002000000000000000000000000000000000000000000000007000000000000000000000000000000000000000000000000000000000000000800000000000000800000000000000001000000000000000700000000000000070000000000000008000000000000000000000000000000000000000000000000000000000000000a00000000000000a200000000000000030000000000000008000000000000000f000000000000000a00000000000000000000000000000000000000000000000000000000000000"},
		{"catalog-recleaned", EncodeCatalog(Catalog{UUID: uuid, CheckpointCutoffSequence: 6, PrunedThroughSequence: 15, ActiveID: 9, Entries: []CatalogEntry{{ID: 7, DataFileDigest: [32]byte{7}, Bytes: 115, Kind: SnapshotKind, FirstSequence: 0, LastSequence: 0}, {ID: 8, DataFileDigest: [32]byte{8}, Bytes: 128, Kind: RawLogKind, FirstSequence: 7, LastSequence: 7}, {ID: 11, DataFileDigest: [32]byte{11}, Bytes: 113, Kind: CleanedLogKind, FirstSequence: 8, LastSequence: 15}}}), "544152504d4e465400000000680100000000000012dd2f1b7254ac0a000000000102030000000000000000000000000006000000000000000f00000000000000090000000000000003000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000700000000000000730000000000000002000000000000000000000000000000000000000000000007000000000000000000000000000000000000000000000000000000000000000800000000000000800000000000000001000000000000000700000000000000070000000000000008000000000000000000000000000000000000000000000000000000000000000b000000000000007100000000000000030000000000000008000000000000000f000000000000000b00000000000000000000000000000000000000000000000000000000000000"},
		{"catalog-empty", EncodeCatalog(Catalog{UUID: uuid, CheckpointCutoffSequence: 6, PrunedThroughSequence: 15, ActiveID: 9, Entries: []CatalogEntry{{ID: 7, DataFileDigest: [32]byte{7}, Bytes: 115, Kind: SnapshotKind, FirstSequence: 0, LastSequence: 0}, {ID: 8, DataFileDigest: [32]byte{8}, Bytes: 128, Kind: RawLogKind, FirstSequence: 7, LastSequence: 7}, {ID: 0, Bytes: 0, Kind: CleanedLogKind, FirstSequence: 8, LastSequence: 15}}}), "544152504d4e46540000000068010000000000002be2ebcbadc5f758000000000102030000000000000000000000000006000000000000000f000000000000000900000000000000030000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000007000000000000007300000000000000020000000000000000000000000000000000000000000000070000000000000000000000000000000000000000000000000000000000000008000000000000008000000000000000010000000000000007000000000000000700000000000000080000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000030000000000000008000000000000000f000000000000000000000000000000000000000000000000000000000000000000000000000000"},
		{"backup", EncodeBackup(Backup{UUID: uuid, LastUserSequence: 6, CheckpointCutoffSequence: 3, SourceValueCacheBudgetBytes: 512, SourceMaxRecordBytes: 128, Files: []BackupFile{{Name: "CURRENT", Bytes: 128}}}), "54415250424b55500000000069000000000000000bead0e50f04b3fa000000000102030000000000000000000000000006000000000000000300000000000000000200000000000080000000000000000100000000000000070043555252454e5480000000000000000000000000000000000000000000000000000000000000000000000000000000"},
	} {
		expected, err := hex.DecodeString(tc.hex)
		if err != nil || !bytes.Equal(tc.got, expected) {
			t.Fatalf("%s changed wire bytes: %x; %v", tc.name, tc.got, err)
		}
	}
}

func TestMetadataOnlyCatalogEntryRetainsSequenceCoverage(t *testing.T) {
	catalog := Catalog{ActiveID: 3, PrunedThroughSequence: 4, Entries: []CatalogEntry{
		{ID: 2, DataFileDigest: [32]byte{2}, Kind: RawLogKind, Bytes: SegmentHeaderBytes + RecordHeaderBytes + 1, FirstSequence: 1, LastSequence: 1},
		{Kind: CleanedLogKind, FirstSequence: 2, LastSequence: 4},
	}}
	got, err := DecodeCatalog(EncodeCatalog(catalog))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Entries[0].HasPhysicalFile() || got.Entries[1].HasPhysicalFile() || got.SealedThroughSequence() != 4 {
		t.Fatalf("unexpected physical files or sequence coverage in %+v", got)
	}
}

func TestDecodeBackupRejectsPreviousSignature(t *testing.T) {
	// The former payload had a 64-byte prefix, including an extra slot before the file count.
	wire := envelope("TARPBACK", make([]byte, 64))
	if _, err := DecodeBackup(wire); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("previous backup signature: %v", err)
	}
}

func TestWireImages(t *testing.T) {
	uuid := [16]byte{1, 2, 3}
	segment := SegmentHeader{UUID: uuid, ID: 7, Kind: RawLogKind}
	if got, err := DecodeSegmentHeader(EncodeSegmentHeader(segment)); err != nil || got != segment {
		t.Fatalf("segment: %#v %v", got, err)
	}
	frontier := Frontier{UUID: uuid, PublicationGeneration: 9, ManifestID: 8, ActiveID: 7, ActiveCommittedEndOffset: 64, LastUserSequence: 6}
	if got, err := DecodeFrontier(EncodeFrontier(frontier)); err != nil || got != frontier {
		t.Fatalf("frontier: %#v %v", got, err)
	}
	catalog := Catalog{UUID: uuid, ActiveID: 7, CheckpointCutoffSequence: 3, PrunedThroughSequence: 5, Entries: []CatalogEntry{{ID: 1, DataFileDigest: [32]byte{1}, Bytes: 113, Kind: SnapshotKind}, {ID: 4, DataFileDigest: [32]byte{4}, Bytes: 113, Kind: RawLogKind, FirstSequence: 4, LastSequence: 4}, {Kind: CleanedLogKind, FirstSequence: 5, LastSequence: 5}}}
	if got, err := DecodeCatalog(EncodeCatalog(catalog)); err != nil || !reflect.DeepEqual(got, catalog) {
		t.Fatalf("catalog: %#v %v", got, err)
	}
	backup := Backup{UUID: uuid, LastUserSequence: 6, CheckpointCutoffSequence: 3, SourceValueCacheBudgetBytes: 512, Files: []BackupFile{{Name: "CURRENT", Bytes: 128}}}
	if got, err := DecodeBackup(EncodeBackup(backup)); err != nil || !reflect.DeepEqual(got, backup) {
		t.Fatalf("backup: %#v %v", got, err)
	}
	for _, wire := range []struct {
		b      []byte
		decode func([]byte) error
	}{
		{EncodeSegmentHeader(segment), func(b []byte) error { _, e := DecodeSegmentHeader(b); return e }},
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

func TestDecodeRecordHeaderValidatesFieldsWithAnIntactChecksum(t *testing.T) {
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
			if _, err := DecodeRecordHeader(header, tc.available); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("DecodeRecordHeader returned %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestDecodeRecordHeaderAcceptsLargestRepresentableRecord(t *testing.T) {
	header := EncodeRecord(Put, math.MaxUint64, []byte("k"), nil)[:RecordHeaderBytes]
	order.PutUint64(header[32:], math.MaxInt64-RecordHeaderBytes-1)
	clear(header[12:16])
	order.PutUint32(header[12:], crc32.Checksum(header, table))
	record, err := DecodeRecordHeader(header, math.MaxInt64)
	if err != nil || record.EncodedRecordBytes() != math.MaxInt64 || record.Sequence != math.MaxUint64 {
		t.Fatalf("maximum legal size and sequence: %+v, %v", record, err)
	}
}

func TestScanCommittedRanges(t *testing.T) {
	for _, value := range [][]byte{nil, {0, 1, 0xff}} {
		record := EncodeRecord(Put, 1, []byte{0, 0xff}, value)
		all := append(EncodeSegmentHeader(SegmentHeader{ID: 1, Kind: RawLogKind}), record...)
		found := 0
		if err := Scan(t.Context(), bytes.NewReader(all), int64(len(all)), func(off int64, r RecordHeader, k string) error {
			got, err := ReadValue(bytes.NewReader(all), off, r.EncodedRecordBytes(), k, r.Sequence)
			if err != nil || !bytes.Equal(got, value) {
				t.Fatalf("read: %x %v", got, err)
			}
			found++
			return nil
		}); err != nil || found != 1 {
			t.Fatalf("scan: %d %v", found, err)
		}
		for end := 65; end < len(all); end++ {
			if err := Scan(t.Context(), bytes.NewReader(all), int64(end), func(int64, RecordHeader, string) error { return nil }); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("truncation %d: %v", end, err)
			}
		}
		all[len(all)-1] ^= 1
		if err := Scan(t.Context(), bytes.NewReader(all), int64(len(all)), func(int64, RecordHeader, string) error { return nil }); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("payload corruption: %v", err)
		}
	}
}

type readerAtFunc func([]byte, int64) (int, error)

func (f readerAtFunc) ReadAt(b []byte, off int64) (int, error) { return f(b, off) }

func TestScanStopsDuringLargeRecord(t *testing.T) {
	data := EncodeSegmentHeader(SegmentHeader{ID: 1, Kind: RawLogKind})
	data = append(data, EncodeRecord(Put, 1, []byte("key"), make([]byte, streamChunkBytes*8))...)
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
	err := Scan(ctx, input, int64(len(data)), func(int64, RecordHeader, string) error {
		t.Fatal("visited a partially checked record")
		return nil
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("interrupted scan returned %v", err)
	}
}

func TestScanBatchesOnlyCommittedBytes(t *testing.T) {
	data := EncodeSegmentHeader(SegmentHeader{ID: 1, Kind: RawLogKind})
	const smallRecords = 400
	for i := range smallRecords {
		data = append(data, EncodeRecord(Put, uint64(i+1), []byte("k"), []byte("v"))...)
	}
	largeKey := bytes.Repeat([]byte{0xff}, streamChunkBytes+1)
	largeValue := bytes.Repeat([]byte{0xee}, 2*streamChunkBytes+1)
	data = append(data, EncodeRecord(Put, smallRecords+1, largeKey, largeValue)...)
	end := int64(len(data))
	data = append(data, 0xff) // An active log can have an uncommitted tail.
	source := bytes.NewReader(data)
	pos, calls := int64(SegmentHeaderBytes), 0
	input := readerAtFunc(func(b []byte, off int64) (int, error) {
		if off != pos || off+int64(len(b)) > end || len(b) > streamChunkBytes {
			t.Fatalf("out-of-range or repeated read at %d, length %d", off, len(b))
		}
		calls++
		pos += int64(len(b))
		return source.ReadAt(b, off)
	})
	seen, next := 0, int64(SegmentHeaderBytes)
	err := Scan(t.Context(), input, end, func(off int64, r RecordHeader, key string) error {
		seen++
		if off != next || r.Sequence != uint64(seen) {
			t.Fatalf("unexpected record %d at %d", r.Sequence, off)
		}
		next += r.EncodedRecordBytes()
		if seen <= smallRecords && key != "k" || seen > smallRecords && key != string(largeKey) {
			t.Fatal("key mismatch crossing the read buffer")
		}
		return nil
	})
	if err != nil || next != end || pos != end || seen != smallRecords+1 || calls > 8 {
		t.Fatalf("scan saw %d records in %d reads ending at %d/%d, want %d: %v", seen, calls, pos, next, end, err)
	}
	data[end-1] ^= 1
	err = Scan(t.Context(), bytes.NewReader(data), end, func(int64, RecordHeader, string) error { return nil })
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
			err := Scan(t.Context(), src, end, func(int64, RecordHeader, string) error {
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
	data, sealed, _ := parityFixture(100)
	parity := parityFor(f, data, sealed)
	for _, b := range [][]byte{
		parity,
		EncodeSegmentHeader(SegmentHeader{ID: 1, Kind: CleanedLogKind}),
		EncodeCatalog(Catalog{ActiveID: 1}),
		EncodeCatalog(Catalog{ActiveID: 1, Index: IndexCheckpoint{ID: 2, Bytes: 1024, IndexFileDigest: [32]byte{1}, CoveredLayoutFingerprint: [32]byte{2}}}),
		EncodeCatalog(Catalog{ActiveID: 1, PrunedThroughSequence: 5, Entries: []CatalogEntry{
			{ID: 2, DataFileDigest: [32]byte{2}, Bytes: 128, Kind: RawLogKind, FirstSequence: 1, LastSequence: 1},
			{Kind: CleanedLogKind, FirstSequence: 2, LastSequence: 2},
			{ID: 3, DataFileDigest: [32]byte{3}, Bytes: 128, Kind: CleanedLogKind, FirstSequence: 3, LastSequence: 5},
		}}),
		EncodeRecord(Put, 1, []byte("k"), nil),
		EncodeFrontier(Frontier{PublicationGeneration: 1, ManifestID: 2, ActiveID: 1, ActiveCommittedEndOffset: SegmentHeaderBytes}),
		EncodeBackup(Backup{}),
	} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = DecodeSegmentHeader(b)
		_, _ = DecodeFrontier(b)
		_, _ = DecodeCatalog(b)
		_, _ = DecodeBackup(b)
		_, _ = DecodeIndexedValue(b, []byte("k"), 1)
		_ = ValidateParitySidecar(t.Context(), bytes.NewReader(b), parityTestUUID, sealed)
		if len(b) >= 8 && string(b[:8]) == "TARPPAR2" {
			_ = RecoverDataFromParity(t.Context(), bytes.NewReader(nil), bytes.NewReader(b), io.Discard, parityTestUUID, sealed)
		}
		if len(b) >= RecordHeaderBytes {
			_, _ = DecodeRecordHeader(b[:RecordHeaderBytes], int64(len(b)))
		}
	})
}

func TestCatalogRejectsAmbiguousCoverage(t *testing.T) {
	const size = SegmentHeaderBytes + RecordHeaderBytes + 1
	raw := CatalogEntry{ID: 2, DataFileDigest: [32]byte{2}, Bytes: size, Kind: RawLogKind, FirstSequence: 4, LastSequence: 4}
	snapshot := CatalogEntry{ID: 3, DataFileDigest: [32]byte{3}, Bytes: size, Kind: SnapshotKind}
	valid := Catalog{CheckpointCutoffSequence: 3, PrunedThroughSequence: 6, ActiveID: 7}
	for _, tc := range []struct {
		name  string
		files []CatalogEntry
	}{
		{"start gap", []CatalogEntry{{ID: 2, DataFileDigest: [32]byte{2}, Bytes: size, Kind: RawLogKind, FirstSequence: 5, LastSequence: 5}}},
		{"start overlap", []CatalogEntry{{ID: 2, DataFileDigest: [32]byte{2}, Bytes: size, Kind: RawLogKind, FirstSequence: 3, LastSequence: 4}}},
		{"duplicate ID", []CatalogEntry{raw, {ID: 2, DataFileDigest: [32]byte{2}, Bytes: size, Kind: CleanedLogKind, FirstSequence: 5, LastSequence: 6}}},
		{"overlapping intervals", []CatalogEntry{raw, {Kind: CleanedLogKind, FirstSequence: 4, LastSequence: 6}}},
		{"reversed interval", []CatalogEntry{raw, {Kind: CleanedLogKind, FirstSequence: 5, LastSequence: 4}}},
		{"empty raw interval", []CatalogEntry{{Kind: RawLogKind, FirstSequence: 4, LastSequence: 4}}},
		{"missing physical length", []CatalogEntry{{ID: 2, Kind: CleanedLogKind, FirstSequence: 4, LastSequence: 4}}},
		{"empty interval has bytes", []CatalogEntry{{Bytes: size, Kind: CleanedLogKind, FirstSequence: 4, LastSequence: 4}}},
		{"past prune boundary", []CatalogEntry{{Kind: CleanedLogKind, FirstSequence: 4, LastSequence: 7}}},
		{"snapshot after log", []CatalogEntry{raw, snapshot}},
		{"snapshot with interval", []CatalogEntry{{ID: 3, DataFileDigest: [32]byte{3}, Bytes: size, Kind: SnapshotKind, FirstSequence: 1, LastSequence: 2}}},
		{"active ID reused", []CatalogEntry{{ID: 7, DataFileDigest: [32]byte{7}, Bytes: size, Kind: SnapshotKind}}},
		{"raw records exceed physical length", []CatalogEntry{{ID: 2, DataFileDigest: [32]byte{2}, Bytes: size, Kind: RawLogKind, FirstSequence: 4, LastSequence: 5}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid
			c.Entries = tc.files
			if _, err := DecodeCatalog(EncodeCatalog(c)); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted invalid coverage: %v", err)
			}
		})
	}
	for _, c := range []Catalog{
		{ActiveID: 7, CheckpointCutoffSequence: 2, PrunedThroughSequence: 1},
		{ActiveID: 7, CheckpointCutoffSequence: 0, Entries: []CatalogEntry{snapshot}},
		{ActiveID: 7, CheckpointCutoffSequence: math.MaxUint64, PrunedThroughSequence: math.MaxUint64, Entries: []CatalogEntry{{Kind: CleanedLogKind, FirstSequence: 0, LastSequence: 0}}},
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
		func(b []byte) error { _, err := DecodeSegmentHeader(b[:SegmentHeaderBytes]); return err },
		func(b []byte) error { _, err := DecodeCatalog(b); return err },
		func(b []byte) error {
			_, err := DecodeRecordHeader(b[:RecordHeaderBytes], RecordHeaderBytes)
			return err
		},
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

func TestReadValueAcceptsCompleteFinalReadWithEOF(t *testing.T) {
	for _, size := range []int{10, indexedReadBufferBytes + 1} {
		value := bytes.Repeat([]byte{0x77}, size)
		record := EncodeRecord(Put, 7, []byte("k"), value)
		r := readerAtFunc(func(b []byte, off int64) (int, error) {
			n, err := bytes.NewReader(record).ReadAt(b, off)
			if off+int64(n) == int64(len(record)) {
				return n, io.EOF
			}
			return n, err
		})
		got, err := ReadValue(r, 0, int64(len(record)), "k", 7)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("size %d: err %v, value length %d", size, err, len(got))
		}
	}
}

func TestCoveredLayoutFingerprintCoversSnapshotsIntervalsAndSelectedBytes(t *testing.T) {
	c := Catalog{UUID: [16]byte{1}, ActiveID: 4, CheckpointCutoffSequence: 2, PrunedThroughSequence: 4, Entries: []CatalogEntry{
		{ID: 1, DataFileDigest: [32]byte{1}, Bytes: 120, Kind: SnapshotKind},
		{ID: 2, DataFileDigest: [32]byte{2}, Bytes: 120, Kind: SnapshotKind},
		{Kind: CleanedLogKind, FirstSequence: 3, LastSequence: 4},
		{ID: 3, DataFileDigest: [32]byte{3}, Bytes: 120, Kind: RawLogKind, FirstSequence: 5, LastSequence: 5},
	}}
	layout, err := c.CoveredLayoutFingerprint(3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", layout) != "9263094565f4a77808ee498e376f2c93796fd07fbddf482990da6b935fcf7e1c" {
		t.Fatalf("layout wire vector %x", layout)
	}
	c.Index = IndexCheckpoint{ID: 5, Bytes: 12000, IndexFileDigest: [32]byte{0xaa}, CoveredSequence: 4, CoveredCatalogEntries: 3, CoveredLayoutFingerprint: layout}
	got, err := DecodeCatalog(EncodeCatalog(c))
	if err != nil || !reflect.DeepEqual(c, got) {
		t.Fatalf("checkpoint descriptor round-trip: %+v, %v", got, err)
	}
	c.Entries[3].DataFileDigest[0]++
	if actual, err := c.CoveredLayoutFingerprint(3, 4); err != nil || actual != layout {
		t.Fatalf("uncovered tail changed binding: %x %v", actual, err)
	}
	c.Entries[1].DataFileDigest[0]++
	if actual, err := c.CoveredLayoutFingerprint(3, 4); err != nil || actual == layout {
		t.Fatalf("covered digest change: %x %v", actual, err)
	}
	c.PrunedThroughSequence = 2
	for _, test := range []struct{ entries, seq uint64 }{{0, 2}, {1, 2}, {3, 5}, {math.MaxUint64, 4}} {
		if _, err := c.CoveredLayoutFingerprint(test.entries, test.seq); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted invalid cut %+v: %v", test, err)
		}
	}
	if _, err := c.CoveredLayoutFingerprint(2, 2); err != nil {
		t.Fatal("complete snapshot cut", err)
	}
	for _, id := range []uint64{0, 1, 4} {
		c.Index.ID = id
		if _, err := DecodeCatalog(EncodeCatalog(c)); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("checkpoint identity %d: %v", id, err)
		}
	}
}
