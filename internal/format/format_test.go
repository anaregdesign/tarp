package format

import (
	"bytes"
	"encoding/hex"
	"errors"
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

func TestScanCommittedRanges(t *testing.T) {
	for _, value := range [][]byte{nil, {0, 1, 0xff}} {
		record := EncodeRecord(Put, 1, []byte{0, 0xff}, value)
		all := append(EncodeSegment(Segment{ID: 1, Kind: Log}), record...)
		found := 0
		if err := Scan(bytes.NewReader(all), int64(len(all)), func(off int64, r Record, k string) error {
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
			if err := Scan(bytes.NewReader(all), int64(end), func(int64, Record, string) error { return nil }); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("truncation %d: %v", end, err)
			}
		}
		all[len(all)-1] ^= 1
		if err := Scan(bytes.NewReader(all), int64(len(all)), func(int64, Record, string) error { return nil }); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("payload corruption: %v", err)
		}
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
