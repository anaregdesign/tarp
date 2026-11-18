package format

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
)

func TestVersionOneGoldenVectors(t *testing.T) {
	uuid := [16]byte{1, 2, 3}
	for _, tc := range []struct {
		name string
		got  []byte
		hex  string
	}{
		{"segment", EncodeSegment(Segment{UUID: uuid, ID: 7, Kind: Log}), "5441525053454731010040000100000001020300000000000000000000000000070000000000000000000000000000000000000000000000000000004124449f"},
		{"record", EncodeRecord(Put, 1, []byte{0, 255}, []byte{1, 2, 3}), "544152505245433101000100148562d201000000000000000200000000000000030000000000000013cf25fc0000000000ff010203"},
		{"frontier", EncodeFrontier(Frontier{UUID: uuid, Generation: 9, ManifestID: 8, ActiveID: 7, ActiveEnd: 64, Sequence: 6}), "5441525043555231010080000000000001020300000000000000000000000000090000000000000008000000000000000000000000000000000000000000000000000000000000000000000000000000070000000000000040000000000000000600000000000000000000000000000000000000000000000000000060cb292e"},
		{"catalog", EncodeCatalog(Catalog{UUID: uuid, Cutoff: 6, ActiveID: 9, Files: []File{{ID: 7, Bytes: 100, Kind: Snapshot}, {ID: 8, Bytes: 128, Kind: Log}}}), "54415250434154310100000058000000000000009e8aa6d5d5008e0b0000000001020300000000000000000000000000060000000000000009000000000000000200000000000000070000000000000064000000000000000200000000000000080000000000000080000000000000000100000000000000"},
		{"backup", EncodeBackup(Backup{UUID: uuid, Sequence: 6, Cutoff: 3, SourceCacheBytes: 512, SourceRecordBytes: 128, SourceFileBytes: 256, Files: []BackupFile{{Name: "CURRENT", Bytes: 128}}}), "5441525042414b3101000000710000000000000085798e61cf0cde190000000001020300000000000000000000000000060000000000000003000000000000000002000000000000800000000000000000010000000000000100000000000000070043555252454e5480000000000000000000000000000000000000000000000000000000000000000000000000000000"},
	} {
		expected, err := hex.DecodeString(tc.hex)
		if err != nil || !bytes.Equal(tc.got, expected) {
			t.Fatalf("%s changed version-one wire bytes: %x; %v", tc.name, tc.got, err)
		}
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
	catalog := Catalog{UUID: uuid, ActiveID: 7, Cutoff: 3, Files: []File{{ID: 1, Bytes: 64, Kind: Snapshot}, {ID: 4, Bytes: 64, Kind: Log}}}
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
	for _, b := range [][]byte{EncodeSegment(Segment{ID: 1, Kind: Log}), EncodeCatalog(Catalog{ActiveID: 1}), EncodeRecord(Put, 1, []byte("k"), nil), EncodeFrontier(Frontier{Generation: 1, ManifestID: 1, ActiveID: 1, ActiveEnd: 64}), EncodeBackup(Backup{})} {
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
