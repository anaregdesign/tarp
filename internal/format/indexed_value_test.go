package format

import (
	"bytes"
	"errors"
	"testing"
)

func TestDecodeIndexedValueChecksEachRecordAndOwnsItsBytes(t *testing.T) {
	key := []byte("one")
	wire := EncodeRecord(Put, 7, key, []byte("abc"))
	value, err := DecodeIndexedValue(wire, key, 7)
	if err != nil || !bytes.Equal(value, []byte("abc")) {
		t.Fatal(value, err)
	}
	empty, err := DecodeIndexedValue(EncodeRecord(Put, 8, key, nil), key, 8)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatal(empty, err)
	}
	for _, tc := range []struct {
		record []byte
		key    []byte
		seq    uint64
	}{
		{wire, []byte("two"), 7}, {wire, key, 8},
		{wire[:RecordHeaderBytes-1], key, 7}, {wire[:len(wire)-1], key, 7},
		{append(bytes.Clone(wire), 0), key, 7}, {EncodeRecord(Delete, 7, key, nil), key, 7},
	} {
		if _, err := DecodeIndexedValue(tc.record, tc.key, tc.seq); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("accepted mismatched indexed record: %v", err)
		}
	}
	wire[len(wire)-1] ^= 1
	if _, err := DecodeIndexedValue(wire, key, 7); !errors.Is(err, ErrCorrupt) || string(value) != "abc" {
		t.Fatalf("checksum err %v, prior result %q", err, value)
	}
}
