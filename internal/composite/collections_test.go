package composite

import (
	"bytes"
	"testing"
)

func TestFrameLengthBoundariesPreserveNextFrame(t *testing.T) {
	next := []byte{1, 42}
	for _, tc := range []struct {
		size   int
		prefix []byte
	}{
		{0, []byte{0}},
		{127, []byte{0x7f}},
		{128, []byte{0x80, 1}},
		{16383, []byte{0xff, 0x7f}},
		{16384, []byte{0x80, 0x80, 1}},
	} {
		payload := bytes.Repeat([]byte{0x75}, tc.size)
		wire := appendFrame(nil, payload)
		if !bytes.HasPrefix(wire, tc.prefix) || len(wire) != len(tc.prefix)+len(payload) {
			t.Fatalf("size %d encoded with the wrong length prefix", tc.size)
		}
		got, rest, err := takeFrame(append(wire, next...))
		if err != nil || !bytes.Equal(got, payload) || cap(got) != len(got) || !bytes.Equal(rest, next) {
			t.Fatalf("size %d: decoded length=%d cap=%d, rest=%x, err=%v", tc.size, len(got), cap(got), rest, err)
		}
	}
}

func TestTakeFrameRejectsInvalidLengthsWithoutReturningData(t *testing.T) {
	for _, data := range [][]byte{
		nil, {0x80}, {0x80, 0}, {0x81, 0, 42}, bytes.Repeat([]byte{0xff}, 11),
		{2, 42}, {0x80, 1, 42},
	} {
		if got, rest, err := takeFrame(data); got != nil || rest != nil || err == nil {
			t.Errorf("takeFrame(%x) = %x, %x, %v", data, got, rest, err)
		}
	}
}
