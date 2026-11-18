package format

import (
	"bytes"
	"errors"
	"io"
	"math"
	"testing"
)

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
