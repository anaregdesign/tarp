package format

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

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
	key := bytes.Repeat([]byte{0, 0xff}, streamChunkBytes+1)
	value := bytes.Repeat([]byte{1}, 3*streamChunkBytes)
	b := EncodeRecord(Put, 6, key, value)
	run := func(dst io.Writer) error {
		reader := &boundedReader{bytes.NewReader(b), streamChunkBytes}
		_, err := SelectRecord(context.Background(), reader, 0, int64(len(b)), make([]byte, 4*streamChunkBytes), func(r RecordHeader, k string) (io.Writer, error) {
			if k != string(key) || r.Sequence != 6 {
				t.Fatal("incorrect key or sequence")
			}
			return dst, nil
		})
		return err
	}
	w := &boundedWriter{max: streamChunkBytes}
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
