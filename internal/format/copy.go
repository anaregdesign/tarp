package format

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
)

type Identity struct {
	Key      string
	Sequence uint64
}

// CopyRecord validates the bytes it actually copies, using bounded buffers.
// A nil identity permits either record kind, as required for backup log copies.
func CopyRecord(ctx context.Context, src io.ReaderAt, dst io.Writer, offset, available int64, expected *Identity, buffer []byte) (Record, error) {
	var header [RecordHeaderBytes]byte
	if _, err := src.ReadAt(header[:], offset); err != nil {
		return Record{}, err
	}
	r, err := DecodeRecord(header[:], available)
	if err != nil {
		return r, err
	}
	if expected != nil && (r.Kind != Put || r.Sequence != expected.Sequence || r.KeyBytes != int64(len(expected.Key))) {
		return r, bad("copied record identity")
	}
	if n, err := dst.Write(header[:]); err != nil {
		return r, err
	} else if n != len(header) {
		return r, io.ErrShortWrite
	}
	if len(buffer) == 0 {
		return r, fmt.Errorf("format: empty copy buffer")
	}
	sum := uint32(0)
	for pos := int64(0); pos < r.KeyBytes+r.ValueBytes; {
		if err := ctx.Err(); err != nil {
			return r, err
		}
		n := min(int64(len(buffer)), r.KeyBytes+r.ValueBytes-pos)
		chunk := buffer[:int(n)]
		if _, err := src.ReadAt(chunk, offset+RecordHeaderBytes+pos); err != nil {
			return r, err
		}
		if expected != nil && pos < r.KeyBytes {
			k := min(n, r.KeyBytes-pos)
			if !bytes.Equal(chunk[:k], []byte(expected.Key[pos:pos+k])) {
				return r, bad("copied key")
			}
		}
		sum = crc32.Update(sum, table, chunk)
		if n, err := dst.Write(chunk); err != nil {
			return r, err
		} else if n != len(chunk) {
			return r, io.ErrShortWrite
		}
		pos += int64(len(chunk))
	}
	if sum != r.PayloadCRC {
		return r, bad("copied record payload checksum")
	}
	return r, nil
}
