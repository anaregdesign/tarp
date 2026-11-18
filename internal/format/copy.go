package format

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"strings"
)

type Identity struct {
	Key      string
	Sequence uint64
}

const streamChunk = 32 << 10

// SelectRecord validates one complete record. After its key is available, choose
// selects a destination or returns nil to discard the bytes. The caller publishes
// staged output only after validation succeeds. Even a discarded value is checked.
func SelectRecord(ctx context.Context, src io.ReaderAt, offset, available int64, buffer []byte, choose func(Record, string) (io.Writer, error)) (Record, error) {
	if len(buffer) == 0 {
		return Record{}, fmt.Errorf("format: empty copy buffer")
	}
	buffer = buffer[:min(len(buffer), streamChunk)]
	var header [RecordHeaderBytes]byte
	if err := readChunk(ctx, src, header[:], offset); err != nil {
		return Record{}, err
	}
	r, err := DecodeRecord(header[:], available)
	if err != nil {
		return r, err
	}
	if offset < 0 || offset > math.MaxInt64-r.Bytes() || uint64(r.KeyBytes) > uint64(int(^uint(0)>>1)) {
		return r, bad("record bounds")
	}

	var key strings.Builder
	key.Grow(int(min(int64(len(buffer)), r.KeyBytes)))
	sum := uint32(0)
	offset += RecordHeaderBytes
	for left := r.KeyBytes; left > 0; {
		chunk := buffer[:min(int64(len(buffer)), left)]
		if err := readChunk(ctx, src, chunk, offset); err != nil {
			return r, err
		}
		key.Write(chunk)
		sum = crc32.Update(sum, table, chunk)
		offset += int64(len(chunk))
		left -= int64(len(chunk))
	}
	dst, err := choose(r, key.String())
	if err != nil {
		return r, err
	}
	if dst != nil {
		if err := writeChunk(ctx, dst, header[:]); err != nil {
			return r, err
		}
		for k := key.String(); len(k) != 0; {
			n := copy(buffer, k)
			if err := writeChunk(ctx, dst, buffer[:n]); err != nil {
				return r, err
			}
			k = k[n:]
		}
	}
	for left := r.ValueBytes; left > 0; {
		chunk := buffer[:min(int64(len(buffer)), left)]
		if err := readChunk(ctx, src, chunk, offset); err != nil {
			return r, err
		}
		sum = crc32.Update(sum, table, chunk)
		if dst != nil {
			if err := writeChunk(ctx, dst, chunk); err != nil {
				return r, err
			}
		}
		offset += int64(len(chunk))
		left -= int64(len(chunk))
	}
	if sum != r.PayloadCRC {
		return r, bad("record payload checksum")
	}
	return r, nil
}

func readChunk(ctx context.Context, src io.ReaderAt, chunk []byte, offset int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := src.ReadAt(chunk, offset)
	if n != len(chunk) && err == nil {
		return io.ErrUnexpectedEOF
	}
	return err
}

func writeChunk(ctx context.Context, dst io.Writer, chunk []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := dst.Write(chunk)
	if n != len(chunk) && err == nil {
		return io.ErrShortWrite
	}
	return err
}

// CopyRecord validates the bytes it copies. An expected identity selects a Put.
func CopyRecord(ctx context.Context, src io.ReaderAt, dst io.Writer, offset, available int64, expected *Identity, buffer []byte) (Record, error) {
	return SelectRecord(ctx, src, offset, available, buffer, func(r Record, key string) (io.Writer, error) {
		if expected != nil && (r.Kind != Put || r.Sequence != expected.Sequence || key != expected.Key) {
			return nil, bad("copied record identity")
		}
		return dst, nil
	})
}

// CopyRecordKey exposes the key from the same bytes validated and copied for backup.
func CopyRecordKey(ctx context.Context, src io.ReaderAt, dst io.Writer, offset, available int64, buffer []byte) (Record, string, error) {
	var key string
	r, err := SelectRecord(ctx, src, offset, available, buffer, func(_ Record, k string) (io.Writer, error) {
		key = k
		return dst, nil
	})
	return r, key, err
}
