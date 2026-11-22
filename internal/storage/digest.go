package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"

	"github.com/anaregdesign/tarp/internal/format"
)

// digestReader hashes precisely the bytes delivered to the sequential decoder.
// Its ReaderAt facade lets SelectRecord and Scan share the same checked stream.
type digestReader struct {
	io.ReaderAt
	hash hash.Hash
	next int64
}

func (r *digestReader) ReadAt(b []byte, off int64) (int, error) {
	if off != r.next {
		return 0, fmt.Errorf("storage: nonsequential DATA digest read at %d, expected %d", off, r.next)
	}
	n, err := r.ReaderAt.ReadAt(b, off)
	r.hash.Write(b[:n])
	r.next += int64(n)
	return n, err
}

func checkSealedDigest(h hash.Hash, entry format.CatalogEntry) error {
	if [sha256.Size]byte(h.Sum(nil)) != entry.DataFileDigest {
		return &SourceError{Err: fmt.Errorf("%w: sealed DATA %d digest", ErrCorrupt, entry.ID)}
	}
	return nil
}

// verifySelectedDigest checks a sealed file's exact committed bytes without
// decoding records when a verified checkpoint already describes their history.
func verifySelectedDigest(ctx context.Context, rd io.ReaderAt, entry format.CatalogEntry) error {
	h := sha256.New()
	var buffer [32 << 10]byte
	for off := int64(0); off < entry.Bytes; {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk := buffer[:min(int64(len(buffer)), entry.Bytes-off)]
		n, err := rd.ReadAt(chunk, off)
		if err != nil && err != io.EOF {
			return err
		}
		if n != len(chunk) {
			return fmt.Errorf("%w: short sealed DATA read: %w", ErrCorrupt, io.ErrUnexpectedEOF)
		}
		h.Write(chunk)
		off += int64(n)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return checkSealedDigest(h, entry)
}
