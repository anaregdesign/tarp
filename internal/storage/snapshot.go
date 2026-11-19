package storage

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"hash"
	"io"
	"os"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
)

type CapturedFile struct {
	File  *File
	Entry format.File
}

type Capture struct {
	Current  format.Frontier
	Catalog  format.Catalog
	Manifest []byte
	Files    []CapturedFile
}

// Capture pins exact committed prefixes, without copying the logical key index.
// The caller holds the writer gate; metadata is immutable after publication.
func (s *Store) Capture() (*Capture, error) {
	active, err := s.Catalog.ActiveFile(s.Current)
	if err != nil {
		return nil, err
	}
	c := &Capture{Current: s.Current, Catalog: s.Catalog, Manifest: bytes.Clone(s.Manifest)}
	c.Catalog.Files = append([]format.File(nil), s.Catalog.Files...)
	for _, entry := range s.Catalog.Files {
		if entry.HasPhysicalFile() {
			c.Files = append(c.Files, CapturedFile{File: s.Files[entry.ID], Entry: entry})
		}
	}
	c.Files = append(c.Files, CapturedFile{File: s.Files[active.ID], Entry: active})
	for _, captured := range c.Files {
		captured.File.Pin()
	}
	return c, nil
}

func (c *Capture) Release() {
	for _, entry := range c.Files {
		entry.File.Unpin()
	}
}

type SourceError struct{ Err error }

func (e *SourceError) Error() string { return "storage: source: " + e.Err.Error() }
func (e *SourceError) Unwrap() error { return e.Err }

type destinationError struct{ err error }

func (e *destinationError) Error() string { return e.err.Error() }
func (e *destinationError) Unwrap() error { return e.err }

type sourceReader struct{ io.ReaderAt }

func (r sourceReader) ReadAt(b []byte, off int64) (int, error) {
	n, err := r.ReaderAt.ReadAt(b, off)
	if err != nil {
		err = &SourceError{Err: err}
	}
	return n, err
}

type outputWriter struct {
	fs     *IO
	fd     *os.File
	offset int64
	point  string
	hash   hash.Hash
}

func (w *outputWriter) Write(b []byte) (int, error) {
	if err := w.fs.Write(w.fd, b, w.offset, w.point); err != nil {
		return 0, &destinationError{err}
	}
	w.offset += int64(len(b))
	if w.hash != nil {
		return w.hash.Write(b)
	}
	return len(b), nil
}

// Replay validates the entire captured image before scheduling the live records
// in physical read order. The same slice later carries the index relocations.
func (c *Capture) Replay(ctx context.Context, fs *IO) ([]Relocation, error) {
	index := make(map[string]Location)
	for _, captured := range c.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fd, _, _, err := openSelected(captured.File.Path, c.Current.UUID, captured.Entry, captured.File.ID == c.Current.ActiveID)
		if err != nil {
			return nil, &SourceError{Err: err}
		}
		err = fs.Do("compact.scan", func() error {
			span := format.NewRecordRange(captured.Entry, c.Catalog.Cutoff)
			err := format.Scan(ctx, sourceReader{fd}, captured.Entry.Bytes, func(offset int64, r format.Record, key string) error {
				if err := span.Accept(r); err != nil {
					return err
				}
				return applyRecord(index, captured.File, c.Catalog.PrunedThrough, offset, r, key)
			})
			if err != nil {
				return err
			}
			return span.Finish()
		})
		if err = errors.Join(err, fd.Close()); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	moves := make([]Relocation, 0, len(index))
	for key, loc := range index {
		moves = append(moves, Relocation{Key: key, Old: loc})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	slices.SortFunc(moves, func(a, b Relocation) int {
		if a.Old.File != b.Old.File {
			return cmp.Compare(a.Old.File.ID, b.Old.File.ID)
		}
		return cmp.Compare(a.Old.Offset, b.Old.Offset)
	})
	return moves, nil
}
