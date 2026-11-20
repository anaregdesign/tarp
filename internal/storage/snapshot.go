package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
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

// replayLive validates the captured record history and visits the live Puts in
// source file/offset order. The index view was captured with this layout under
// the writer gate. The private builder independently validates key semantics,
// including historical records that are absent from the final view.
func (c *Capture) replayLive(ctx context.Context, fs *IO, history index.Backend, live index.View, visit func(*os.File, string, Location) error) error {
	replay := newReplayBuilder(ctx, history, c.Catalog.PrunedThrough)
	defer replay.abort()
	for _, captured := range c.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		fd, _, _, err := openSelected(captured.File.Path, c.Current.UUID, captured.Entry, captured.File.ID == c.Current.ActiveID)
		if err != nil {
			return &SourceError{Err: err}
		}
		copying := false
		err = fs.Do("compact.scan", func() error {
			return scanSelectedRecords(ctx, sourceReader{fd}, captured.Entry, c.Catalog.Cutoff, func(off int64, r format.Record, key string) error {
				if err := replay.apply(captured.File, off, r, key); err != nil {
					return err
				}
				if r.Kind != format.Put {
					return nil
				}
				loc, exists, err := live.Lookup(key)
				if err != nil {
					return &SourceError{Err: err}
				}
				if !exists || loc.FileID != captured.File.ID || loc.Offset != off {
					return nil
				}
				if loc.Bytes != r.Bytes() || loc.Sequence != r.Sequence || loc.ValueBytes != r.ValueBytes {
					return fmt.Errorf("%w: index disagrees with captured DATA", ErrCorrupt)
				}
				if !copying {
					// The same source descriptor now serves physical record copies.
					if err := fs.Check("compact.source.open:after"); err != nil {
						return &SourceError{Err: err}
					}
					copying = true
				}
				return visit(fd, key, Location{File: captured.File, Offset: off, Bytes: r.Bytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence})
			})
		})
		if closeErr := fd.Close(); closeErr != nil {
			err = errors.Join(err, &SourceError{Err: closeErr})
		}
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(captured.File.Path), err)
		}
	}
	if err := replay.finish(); err != nil {
		return err
	}
	return ctx.Err()
}
