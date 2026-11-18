package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"

	"github.com/anaregdesign/tarp/internal/format"
)

type CapturedFile struct {
	File  *File
	Bytes int64
}

type Capture struct {
	Current  format.Frontier
	Catalog  format.Catalog
	Manifest []byte
	Files    []CapturedFile
}

// Capture pins exact committed prefixes, without copying the logical key index.
// The caller holds the writer gate; metadata is immutable after publication.
func (s *Store) Capture() *Capture {
	c := &Capture{Current: s.Current, Catalog: s.Catalog, Manifest: bytes.Clone(s.Manifest)}
	c.Catalog.Files = append([]format.File(nil), s.Catalog.Files...)
	for _, entry := range s.Catalog.Files {
		c.Files = append(c.Files, CapturedFile{File: s.Files[entry.ID], Bytes: entry.Bytes})
	}
	c.Files = append(c.Files, CapturedFile{File: s.Files[s.Current.ActiveID], Bytes: s.Current.ActiveEnd})
	for _, entry := range c.Files {
		entry.File.Pin()
	}
	return c
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

func (c *Capture) Replay(ctx context.Context, fs *IO) (map[string]Location, error) {
	index := make(map[string]Location)
	last := c.Catalog.Cutoff
	for _, captured := range c.Files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fd, err := OpenRegular(captured.File.Path)
		if err != nil {
			return nil, &SourceError{Err: err}
		}
		err = fs.Do("compact.scan", func() error {
			header := make([]byte, format.SegmentHeaderBytes)
			if _, err := fd.ReadAt(header, 0); err != nil {
				return &SourceError{Err: err}
			}
			s, err := format.DecodeSegment(header)
			if err != nil {
				return err
			}
			if s.UUID != c.Current.UUID || s.ID != captured.File.ID || s.Kind != captured.File.Kind {
				return fmt.Errorf("%w: captured segment identity", ErrCorrupt)
			}
			st, err := fd.Stat()
			if err != nil {
				return &SourceError{Err: err}
			}
			if st.Size() < captured.Bytes || captured.File.ID != c.Current.ActiveID && st.Size() != captured.Bytes {
				return fmt.Errorf("%w: captured segment length", ErrCorrupt)
			}
			return format.Scan(sourceReader{fd}, captured.Bytes, func(offset int64, r format.Record, key string) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				return applyRecord(index, captured.File, c.Catalog.Cutoff, &last, offset, r, key)
			})
		})
		if err = errors.Join(err, fd.Close()); err != nil {
			return nil, err
		}
	}
	if last != c.Current.Sequence {
		return nil, fmt.Errorf("%w: captured frontier sequence", ErrCorrupt)
	}
	return index, nil
}

func (s *Store) NewSnapshotFile() (*File, *os.File, error) {
	return s.newSegment(format.Snapshot, "compact.segment")
}

func (s *Store) CopyRecord(ctx context.Context, old Location, key string, dst *os.File, offset int64, buffer []byte) error {
	fd, err := OpenRegular(old.File.Path)
	if err != nil {
		return &SourceError{Err: err}
	}
	writer := &outputWriter{fs: s.IO, fd: dst, offset: offset, point: "compact.write"}
	_, err = format.CopyRecord(ctx, sourceReader{fd}, writer, old.Offset, old.Bytes,
		&format.Identity{Key: key, Sequence: old.Sequence}, buffer)
	var destination *destinationError
	if err != nil && !errors.As(err, &destination) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		err = &SourceError{Err: err}
	}
	return errors.Join(err, fd.Close())
}

func (s *Store) InstallCheckpoint(capture *Capture, outputs []format.File) error {
	if s.Current.Generation == math.MaxUint64 {
		return fault("compact.generation", "not-applied", false, errors.New("storage: publication generation exhausted"))
	}
	old := make(map[uint64]bool)
	for _, f := range capture.Catalog.Files {
		old[f.ID] = true
	}
	catalog := format.Catalog{UUID: s.Current.UUID, Cutoff: capture.Current.Sequence, ActiveID: s.Current.ActiveID, Files: append([]format.File(nil), outputs...)}
	for _, file := range s.Catalog.Files {
		if !old[file.ID] {
			catalog.Files = append(catalog.Files, file)
		}
	}
	candidate := s.Current
	candidate.Generation++
	return s.publishCatalog(catalog, candidate)
}

// DiscardOutputs removes only this compaction's unpublished, closed outputs.
func (s *Store) DiscardOutputs(outputs []*File) error {
	var result error
	for _, f := range outputs {
		err := s.IO.Do("compact.discard", func() error { return os.Remove(f.Path) })
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		result = errors.Join(result, err)
		if err == nil {
			delete(s.Files, f.ID)
		}
	}
	if len(outputs) != 0 {
		result = errors.Join(result, s.IO.SyncDir(s.Dir, "compact.discard.dirsync"))
	}
	return result
}
