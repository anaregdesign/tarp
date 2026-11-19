package storage

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
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
		if entry.ID != 0 {
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
			err := format.Scan(sourceReader{fd}, captured.Entry.Bytes, func(offset int64, r format.Record, key string) error {
				if err := ctx.Err(); err != nil {
					return err
				}
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

func (s *Store) NewSnapshotFile() (*File, *os.File, error) {
	return s.newSegment(format.Snapshot, "compact.segment", appendData{})
}

// SnapshotReader reuses a single source descriptor while copying a pinned
// checkpoint in physical read order. Close it before publishing the result.
type SnapshotReader struct {
	source *File
	fd     *os.File
}

func (r *SnapshotReader) Close() error {
	if r.fd == nil {
		return nil
	}
	err := r.fd.Close()
	r.source, r.fd = nil, nil
	if err != nil {
		return &SourceError{err}
	}
	return nil
}

func (r *SnapshotReader) CopyRecord(ctx context.Context, fs *IO, old Location, key string, dst *os.File, offset int64, buffer []byte) error {
	if r.source != old.File {
		if err := r.Close(); err != nil {
			return err
		}
		fd, err := OpenRegular(old.File.Path)
		if err != nil {
			return &SourceError{err}
		}
		r.source, r.fd = old.File, fd
		if err := fs.Check("compact.source.open:after"); err != nil {
			return &SourceError{err}
		}
	}
	writer := &outputWriter{fs: fs, fd: dst, offset: offset, point: "compact.write"}
	record, err := format.CopyRecord(ctx, sourceReader{r.fd}, writer, old.Offset, old.Bytes,
		&format.Identity{Key: key, Sequence: old.Sequence}, buffer)
	if err == nil {
		if record.Bytes() != old.Bytes {
			return fmt.Errorf("%w: changed checkpoint record length", ErrCorrupt)
		}
		return fs.Check("compact.record:after")
	}
	var destination *destinationError
	if !errors.As(err, &destination) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return &SourceError{err}
	}
	return err
}

func (s *Store) InstallCheckpoint(capture *Capture, outputs []format.File) error {
	if s.Current.Generation == math.MaxUint64 {
		return fault("compact.generation", "not-applied", false, errors.New("storage: publication generation exhausted"))
	}
	// Incremental work is serialized with a full checkpoint. Only foreground
	// rotations can extend this captured prefix while copying takes place.
	catalog := format.Catalog{
		UUID: s.Current.UUID, Cutoff: capture.Current.Sequence, PrunedThrough: capture.Current.Sequence,
		ActiveID: s.Current.ActiveID, Files: append([]format.File(nil), outputs...),
	}
	catalog.Files = append(catalog.Files, s.Catalog.Files[len(capture.Catalog.Files):]...)
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
