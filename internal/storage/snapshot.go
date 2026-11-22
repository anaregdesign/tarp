package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

// CapturedSegment binds a pinned DATA segment to the catalog entry used to read it.
type CapturedSegment struct {
	Segment             *DataSegment
	Entry               format.CatalogEntry
	liveKeys, liveBytes int64 // owned by the logical index at capture
}

type Capture struct {
	Current  format.Frontier
	Catalog  format.Catalog
	Manifest []byte
	Segments []CapturedSegment
}

// Capture pins the selected DATA segments and captures the committed end
// offset of the active segment. The caller holds the writer gate. Published
// metadata is immutable, and the runtime index is captured separately.
func (s *Store) Capture() (*Capture, error) {
	active, err := s.Catalog.ActiveEntry(s.Current)
	if err != nil {
		return nil, err
	}
	c := &Capture{Current: s.Current, Catalog: s.Catalog, Manifest: bytes.Clone(s.Manifest)}
	c.Catalog.Entries = append([]format.CatalogEntry(nil), s.Catalog.Entries...)
	for _, entry := range s.Catalog.Entries {
		if entry.HasPhysicalFile() {
			c.Segments = append(c.Segments, CapturedSegment{Segment: s.Segments[entry.ID], Entry: entry})
		}
	}
	c.Segments = append(c.Segments, CapturedSegment{Segment: s.Segments[active.ID], Entry: active})
	for i := range c.Segments {
		captured := &c.Segments[i]
		captured.liveKeys = captured.Segment.LiveKeys.Load()
		captured.liveBytes = captured.Segment.LiveBytes.Load()
		captured.Segment.Pin()
	}
	return c, nil
}

func (c *Capture) Release() {
	for _, entry := range c.Segments {
		entry.Segment.Unpin()
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
func (c *Capture) replayLive(ctx context.Context, fs *IO, history index.Backend, live index.View, selectOutput func(string, Location) (io.Writer, error)) error {
	replay := newReplayBuilder(ctx, history, c.Catalog.PrunedThroughSequence)
	defer replay.abort()
	buffer := make([]byte, 32<<10)
	reader := bufio.NewReaderSize(nil, len(buffer))
	for _, captured := range c.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		fd, header, _, err := openSelected(captured.Segment.Path, c.Current.UUID, captured.Entry, captured.Segment.ID == c.Current.ActiveID)
		if err != nil {
			return selectedReadFailure(captured.Entry.ID, &SourceError{Err: err})
		}
		copying := false
		err = fs.Do("compact.scan", func() error {
			span := format.NewRecordSequenceValidator(captured.Entry, c.Catalog.CheckpointCutoffSequence)
			const start = int64(format.SegmentHeaderBytes)
			source := io.Reader(io.NewSectionReader(sourceReader{fd}, start, captured.Entry.Bytes-start))
			var h hash.Hash
			if captured.Entry.DataFileDigest != ([32]byte{}) {
				h = sha256.New()
				h.Write(header)
				source = io.TeeReader(source, h)
			}
			reader.Reset(source)
			for off := start; off < captured.Entry.Bytes; {
				copied := false
				var callbackErr error
				r, err := format.SelectStreamRecord(ctx, reader, off, captured.Entry.Bytes-off, buffer, func(r format.RecordHeader, key string) (_ io.Writer, outcome error) {
					defer func() { callbackErr = outcome }()
					if err := span.Accept(r); err != nil {
						return nil, selectedReadFailure(captured.Entry.ID, err)
					}
					if err := replay.apply(captured.Segment, off, r, key); err != nil {
						// The key is decoded before SelectStreamRecord reaches its
						// payload checksum. A damaged Delete can fail replay first.
						var source *SourceError
						if errors.As(err, &source) {
							err = selectedReadFailure(captured.Entry.ID, err)
						}
						return nil, err
					}
					if r.Kind != format.Put {
						return nil, nil
					}
					loc, exists, err := live.Lookup(key)
					if err != nil {
						return nil, &SourceError{Err: err}
					}
					if !exists || loc.FileID != captured.Segment.ID || loc.RecordOffset != off {
						return nil, nil
					}
					if loc.RecordBytes != r.EncodedRecordBytes() || loc.Sequence != r.Sequence || loc.ValueBytes != r.ValueBytes {
						return nil, fmt.Errorf("%w: index disagrees with captured DATA", ErrCorrupt)
					}
					if !copying {
						if err := fs.Check("compact.source.open:after"); err != nil {
							return nil, &SourceError{Err: err}
						}
						copying = true
					}
					copied = true
					return selectOutput(key, Location{Segment: captured.Segment, RecordOffset: off, RecordBytes: r.EncodedRecordBytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence})
				})
				if err != nil {
					var destination *destinationError
					if callbackErr != nil || errors.As(err, &destination) {
						return err
					}
					if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						err = errors.Join(ErrCorrupt, err)
					}
					return selectedReadFailure(captured.Entry.ID, err)
				}
				if copied {
					if err := fs.Check("compact.record:after"); err != nil {
						return err
					}
				}
				off += r.EncodedRecordBytes()
			}
			if err := span.Finish(); err != nil {
				return selectedReadFailure(captured.Entry.ID, err)
			}
			if h != nil {
				return selectedReadFailure(captured.Entry.ID, checkSealedDigest(h, captured.Entry))
			}
			return ctx.Err()
		})
		if closeErr := fd.Close(); closeErr != nil {
			err = errors.Join(err, &SourceError{Err: closeErr})
		}
		if err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(captured.Segment.Path), err)
		}
	}
	if err := replay.finish(); err != nil {
		return err
	}
	return ctx.Err()
}
