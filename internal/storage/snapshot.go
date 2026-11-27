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
	Current         format.Frontier
	Catalog         format.Catalog
	Manifest        []byte
	Segments        []CapturedSegment
	activeDigest    [sha256.Size]byte
	orderedSnapshot bool
}

// Capture pins the selected DATA segments and captures the committed end
// offset of the active segment. The caller holds the writer gate. Published
// metadata is immutable, and the runtime index is captured separately.
func (s *Store) Capture() (*Capture, error) {
	active, err := s.Catalog.ActiveEntry(s.Current)
	if err != nil {
		return nil, err
	}
	c := &Capture{Current: s.Current, Catalog: s.Catalog, Manifest: bytes.Clone(s.Manifest), orderedSnapshot: s.HasOrderedBase()}
	c.Catalog.Entries = append([]format.CatalogEntry(nil), s.Catalog.Entries...)
	for _, entry := range s.Catalog.Entries {
		if entry.HasPhysicalFile() {
			c.Segments = append(c.Segments, CapturedSegment{Segment: s.Segments[entry.ID], Entry: entry})
		}
	}
	activeSegment := s.Segments[active.ID]
	activeSegment.activeDigest.Sum(c.activeDigest[:0])
	c.Segments = append(c.Segments, CapturedSegment{Segment: activeSegment, Entry: active})
	for i := range c.Segments {
		captured := &c.Segments[i]
		captured.liveKeys = captured.Segment.LiveKeys.Load()
		captured.liveBytes = captured.Segment.LiveBytes.Load()
		captured.Segment.PinRetained()
	}
	return c, nil
}

func (c *Capture) Release() {
	for _, entry := range c.Segments {
		entry.Segment.UnpinRetained()
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

// replayLive validates all captured DATA and selects output in source order.
// The captured live index guides copying, but an independent replay must
// confirm the winning records before any output is published.
func (c *DataCheckpoint) replayLive(ctx context.Context, history index.Backend, live index.View, selectOutput func(string, Location) (io.Writer, error), reuse reuseSnapshot) error {
	capture, fs := c.capture, c.store.IO
	replay := newReplayBuilder(ctx, history, capture.Catalog.PrunedThroughSequence)
	defer replay.abort()
	buffer := make([]byte, 32<<10)
	reader := bufio.NewReaderSize(nil, len(buffer))
	for _, captured := range capture.Segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		fd, header, _, err := openSelected(captured.Segment.Path, capture.Current.UUID, captured.Entry, captured.Segment.ID == capture.Current.ActiveID)
		if err != nil {
			return selectedReadFailure(captured.Entry.ID, &SourceError{Err: err})
		}
		copying := false
		// Captured byte accounting is only a hint to avoid staging a segment
		// whose records are likely to survive. Every address and replay winner
		// still has to agree, even if the encoded lengths have not changed.
		keep := captured.Entry.Kind == format.SnapshotKind &&
			captured.liveBytes == captured.Entry.Bytes-format.SegmentHeaderBytes && captured.Segment.UnavailableData() == nil
		var bounds scannedSnapshot
		mismatch := false
		err = fs.Do("compact.scan", func() error {
			span := format.NewRecordSequenceValidator(captured.Entry, capture.Catalog.CheckpointCutoffSequence)
			const start = int64(format.SegmentHeaderBytes)
			source := io.Reader(io.NewSectionReader(c.paceInput(sourceReader{fd}), start, captured.Entry.Bytes-start))
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
						mismatch = mismatch || keep
						return nil, nil
					}
					if loc.RecordBytes != r.EncodedRecordBytes() || loc.Sequence != r.Sequence || loc.ValueBytes != r.ValueBytes {
						return nil, fmt.Errorf("%w: index disagrees with captured DATA", ErrCorrupt)
					}
					if keep {
						bounds.add(key, off)
						return nil, nil
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
					var work *IndexWorkError
					if callbackErr != nil || errors.As(err, &destination) || errors.As(err, &work) {
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
		if keep {
			if mismatch {
				return fmt.Errorf("%w: captured live byte count disagrees with snapshot %d", ErrCorrupt, captured.Entry.ID)
			}
			if err := reuse(captured.Entry, &bounds); err != nil {
				return err
			}
		}
	}
	if len(c.reused) == 0 {
		if err := replay.finish(); err != nil {
			return err
		}
		return ctx.Err()
	}
	// Replay can produce only a subset of the validated original addresses
	// in each snapshot. Equality of the per-file counts proves that every
	// original record still describes a live key.
	winners := make(map[uint64]int64, len(c.reused))
	if err := replay.finishWith(func(_ string, addr index.Address) error {
		if _, kept := c.reused[addr.FileID]; kept {
			winners[addr.FileID]++
		}
		return ctx.Err()
	}); err != nil {
		return err
	}
	for id, recordCount := range c.reused {
		if winners[id] != recordCount {
			return fmt.Errorf("%w: snapshot %d winners disagree with replay", ErrCorrupt, id)
		}
	}
	return ctx.Err()
}
