package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/publication"
)

// SegmentCleaning pins DATA segments represented by consecutive catalog entries. Its
// private output is unreachable until InstallCleaned publishes the replacement.
type SegmentCleaning struct {
	sources  []CapturedSegment
	entries  []format.CatalogEntry
	position int
	output   *DataSegment
	uuid     [16]byte
	cutoff   uint64
	fences   *fenceCache
	bounds   snapshotBoundsBuilder
	created  bool
	digest   [sha256.Size]byte
}

// OutputBytes is the logical length of the finished replacement DATA file.
// An empty cleaned interval has zero bytes.
func (c *SegmentCleaning) OutputBytes() int64 { return c.output.Size.Load() }

func (c *SegmentCleaning) Sources() []CapturedSegment { return c.sources }

// ResetCopy prepares the same pinned input and reserved output ID for another
// attempt. Nothing has been installed or relocated at this point.
func (c *SegmentCleaning) ResetCopy(fs *IO) error {
	if err := c.Discard(fs); err != nil {
		return err
	}
	c.created = false
	c.output.parityReady, c.output.parityNeedsSync = false, false
	c.output.Size.Store(0)
	c.output.DeleteBytes.Store(0)
	c.digest = [sha256.Size]byte{}
	c.bounds = snapshotBoundsBuilder{}
	return nil
}

// BeginClean runs under the writer gate. The selected catalog is immutable.
// New log rotations may append to the catalog while these sources are copied.
func (s *Store) BeginClean(entries []format.CatalogEntry) (*SegmentCleaning, error) {
	if len(entries) == 0 || !entries[0].HasPhysicalFile() || !entries[len(entries)-1].HasPhysicalFile() {
		return nil, nil
	}
	pos := slices.Index(s.Catalog.Entries, entries[0])
	if pos < 0 || len(entries) > len(s.Catalog.Entries)-pos {
		return nil, nil
	}
	run := s.Catalog.Entries[pos : pos+len(entries)]
	if !slices.Equal(run, entries) {
		return nil, nil
	}
	kind := uint32(format.CleanedLogKind)
	if run[0].Kind == format.SnapshotKind {
		kind = format.SnapshotKind
	}
	c := &SegmentCleaning{entries: run, position: pos, uuid: s.Current.UUID, cutoff: s.Catalog.CheckpointCutoffSequence}
	if kind == format.SnapshotKind && s.HasOrderedBase() {
		c.fences = &s.fences
	}
	for _, entry := range run {
		if (entry.Kind == format.SnapshotKind) != (kind == format.SnapshotKind) {
			return nil, fmt.Errorf("%w: cleaner crossed checkpoint boundary", ErrCorrupt)
		}
		if !entry.HasPhysicalFile() {
			continue // Metadata-only coverage, carried forward by the merged interval.
		}
		f := s.Segments[entry.ID]
		if f == nil || f.Kind != entry.Kind || entry.ID == s.Current.ActiveID {
			return nil, fmt.Errorf("%w: cleaner source registry", ErrCorrupt)
		}
		c.sources = append(c.sources, CapturedSegment{Segment: f, Entry: entry})
	}
	id, err := s.allocateID()
	if err != nil {
		return nil, err
	}
	c.output = &DataSegment{ID: id, Kind: kind, Path: filepath.Join(s.Dir, DataName(s.Current.UUID, id))}
	for _, source := range c.sources {
		source.Segment.Pin()
	}
	return c, nil
}

func (c *SegmentCleaning) Release() {
	for _, source := range c.sources {
		source.Segment.Unpin()
	}
}

type pacedSource struct {
	fd    *os.File
	yield func() error
}

func (s pacedSource) ReadAt(b []byte, off int64) (int, error) {
	if err := s.yield(); err != nil {
		return 0, err
	}
	return (sourceReader{s.fd}).ReadAt(b, off)
}

// A retained key may span many chunks written from memory with no intervening
// source read. Yield between those writes just as we do on a discarded value.
type pacedOutput struct {
	w     *outputWriter
	yield func() error
}

func (w pacedOutput) Write(b []byte) (int, error) {
	if err := w.yield(); err != nil {
		return 0, &destinationError{err}
	}
	return w.w.Write(b)
}

// Copy streams each source in catalog order into the same output. Every
// tombstone is retained until the next full DATA checkpoint.
func (c *SegmentCleaning) Copy(ctx context.Context, fs *IO, moves *Relocations, history index.Backend, yield func() error, keep func(string, Location) (bool, error)) (result error) {
	writer := &outputWriter{fs: fs, offset: format.SegmentHeaderBytes, point: "clean.copy", hash: sha256.New()}
	defer func() {
		if writer.fd != nil {
			result = errors.Join(result, writer.fd.Close())
		}
	}()
	replay := replayBuilder{backend: history}
	defer replay.abort()
	choose := func(r format.RecordHeader, key string, old Location) (io.Writer, error) {
		if c.output.Kind == format.SnapshotKind {
			if err := replay.apply(old.Segment, old.RecordOffset, r, key); err != nil {
				// A damaged key can look like a duplicate before the
				// streaming decoder reaches the record's payload CRC.
				var input *SourceError
				if errors.As(err, &input) {
					err = selectedReadFailure(old.Segment.ID, err)
				}
				return nil, err
			}
		}
		if r.Kind == format.Put {
			retain, err := keep(key, old)
			if err != nil {
				return nil, err
			}
			if !retain {
				return nil, nil
			}
		}
		if writer.fd == nil {
			fd, err := os.OpenFile(c.output.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return nil, err
			}
			writer.fd, c.created = fd, true
			header := format.EncodeSegmentHeader(format.SegmentHeader{UUID: c.uuid, ID: c.output.ID, Kind: c.output.Kind})
			writer.hash.Write(header)
			if err := fs.Write(fd, header, 0, "clean.header"); err != nil {
				return nil, err
			}
		}
		if c.fences != nil {
			if c.bounds.last >= key {
				return nil, fmt.Errorf("%w: unordered clean snapshot output", ErrCorrupt)
			}
			c.bounds.add(key, writer.offset)
		}
		if r.Kind == format.Put {
			next := old
			next.Segment, next.RecordOffset = c.output, writer.offset
			if err := moves.append(key, old, next); err != nil {
				return nil, err
			}
		} else {
			c.output.DeleteBytes.Add(r.EncodedRecordBytes())
		}
		return pacedOutput{writer, yield}, nil
	}

	buf := make([]byte, 32<<10)
	for _, source := range c.sources {
		if err := c.scanSource(ctx, fs, source, buf, yield, choose); err != nil {
			return err
		}
	}
	if writer.fd != nil {
		if err := fs.Sync(writer.fd, "clean.output.sync"); err != nil {
			return err
		}
		c.output.Size.Store(writer.offset)
		c.digest = [sha256.Size]byte(writer.hash.Sum(nil))
		if c.fences != nil {
			c.bounds.complete(c.output, c.fences)
		}
	}
	return moves.seal()
}

// Validate each file with its own original kind and sequence interval before
// moving to the next input. Only one source descriptor is open at a time.
func (c *SegmentCleaning) scanSource(ctx context.Context, fs *IO, source CapturedSegment, buf []byte, yield func() error, choose func(format.RecordHeader, string, Location) (io.Writer, error)) (result error) {
	if err := yield(); err != nil {
		return err
	}
	fd, header, _, err := openSelected(source.Segment.Path, c.uuid, source.Entry, false)
	if err != nil {
		return selectedReadFailure(source.Entry.ID, &SourceError{Err: err})
	}
	defer func() {
		if err := fd.Close(); err != nil {
			result = errors.Join(result, &SourceError{Err: err})
		}
	}()
	digest := sha256.New()
	digest.Write(header)
	var pacedErr error
	input := &digestReader{ReaderAt: pacedSource{fd: fd, yield: func() error {
		pacedErr = yield()
		return pacedErr
	}}, hash: digest, next: format.SegmentHeaderBytes}
	span := format.NewRecordSequenceValidator(source.Entry, c.cutoff)
	return fs.Do("clean.scan", func() error {
		for off := int64(format.SegmentHeaderBytes); off < source.Entry.Bytes; {
			var callbackErr error
			r, err := format.SelectRecord(ctx, input, off, source.Entry.Bytes-off, buf, func(r format.RecordHeader, key string) (io.Writer, error) {
				if err := span.Accept(r); err != nil {
					return nil, selectedReadFailure(source.Entry.ID, err)
				}
				old := Location{Segment: source.Segment, RecordOffset: off, RecordBytes: r.EncodedRecordBytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence}
				var w io.Writer
				w, callbackErr = choose(r, key, old)
				return w, callbackErr
			})
			if err != nil {
				var destination *destinationError
				if callbackErr != nil || pacedErr != nil || errors.As(err, &destination) {
					return err
				}
				return selectedReadFailure(source.Entry.ID, err)
			}
			off += r.EncodedRecordBytes()
		}
		if err := span.Finish(); err != nil {
			return selectedReadFailure(source.Entry.ID, err)
		}
		return selectedReadFailure(source.Entry.ID, checkSealedDigest(digest, source.Entry))
	})
}

func (c *SegmentCleaning) replacement() format.CatalogEntry {
	replacement := format.CatalogEntry{Kind: c.output.Kind, FirstSequence: c.entries[0].FirstSequence, LastSequence: c.entries[len(c.entries)-1].LastSequence}
	if c.created {
		replacement.ID, replacement.Bytes, replacement.DataFileDigest = c.output.ID, c.output.Size.Load(), c.digest
	}
	return replacement
}

// ProtectCleaned verifies and syncs the replacement sidecar outside the writer
// gate. A metadata-only interval has no physical DATA to protect.
func (s *Store) ProtectCleaned(ctx context.Context, c *SegmentCleaning, yield func() error) error {
	if !c.created {
		return nil
	}
	src := CapturedSegment{Segment: c.output, Entry: c.replacement()}
	if err := s.buildParity(ctx, c.uuid, src, false, yield, "clean.parity"); err != nil {
		return err
	}
	if err := s.IO.SyncDir(s.Dir, "clean.parity.dirsync"); err != nil {
		return err
	}
	c.output.parityReady, c.output.parityNeedsSync = true, false
	return nil
}

// Discard removes only an exclusively created, unpublished output. Once the
// CURRENT replacement has been attempted, recovery owns ambiguous layouts.
func (c *SegmentCleaning) Discard(fs *IO) error {
	if !c.created {
		return nil
	}
	err := c.output.discardReplacement(fs, c.uuid, "clean.discard")
	if err != nil {
		return err
	}
	return fs.SyncDir(filepath.Dir(c.output.Path), "clean.discard.dirsync")
}

// InstallCleaned splices the replacement into the latest catalog under the
// writer gate. Every tail rotation and append since BeginClean survives.
func (s *Store) InstallCleaned(c *SegmentCleaning, offGate func(func() error) error) error {
	if c.created && !c.output.parityReady {
		return errors.New("storage: cleaner replacement is not protected")
	}
	start, end := c.position, c.position+len(c.entries)
	if end > len(s.Catalog.Entries) || !slices.Equal(s.Catalog.Entries[start:end], c.entries) {
		return fmt.Errorf("%w: cleaner source selection changed", ErrCorrupt)
	}
	for _, source := range c.sources {
		if s.Segments[source.Entry.ID] != source.Segment || source.Entry.ID == s.Current.ActiveID {
			return fmt.Errorf("%w: cleaner source registry changed", ErrCorrupt)
		}
	}
	if s.Current.PublicationGeneration == math.MaxUint64 {
		return fault("clean.generation", publication.NotApplied, false, errors.New("storage: publication generation exhausted"))
	}
	catalog := s.Catalog
	catalog.Index = format.IndexCheckpoint{}
	catalog.Entries = slices.Clone(catalog.Entries)
	catalog.PrunedThroughSequence = s.Current.LastUserSequence
	replacement := c.replacement()
	if !c.created && replacement.Kind == format.SnapshotKind {
		catalog.Entries = slices.Delete(catalog.Entries, start, end)
	} else {
		catalog.Entries = slices.Replace(catalog.Entries, start, end, replacement)
	}
	candidate := s.Current
	candidate.PublicationGeneration++
	if err := s.publishCatalogFor(catalog, candidate, "clean.", offGate); err != nil {
		if err == errCatalogChanged {
			// A rotation changed the catalog during preparation. Publish the
			// rebased catalog once under the gate to guarantee progress.
			return s.InstallCleaned(c, nil)
		}
		return err
	}
	if c.created {
		c.output.markSealed(catalog.UUID, replacement)
		s.Segments[c.output.ID] = c.output
	}
	for _, source := range c.sources {
		s.retireData(source.Segment)
	}
	return nil
}
