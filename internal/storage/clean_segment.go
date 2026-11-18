package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
)

// CleanSegment pins the physical sources in one contiguous catalog run. Its
// private output is unreachable until InstallCleaned publishes the replacement.
type CleanSegment struct {
	sources  []CapturedFile
	entries  []format.File
	position int
	output   *File
	uuid     [16]byte
	cutoff   uint64
	created  bool
}

type Relocation struct {
	Key      string
	Old, New Location
}

// BeginClean runs under the writer gate. The selected catalog is immutable.
// New log rotations may append to the catalog while these sources are copied.
func (s *Store) BeginClean(entries []format.File) (*CleanSegment, error) {
	if len(entries) == 0 || entries[0].ID == 0 || entries[len(entries)-1].ID == 0 {
		return nil, nil
	}
	pos := slices.Index(s.Catalog.Files, entries[0])
	if pos < 0 || len(entries) > len(s.Catalog.Files)-pos {
		return nil, nil
	}
	run := s.Catalog.Files[pos : pos+len(entries)]
	if !slices.Equal(run, entries) {
		return nil, nil
	}
	kind := uint32(format.Cleaned)
	if run[0].Kind == format.Snapshot {
		kind = format.Snapshot
	}
	c := &CleanSegment{entries: run, position: pos, uuid: s.Current.UUID, cutoff: s.Catalog.Cutoff}
	for _, entry := range run {
		if (entry.Kind == format.Snapshot) != (kind == format.Snapshot) {
			return nil, fmt.Errorf("%w: cleaner crossed checkpoint boundary", ErrCorrupt)
		}
		if entry.ID == 0 {
			continue // Metadata-only coverage, carried forward by the merged interval.
		}
		f := s.Files[entry.ID]
		if f == nil || f.Kind != entry.Kind || entry.ID == s.Current.ActiveID {
			return nil, fmt.Errorf("%w: cleaner source registry", ErrCorrupt)
		}
		c.sources = append(c.sources, CapturedFile{File: f, Entry: entry})
	}
	id, err := s.allocateID()
	if err != nil {
		return nil, err
	}
	c.output = &File{ID: id, Kind: kind, Path: filepath.Join(s.Dir, DataName(s.Current.UUID, id))}
	for _, source := range c.sources {
		source.File.Pin()
	}
	return c, nil
}

func (c *CleanSegment) Release() {
	for _, source := range c.sources {
		source.File.Unpin()
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
		return 0, err
	}
	return w.w.Write(b)
}

// Copy streams each source in catalog order into the same output. Every
// tombstone is retained until the next complete checkpoint.
func (c *CleanSegment) Copy(ctx context.Context, fs *IO, yield func() error, keep func(string, Location) bool) (moves []Relocation, result error) {
	writer := &outputWriter{fs: fs, offset: format.SegmentHeaderBytes, point: "clean.copy"}
	defer func() {
		if writer.fd != nil {
			result = errors.Join(result, writer.fd.Close())
		}
	}()
	var snapshotKeys map[string]struct{}
	if c.output.Kind == format.Snapshot {
		snapshotKeys = make(map[string]struct{})
	}
	choose := func(r format.Record, key string, old Location) (io.Writer, error) {
		if snapshotKeys != nil {
			if _, exists := snapshotKeys[key]; exists {
				return nil, fmt.Errorf("%w: duplicate snapshot key", ErrCorrupt)
			}
			snapshotKeys[key] = struct{}{}
		}
		if r.Kind == format.Put && !keep(key, old) {
			return nil, nil
		}
		if writer.fd == nil {
			fd, err := os.OpenFile(c.output.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err != nil {
				return nil, err
			}
			writer.fd, c.created = fd, true
			header := format.EncodeSegment(format.Segment{UUID: c.uuid, ID: c.output.ID, Kind: c.output.Kind})
			if err := fs.Write(fd, header, 0, "clean.header"); err != nil {
				return nil, err
			}
		}
		if r.Kind == format.Put {
			next := old
			next.File, next.Offset = c.output, writer.offset
			moves = append(moves, Relocation{key, old, next})
		} else {
			c.output.DeleteBytes.Add(r.Bytes())
		}
		return pacedOutput{writer, yield}, nil
	}

	buf := make([]byte, 32<<10)
	for _, source := range c.sources {
		if err := c.scanSource(ctx, fs, source, buf, yield, choose); err != nil {
			return nil, err
		}
	}
	if writer.fd != nil {
		if err := fs.Sync(writer.fd, "clean.output.sync"); err != nil {
			return nil, err
		}
		c.output.Size.Store(writer.offset)
	}
	return moves, nil
}

// Validate each file with its own original kind and sequence interval before
// moving to the next input. Only one source descriptor is open at a time.
func (c *CleanSegment) scanSource(ctx context.Context, fs *IO, source CapturedFile, buf []byte, yield func() error, choose func(format.Record, string, Location) (io.Writer, error)) (result error) {
	if err := yield(); err != nil {
		return err
	}
	fd, _, _, err := openSelected(source.File.Path, c.uuid, source.Entry, false)
	if err != nil {
		return &SourceError{Err: err}
	}
	defer func() {
		if err := fd.Close(); err != nil {
			result = errors.Join(result, &SourceError{Err: err})
		}
	}()
	input := pacedSource{fd: fd, yield: yield}
	span := format.NewRecordRange(source.Entry, c.cutoff)
	return fs.Do("clean.scan", func() error {
		for off := int64(format.SegmentHeaderBytes); off < source.Entry.Bytes; {
			r, err := format.SelectRecord(ctx, input, off, source.Entry.Bytes-off, buf, func(r format.Record, key string) (io.Writer, error) {
				if err := span.Accept(r); err != nil {
					return nil, err
				}
				old := Location{File: source.File, Offset: off, Bytes: r.Bytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence}
				return choose(r, key, old)
			})
			if err != nil {
				return err
			}
			off += r.Bytes()
		}
		return span.Finish()
	})
}

// Discard removes only an exclusively created, unpublished output. Once the
// CURRENT replacement has been attempted, recovery owns ambiguous layouts.
func (c *CleanSegment) Discard(fs *IO) error {
	if !c.created {
		return nil
	}
	err := fs.Do("clean.discard", func() error { return os.Remove(c.output.Path) })
	if err != nil {
		return err
	}
	return fs.SyncDir(filepath.Dir(c.output.Path), "clean.discard.dirsync")
}

// InstallCleaned splices the replacement into the latest catalog under the
// writer gate. Every tail rotation and append since BeginClean survives.
func (s *Store) InstallCleaned(c *CleanSegment, offGate func(func() error) error) error {
	start, end := c.position, c.position+len(c.entries)
	if end > len(s.Catalog.Files) || !slices.Equal(s.Catalog.Files[start:end], c.entries) {
		return fmt.Errorf("%w: cleaner source selection changed", ErrCorrupt)
	}
	for _, source := range c.sources {
		if s.Files[source.Entry.ID] != source.File || source.Entry.ID == s.Current.ActiveID {
			return fmt.Errorf("%w: cleaner source registry changed", ErrCorrupt)
		}
	}
	if s.Current.Generation == math.MaxUint64 {
		return fault("clean.generation", "not-applied", false, errors.New("storage: publication generation exhausted"))
	}
	catalog := s.Catalog
	catalog.Files = slices.Clone(catalog.Files)
	catalog.PrunedThrough = s.Current.Sequence
	replacement := format.File{Kind: c.output.Kind, First: c.entries[0].First, Last: c.entries[len(c.entries)-1].Last}
	if c.created {
		replacement.ID, replacement.Bytes = c.output.ID, c.output.Size.Load()
	}
	if !c.created && replacement.Kind == format.Snapshot {
		catalog.Files = slices.Delete(catalog.Files, start, end)
	} else {
		catalog.Files = slices.Replace(catalog.Files, start, end, replacement)
	}
	candidate := s.Current
	candidate.Generation++
	if err := s.publishCatalogFor(catalog, candidate, "clean.", offGate); err != nil {
		if err == errCatalogChanged {
			// A rotation changed the catalog during preparation. Publish the
			// rebased catalog once under the gate to guarantee progress.
			return s.InstallCleaned(c, nil)
		}
		return err
	}
	if c.created {
		s.Files[c.output.ID] = c.output
	}
	if s.retired == nil {
		s.retired = make(map[uint64]*File)
	}
	for _, source := range c.sources {
		s.retired[source.File.ID] = source.File
	}
	return nil
}
