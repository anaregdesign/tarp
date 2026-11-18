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

// CleanSegment pins a single immutable catalog entry. Its private output is
// unreachable until InstallCleaned publishes the replacement catalog.
type CleanSegment struct {
	Source   *File
	output   *File
	original format.File
	uuid     [16]byte
	cutoff   uint64
	created  bool
}

type Relocation struct {
	Key      string
	Old, New Location
}

// BeginClean runs under the writer gate. Reserving the output ID here allows
// file creation, streaming and sync to proceed while foreground writers append.
func (s *Store) BeginClean(entry format.File) (*CleanSegment, error) {
	if entry.ID == 0 || entry.ID == s.Current.ActiveID || !slices.Contains(s.Catalog.Files, entry) {
		return nil, nil
	}
	source := s.Files[entry.ID]
	if source == nil || source.Kind != entry.Kind {
		return nil, fmt.Errorf("%w: cleaner source registry", ErrCorrupt)
	}
	id, err := s.allocateID()
	if err != nil {
		return nil, err
	}
	kind := uint32(format.Cleaned)
	if entry.Kind == format.Snapshot {
		kind = format.Snapshot
	}
	output := &File{ID: id, Kind: kind, Path: filepath.Join(s.Dir, DataName(s.Current.UUID, id))}
	source.Pin()
	return &CleanSegment{Source: source, output: output, original: entry, uuid: s.Current.UUID, cutoff: s.Catalog.Cutoff}, nil
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

// Copy reads the source once. keep consults the current index one key at a
// time. Every tombstone is retained until the next complete checkpoint.
func (c *CleanSegment) Copy(ctx context.Context, fs *IO, yield func() error, keep func(string, Location) bool) (moves []Relocation, result error) {
	if err := yield(); err != nil {
		return nil, err
	}
	source, _, _, err := openSelected(c.Source.Path, c.uuid, c.original, false)
	if err != nil {
		return nil, &SourceError{Err: err}
	}
	defer func() {
		if err := source.Close(); err != nil {
			result = errors.Join(result, &SourceError{Err: err})
		}
	}()
	input := pacedSource{fd: source, yield: yield}
	writer := &outputWriter{fs: fs, offset: format.SegmentHeaderBytes, point: "clean.copy"}
	defer func() {
		if writer.fd != nil {
			result = errors.Join(result, writer.fd.Close())
		}
	}()
	var snapshotKeys map[string]struct{}
	if c.Source.Kind == format.Snapshot {
		snapshotKeys = make(map[string]struct{})
	}
	span := format.NewRecordRange(c.original, c.cutoff)
	buf := make([]byte, 32<<10)
	err = fs.Do("clean.scan", func() error {
		for off := int64(format.SegmentHeaderBytes); off < c.original.Bytes; {
			r, err := format.SelectRecord(ctx, input, off, c.original.Bytes-off, buf, func(r format.Record, key string) (io.Writer, error) {
				if err := span.Accept(r); err != nil {
					return nil, err
				}
				if snapshotKeys != nil {
					if _, exists := snapshotKeys[key]; exists {
						return nil, fmt.Errorf("%w: duplicate snapshot key", ErrCorrupt)
					}
					snapshotKeys[key] = struct{}{}
				}
				old := Location{File: c.Source, Offset: off, Bytes: r.Bytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence}
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
			})
			if err != nil {
				return err
			}
			off += r.Bytes()
		}
		return span.Finish()
	})
	if err != nil {
		return nil, err
	}
	if writer.fd != nil {
		if err := fs.Sync(writer.fd, "clean.output.sync"); err != nil {
			return nil, err
		}
		c.output.Size.Store(writer.offset)
	}
	return moves, nil
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
func (s *Store) InstallCleaned(c *CleanSegment) error {
	pos := slices.Index(s.Catalog.Files, c.original)
	if pos < 0 || s.Files[c.original.ID] != c.Source || c.original.ID == s.Current.ActiveID {
		return fmt.Errorf("%w: cleaner source selection changed", ErrCorrupt)
	}
	if s.Current.Generation == math.MaxUint64 {
		return fault("clean.generation", "not-applied", false, errors.New("storage: publication generation exhausted"))
	}
	catalog := s.Catalog
	catalog.Files = slices.Clone(catalog.Files)
	catalog.PrunedThrough = s.Current.Sequence
	replacement := c.original
	switch {
	case c.created:
		replacement.ID, replacement.Kind, replacement.Bytes = c.output.ID, c.output.Kind, c.output.Size.Load()
		catalog.Files[pos] = replacement
	case c.original.Kind == format.Snapshot:
		catalog.Files = slices.Delete(catalog.Files, pos, pos+1)
	default:
		replacement.ID, replacement.Kind, replacement.Bytes = 0, format.Cleaned, 0
		catalog.Files[pos] = replacement
	}
	candidate := s.Current
	candidate.Generation++
	if err := s.publishCatalogFor(catalog, candidate, "clean."); err != nil {
		return err
	}
	if c.created {
		s.Files[c.output.ID] = c.output
	}
	if s.retired == nil {
		s.retired = make(map[uint64]*File)
	}
	s.retired[c.Source.ID] = c.Source
	return nil
}
