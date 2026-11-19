package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"

	"github.com/anaregdesign/tarp/internal/format"
)

// Checkpoint owns the private snapshot files until Install attempts publication.
// Copy runs outside the writer gate. ReserveSnapshotFile and Install require it.
type Checkpoint struct {
	store     *Store
	capture   *Capture
	files     []*File
	entries   []format.File
	attempted bool
}

func (s *Store) NewCheckpoint(capture *Capture) *Checkpoint {
	return &Checkpoint{store: s, capture: capture}
}

// ReserveSnapshotFile assigns a name under the writer gate. Only a successfully
// installed checkpoint registers its files in the store.
func (s *Store) ReserveSnapshotFile() (*File, error) {
	return s.reserveSegment(format.Snapshot)
}

// Entries describes completed snapshot files for checkpoint size accounting.
func (c *Checkpoint) Entries() []format.File { return c.entries }

// Copy streams the live records into target-sized files and fills each new
// location. Only reserve needs the caller's writer gate, once per output.
func (c *Checkpoint) Copy(ctx context.Context, moves []Relocation, reserve func() (*File, error)) (result error) {
	var reader snapshotReader
	var file *File
	var fd *os.File
	var offset int64
	fs := c.store.IO
	finish := func() error {
		if fd == nil {
			return nil
		}
		err := errors.Join(fs.Sync(fd, "compact.sync"), fd.Close())
		fd = nil
		if err != nil {
			return fault("output-sync", "not-applied", false, err)
		}
		file.Size.Store(offset)
		c.entries = append(c.entries, format.File{ID: file.ID, Bytes: offset, Kind: format.Snapshot})
		return nil
	}
	defer func() {
		if err := reader.close(); err != nil {
			result = errors.Join(result, fault("source-close", "not-applied", false, err))
		}
		if fd != nil {
			result = errors.Join(result, fd.Close())
		}
	}()
	buffer := make([]byte, 32<<10)
	for i := range moves {
		move := &moves[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		if fd == nil || offset > c.store.Config.TargetSegmentBytes-move.Old.Bytes {
			if err := finish(); err != nil {
				return err
			}
			var err error
			file, err = reserve()
			if err != nil {
				return fault("output-create", "not-applied", false, err)
			}
			fd, err = file.create(c.capture.Current.UUID, fs, "compact.segment", appendData{})
			if err != nil {
				return fault("output-create", "not-applied", false, err)
			}
			c.files = append(c.files, file)
			offset = format.SegmentHeaderBytes
		}
		if err := reader.copyRecord(ctx, fs, move.Old, move.Key, fd, offset, buffer); err != nil {
			return err
		}
		move.New = move.Old
		move.New.File, move.New.Offset = file, offset
		offset += move.Old.Bytes
	}
	if err := reader.close(); err != nil {
		return fault("source-close", "not-applied", false, err)
	}
	return finish()
}

// snapshotReader reuses a single source descriptor while copying a pinned
// checkpoint in physical read order. Close it before publishing the result.
type snapshotReader struct {
	source *File
	fd     *os.File
}

func (r *snapshotReader) close() error {
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

func (r *snapshotReader) copyRecord(ctx context.Context, fs *IO, old Location, key string, dst *os.File, offset int64, buffer []byte) error {
	if r.source != old.File {
		if err := r.close(); err != nil {
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

// Install splices the captured prefix into the current tail under the writer
// gate. A failed publication may have selected the snapshot files on disk.
func (c *Checkpoint) Install() error {
	s := c.store
	if s.Current.Generation == math.MaxUint64 {
		return fault("compact.generation", "not-applied", false, errors.New("storage: publication generation exhausted"))
	}
	catalog := format.Catalog{
		UUID: s.Current.UUID, Cutoff: c.capture.Current.Sequence, PrunedThrough: c.capture.Current.Sequence,
		ActiveID: s.Current.ActiveID, Files: append([]format.File(nil), c.entries...),
	}
	catalog.Files = append(catalog.Files, s.Catalog.Files[len(c.capture.Catalog.Files):]...)
	candidate := s.Current
	candidate.Generation++
	c.attempted = true
	if err := s.publishCatalog(catalog, candidate); err != nil {
		return err
	}
	for _, file := range c.files {
		s.Files[file.ID] = file
	}
	return nil
}

// Discard removes only privately owned files before publication was attempted.
// Installation transfers cleanup to the store and its recovery protocol.
func (c *Checkpoint) Discard() error {
	if c.attempted || len(c.files) == 0 {
		return nil
	}
	var result error
	for _, f := range c.files {
		err := c.store.IO.Do("compact.discard", func() error { return os.Remove(f.Path) })
		if !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return errors.Join(result, c.store.IO.SyncDir(c.store.Dir, "compact.discard.dirsync"))
}
