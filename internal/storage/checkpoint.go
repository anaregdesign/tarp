package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/publication"
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

// Copy validates the captured history, copies live records in physical order and
// records their new addresses in moves. Only reserve takes the writer gate.
func (c *Checkpoint) Copy(ctx context.Context, history index.Backend, live index.View, moves *Relocations, reserve func() (*File, error)) (result error) {
	var file *File
	var fd *os.File
	var offset int64
	digest := sha256.New()
	fs := c.store.IO
	finish := func() error {
		if fd == nil {
			return nil
		}
		err := errors.Join(fs.Sync(fd, "compact.sync"), fd.Close())
		fd = nil
		if err != nil {
			return fault("output-sync", publication.NotApplied, false, err)
		}
		file.Size.Store(offset)
		c.entries = append(c.entries, format.File{ID: file.ID, Bytes: offset, Kind: format.Snapshot, Digest: [32]byte(digest.Sum(nil))})
		return nil
	}
	defer func() {
		if fd != nil {
			result = errors.Join(result, fd.Close())
		}
	}()
	err := c.capture.replayLive(ctx, fs, history, live, func(key string, old Location) (io.Writer, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fd == nil || offset > c.store.Config.TargetSegmentBytes-old.Bytes {
			if err := finish(); err != nil {
				return nil, err
			}
			var err error
			file, err = reserve()
			if err != nil {
				return nil, fault("output-create", publication.NotApplied, false, err)
			}
			fd, err = file.create(c.capture.Current.UUID, fs, "compact.segment", appendData{})
			if err != nil {
				return nil, fault("output-create", publication.NotApplied, false, err)
			}
			c.files = append(c.files, file)
			offset = format.SegmentHeaderBytes
			digest.Reset()
			digest.Write(format.EncodeSegment(format.Segment{UUID: c.capture.Current.UUID, ID: file.ID, Kind: file.Kind}))
		}
		writer := &outputWriter{fs: fs, fd: fd, offset: offset, point: "compact.write", hash: digest}
		next := old
		next.File, next.Offset = file, offset
		offset += old.Bytes
		return writer, moves.append(key, old, next)
	})
	if err != nil {
		return err
	}
	if err := finish(); err != nil {
		return err
	}
	if err := moves.seal(); err != nil {
		return err
	}
	// Verify the complete set against independently replayed DATA before the
	// new checkpoint is allowed to replace its sources. An inconsistent derived
	// index must never suppress the only surviving authoritative copy of a Put.
	if moves.count != history.Count() {
		return fmt.Errorf("%w: checkpoint index live count disagrees with replay", ErrCorrupt)
	}
	tx := history.Begin()
	defer tx.Abort()
	return moves.Each(func(key string, old, _ Location) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		loc, exists, err := tx.Lookup(key)
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		if !exists || loc.FileID != old.File.ID || loc.Offset != old.Offset || loc.Sequence != old.Sequence {
			return fmt.Errorf("%w: checkpoint index location disagrees with replay", ErrCorrupt)
		}
		return nil
	})
}

// Install splices the captured prefix into the current tail under the writer
// gate. A failed publication may have selected the snapshot files on disk.
func (c *Checkpoint) Install() error {
	s := c.store
	if s.Current.Generation == math.MaxUint64 {
		return fault("compact.generation", publication.NotApplied, false, errors.New("storage: publication generation exhausted"))
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
