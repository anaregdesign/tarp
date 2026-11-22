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

// DataCheckpoint owns the private snapshot files until Install attempts publication.
// Copy runs outside the writer gate. ReserveSnapshotFile and Install require it.
type DataCheckpoint struct {
	store       *Store
	capture     *Capture
	files       []*DataSegment
	entries     []format.CatalogEntry
	lossSources []lossSource
	attempted   bool
}

func (s *Store) NewDataCheckpoint(capture *Capture) *DataCheckpoint {
	return &DataCheckpoint{store: s, capture: capture}
}

// ReserveSnapshotFile assigns a name under the writer gate. Only a successfully
// installed checkpoint registers its files in the store.
func (s *Store) ReserveSnapshotFile() (*DataSegment, error) {
	return s.reserveSegment(format.SnapshotKind)
}

// Entries describes completed snapshot files for checkpoint size accounting.
func (c *DataCheckpoint) Entries() []format.CatalogEntry { return c.entries }

// Copy validates the captured history, copies live records in physical order and
// records their new addresses in moves. Only reserve takes the writer gate.
func (c *DataCheckpoint) Copy(ctx context.Context, history index.Backend, live index.View, moves *Relocations, reserve func() (*DataSegment, error)) error {
	err := c.writeSnapshot(ctx, moves, reserve, func(emit func(string, Location) (io.Writer, error)) error {
		return c.capture.replayLive(ctx, c.store.IO, history, live, emit)
	})
	if err != nil {
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
		if !exists || loc.FileID != old.File.ID || loc.RecordOffset != old.RecordOffset || loc.Sequence != old.Sequence {
			return fmt.Errorf("%w: checkpoint index location disagrees with replay", ErrCorrupt)
		}
		return nil
	})
}

// writeSnapshot owns private outputs through the complete source scan. Both
// normal replay and recovery use the same rollover, sync and relocation path.
func (c *DataCheckpoint) writeSnapshot(ctx context.Context, moves *Relocations, reserve func() (*DataSegment, error), scan func(func(string, Location) (io.Writer, error)) error) (result error) {
	var file *DataSegment
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
		c.entries = append(c.entries, format.CatalogEntry{ID: file.ID, Bytes: offset, Kind: format.SnapshotKind, DataFileDigest: [32]byte(digest.Sum(nil))})
		return nil
	}
	defer func() {
		if fd != nil {
			result = errors.Join(result, fd.Close())
		}
	}()
	err := scan(func(key string, old Location) (io.Writer, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fd == nil || offset > c.store.Config.TargetSegmentBytes-old.RecordBytes {
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
			digest.Write(format.EncodeSegmentHeader(format.SegmentHeader{UUID: c.capture.Current.UUID, ID: file.ID, Kind: file.Kind}))
		}
		writer := &outputWriter{fs: fs, fd: fd, offset: offset, point: "compact.write", hash: digest}
		next := old
		next.File, next.RecordOffset = file, offset
		offset += old.RecordBytes
		return writer, moves.append(key, old, next)
	})
	if err != nil {
		return err
	}
	if err := finish(); err != nil {
		return err
	}
	return moves.seal()
}

// Protect validates each staged snapshot against the digest computed during
// Copy. Sync all new sidecar names together before publishing their catalog.
func (c *DataCheckpoint) Protect(ctx context.Context) error {
	if len(c.entries) == 0 {
		return nil
	}
	for i, entry := range c.entries {
		src := CapturedFile{File: c.files[i], Entry: entry}
		if err := c.store.buildParity(ctx, c.capture.Current.UUID, src, false, nil, "compact.parity"); err != nil {
			return err
		}
	}
	if err := c.store.IO.SyncDir(c.store.Dir, "compact.parity.dirsync"); err != nil {
		return err
	}
	for _, f := range c.files {
		f.parityReady, f.parityNeedsSync = true, false
	}
	return nil
}

// Install splices the captured prefix into the current tail under the writer
// gate. A failed publication may have selected the snapshot files on disk.
func (c *DataCheckpoint) Install() error {
	s := c.store
	for _, f := range c.files {
		if !f.parityReady {
			return errors.New("storage: checkpoint replacement is not protected")
		}
	}
	if s.Current.PublicationGeneration == math.MaxUint64 {
		return fault("compact.generation", publication.NotApplied, false, errors.New("storage: publication generation exhausted"))
	}
	catalog := format.Catalog{
		UUID: s.Current.UUID, CheckpointCutoffSequence: c.capture.Current.LastUserSequence, PrunedThroughSequence: c.capture.Current.LastUserSequence,
		ActiveID: s.Current.ActiveID, Entries: append([]format.CatalogEntry(nil), c.entries...),
	}
	catalog.Entries = append(catalog.Entries, s.Catalog.Entries[len(c.capture.Catalog.Entries):]...)
	candidate := s.Current
	candidate.PublicationGeneration++
	c.attempted = true
	if err := s.publishCatalog(catalog, candidate); err != nil {
		return err
	}
	for i, file := range c.files {
		file.markSealed(catalog.UUID, c.entries[i])
		s.Files[file.ID] = file
	}
	return nil
}

// Discard removes only privately owned files before publication was attempted.
// Installation transfers cleanup to the store and its recovery protocol.
func (c *DataCheckpoint) Discard() error {
	if c.attempted || len(c.files) == 0 {
		return nil
	}
	var result error
	for _, f := range c.files {
		result = errors.Join(result, f.discardReplacement(c.store.IO, c.capture.Current.UUID, "compact.discard"))
	}
	return errors.Join(result, c.store.IO.SyncDir(c.store.Dir, "compact.discard.dirsync"))
}
