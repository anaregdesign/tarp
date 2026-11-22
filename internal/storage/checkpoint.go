package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/publication"
)

// DataCheckpoint owns the private snapshot files until Install attempts publication.
// Copy runs outside the writer gate. ReserveSnapshotSegment and Install require it.
type DataCheckpoint struct {
	store          *Store
	scratch        *orderedScratchBudget
	capture        *Capture
	segments       []*DataSegment
	entries        []format.CatalogEntry // all selected outputs, including reused snapshots
	privateEntries []format.CatalogEntry // completed outputs owned by this attempt
	reused         map[uint64]struct{}
	ordered        bool // globally ordered output, valid only after successful scan
	certifiedBase  bool
	ordinals       map[uint64]int
	lossSources    []lossSource
	attempted      bool
}

func (s *Store) NewDataCheckpoint(capture *Capture) *DataCheckpoint {
	return &DataCheckpoint{store: s, capture: capture}
}

// ReserveSnapshotSegment assigns a name under the writer gate. Only a successfully
// installed checkpoint registers its files in the store.
func (s *Store) ReserveSnapshotSegment() (*DataSegment, error) {
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
		if !exists || loc.FileID != old.Segment.ID || loc.RecordOffset != old.RecordOffset || loc.Sequence != old.Sequence {
			return fmt.Errorf("%w: checkpoint index location disagrees with replay", ErrCorrupt)
		}
		return nil
	})
}

// writeSnapshot owns private outputs through the complete source scan. Both
// normal replay and recovery use the same rollover, sync and relocation path.
func (c *DataCheckpoint) writeSnapshot(ctx context.Context, moves *Relocations, reserve func() (*DataSegment, error), scan func(func(string, Location) (io.Writer, error)) error) error {
	return c.writeSnapshotLayout(ctx, moves, reserve, false, func(emit func(string, Location) (io.Writer, error), _ func(format.CatalogEntry) error) error {
		return scan(emit)
	})
}

type snapshotLayoutScan func(emit func(string, Location) (io.Writer, error), reuse func(format.CatalogEntry) error) error

func (c *DataCheckpoint) writeOrderedSnapshot(ctx context.Context, moves *Relocations, reserve func() (*DataSegment, error), scan snapshotLayoutScan) error {
	return c.writeSnapshotLayout(ctx, moves, reserve, true, scan)
}

func (c *DataCheckpoint) writeSnapshotLayout(ctx context.Context, moves *Relocations, reserve func() (*DataSegment, error), requireOrder bool, scan snapshotLayoutScan) (result error) {
	var segment *DataSegment
	var bounds snapshotBoundsBuilder
	var previousKey string
	var previousCarried format.CatalogEntry
	lastCarriedPos := -1
	ordered := true
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
		segment.Size.Store(offset)
		entry := format.CatalogEntry{ID: segment.ID, Bytes: offset, Kind: format.SnapshotKind, DataFileDigest: [32]byte(digest.Sum(nil))}
		c.entries = append(c.entries, entry)
		c.privateEntries = append(c.privateEntries, entry)
		if ordered {
			bounds.complete(segment, &c.store.fences)
		}
		bounds = snapshotBoundsBuilder{}
		return nil
	}
	defer func() {
		if fd != nil {
			result = errors.Join(result, fd.Close())
		}
	}()
	emit := func(key string, old Location) (io.Writer, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if previousCarried.ID != 0 {
			_, last, err := c.snapshotBounds(ctx, previousCarried)
			if err != nil {
				return nil, err
			}
			previousKey = last
			previousCarried = format.CatalogEntry{}
		}
		if ordered && previousKey >= key {
			if requireOrder {
				return nil, fmt.Errorf("%w: unordered checkpoint output", ErrCorrupt)
			}
			ordered = false
			previousKey = ""
		}
		previousOutputEnd := offset
		if fd == nil || offset > c.store.Config.TargetSegmentBytes-old.RecordBytes {
			previousOutputEnd = 0
			if err := finish(); err != nil {
				return nil, err
			}
			var err error
			segment, err = reserve()
			if err != nil {
				return nil, fault("output-create", publication.NotApplied, false, err)
			}
			fd, err = segment.create(c.capture.Current.UUID, fs, "compact.segment", appendData{})
			if err != nil {
				return nil, fault("output-create", publication.NotApplied, false, err)
			}
			c.segments = append(c.segments, segment)
			offset = format.SegmentHeaderBytes
			digest.Reset()
			digest.Write(format.EncodeSegmentHeader(format.SegmentHeader{UUID: c.capture.Current.UUID, ID: segment.ID, Kind: segment.Kind}))
		}
		if requireOrder {
			if err := c.scratch.growOutput(previousOutputEnd, offset+old.RecordBytes); err != nil {
				return nil, err
			}
		}
		if ordered {
			bounds.add(key, offset)
			previousKey = key
		}
		writer := &outputWriter{fs: fs, fd: fd, offset: offset, point: "compact.write", hash: digest}
		next := old
		next.Segment, next.RecordOffset = segment, offset
		offset += old.RecordBytes
		return writer, moves.append(key, old, next)
	}
	reuse := func(entry format.CatalogEntry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		src, pos, err := c.findSnapshot(entry)
		if err != nil {
			return err
		}
		if !requireOrder || !c.HasOrderedBase() || pos <= lastCarriedPos || src.Segment.snapshotRange == nil {
			return fmt.Errorf("%w: invalid snapshot reuse", ErrCorrupt)
		}
		if damage := src.Segment.UnavailableData(); damage != nil {
			return damage
		}
		if previousCarried.ID == 0 && previousKey != "" {
			first, _, err := c.snapshotBounds(ctx, entry)
			if err != nil {
				return err
			}
			if previousKey >= first {
				return fmt.Errorf("%w: overlapping checkpoint ranges", ErrCorrupt)
			}
		}
		if err := finish(); err != nil {
			return err
		}
		if c.reused == nil {
			c.reused = make(map[uint64]struct{})
		}
		c.reused[entry.ID] = struct{}{}
		c.entries = append(c.entries, entry)
		lastCarriedPos, previousCarried = pos, entry
		previousKey = ""
		return nil
	}
	if err := scan(emit, reuse); err != nil {
		return err
	}
	if err := finish(); err != nil {
		return err
	}
	c.ordered = ordered
	return moves.seal()
}

// Protect validates each staged snapshot against the digest computed during
// Copy. Sync all new sidecar names together before publishing their catalog.
func (c *DataCheckpoint) Protect(ctx context.Context) error {
	if len(c.privateEntries) == 0 {
		return nil
	}
	for i, entry := range c.privateEntries {
		src := CapturedSegment{Segment: c.segments[i], Entry: entry}
		if err := c.store.buildParity(ctx, c.capture.Current.UUID, src, false, nil, "compact.parity"); err != nil {
			return err
		}
	}
	if err := c.store.IO.SyncDir(c.store.Dir, "compact.parity.dirsync"); err != nil {
		return err
	}
	for _, f := range c.segments {
		f.parityReady, f.parityNeedsSync = true, false
	}
	return nil
}

// AdoptUnchangedOrdered records a verified process-local proof when no DATA
// history or address changed. The caller holds the writer gate and maintenance
// slot. Commits made after capture may have appended a tail to this same base.
func (c *DataCheckpoint) AdoptUnchangedOrdered() bool {
	s := c.store
	if !c.ordered || !c.HasOrderedBase() || len(c.segments) != 0 ||
		c.capture.Current.LastUserSequence != c.capture.Catalog.CheckpointCutoffSequence ||
		s.Catalog.CheckpointCutoffSequence != c.capture.Catalog.CheckpointCutoffSequence ||
		len(s.Catalog.Entries) < len(c.entries) || !slices.Equal(c.entries, c.capture.Catalog.Entries) ||
		!slices.Equal(c.entries, s.Catalog.Entries[:len(c.entries)]) {
		return false
	}
	s.orderedSnapshot = true
	return true
}

// Install splices the captured prefix into the current tail under the writer
// gate. A failed publication may have selected the snapshot files on disk.
func (c *DataCheckpoint) Install() error {
	s := c.store
	for _, f := range c.segments {
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
	for i, segment := range c.segments {
		segment.markSealed(catalog.UUID, c.privateEntries[i])
		s.Segments[segment.ID] = segment
	}
	s.orderedSnapshot = c.ordered
	// Rotations during the build can seal the captured active file as part
	// of the surviving tail. Only catalog entries in the captured prefix retire.
	for _, src := range c.capture.Segments {
		if src.Entry.ID != c.capture.Current.ActiveID {
			if _, retained := c.reused[src.Entry.ID]; !retained {
				s.retireData(src.Segment)
			}
		}
	}
	return nil
}

// Discard removes only privately owned files before publication was attempted.
// Installation transfers cleanup to the store and its recovery protocol.
func (c *DataCheckpoint) Discard() error {
	if c.attempted || len(c.segments) == 0 {
		return nil
	}
	var result error
	for _, f := range c.segments {
		result = errors.Join(result, f.discardReplacement(c.store.IO, c.capture.Current.UUID, "compact.discard"))
	}
	return errors.Join(result, c.store.IO.SyncDir(c.store.Dir, "compact.discard.dirsync"))
}

// QueueFailedDiscard schedules only this attempt's unpublished outputs. The
// caller holds the writer gate. Some unlinks may already have succeeded, so
// retry the directory sync even when every private name is gone.
func (c *DataCheckpoint) QueueFailedDiscard() {
	if c.attempted || len(c.segments) == 0 {
		return
	}
	for _, f := range c.segments {
		c.store.retireData(f)
	}
	c.store.retireSyncPending = true
}
