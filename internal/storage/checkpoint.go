package storage

import (
	"bufio"
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
	yield          func() error
	bytesToYield   int
	scratch        *orderedScratchBudget
	capture        *Capture
	segments       []*DataSegment
	entries        []format.CatalogEntry // all selected outputs, including reused snapshots
	privateEntries []format.CatalogEntry // completed outputs owned by this attempt
	reused         map[uint64]int64      // DATA IDs and record counts of unchanged snapshots
	ordered        bool                  // globally ordered output, valid only after successful scan
	certifiedBase  bool
	ordinals       map[uint64]int
	attempted      bool
}

func (s *Store) NewDataCheckpoint(capture *Capture) *DataCheckpoint {
	return &DataCheckpoint{store: s, capture: capture}
}

// SetYield schedules pauses between chunks of DATA and PARITY I/O outside the
// writer gate.
func (c *DataCheckpoint) SetYield(yield func() error) {
	c.yield = yield
	c.bytesToYield = checkpointPaceBytes
}

const checkpointPaceBytes = 256 << 10

func (c *DataCheckpoint) pace(n int) error {
	c.bytesToYield -= n
	if c.bytesToYield > 0 {
		return nil
	}
	c.bytesToYield += checkpointPaceBytes
	return c.yield()
}

func (c *DataCheckpoint) paceInput(src io.ReaderAt) io.ReaderAt {
	if c.yield == nil {
		return src
	}
	return pacedInput{c: c, src: src}
}

type pacedInput struct {
	c   *DataCheckpoint
	src io.ReaderAt
}

func (r pacedInput) ReadAt(p []byte, off int64) (int, error) {
	var read int
	for len(p) != 0 {
		n := min(len(p), checkpointPaceBytes)
		if err := r.c.pace(n); err != nil {
			// A scheduling failure occurred before reading authoritative DATA.
			var work *IndexWorkError
			if errors.As(err, &work) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return read, err
			}
			return read, &IndexWorkError{Err: err}
		}
		got, err := r.src.ReadAt(p[:n], off)
		read += got
		if err != nil && !(err == io.EOF && got == n) {
			return read, err
		}
		if got != n {
			return read, io.ErrUnexpectedEOF
		}
		p, off = p[n:], off+int64(n)
	}
	return read, nil
}

func (c *DataCheckpoint) paceOutput(dst *outputWriter) io.Writer {
	if c.yield == nil {
		return dst
	}
	return checkpointOutput{dst, c}
}

type checkpointOutput struct {
	dst *outputWriter
	c   *DataCheckpoint
}

func (w checkpointOutput) Write(b []byte) (int, error) {
	var written int
	for len(b) != 0 {
		n := min(len(b), checkpointPaceBytes)
		if err := w.c.pace(n); err != nil {
			return written, &destinationError{err}
		}
		count, err := w.dst.Write(b[:n])
		written += count
		if err != nil {
			return written, err
		}
		b = b[n:]
	}
	return written, nil
}

// ReserveSnapshotSegment assigns a name under the writer gate. Only a successfully
// installed checkpoint registers its files in the store.
func (s *Store) ReserveSnapshotSegment() (*DataSegment, error) {
	return s.reserveSegment(format.SnapshotKind)
}

// Entries describes completed snapshot files for checkpoint size accounting.
func (c *DataCheckpoint) Entries() []format.CatalogEntry { return c.entries }

// Copy validates the captured history and writes a snapshot in source order.
// Whole unchanged snapshots keep their addresses; moves contains only copies.
// Only reserve takes the writer gate.
func (c *DataCheckpoint) Copy(ctx context.Context, history index.Backend, live index.View, moves *Relocations, reserve func() (*DataSegment, error)) error {
	err := c.writeSnapshotLayout(ctx, moves, reserve, false, func(emit func(string, Location) (io.Writer, error), reuse reuseSnapshot) error {
		return c.replayLive(ctx, history, live, emit, reuse)
	})
	if err != nil {
		return err
	}
	// Verify the complete set against independently replayed DATA before the
	// new checkpoint is allowed to replace its sources. An inconsistent derived
	// index must never suppress the only surviving authoritative copy of a Put.
	recordCount := int64(moves.count)
	for _, kept := range c.reused {
		recordCount += kept
	}
	if recordCount != int64(history.Count()) {
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
		if !exists || loc != old.Address() {
			return fmt.Errorf("%w: checkpoint index location disagrees with replay", ErrCorrupt)
		}
		return nil
	})
}

// writeSnapshot owns private outputs through the complete source scan. Both
// normal replay and recovery use the same rollover, sync and relocation path.
func (c *DataCheckpoint) writeSnapshot(ctx context.Context, moves *Relocations, reserve func() (*DataSegment, error), scan func(func(string, Location) (io.Writer, error)) error) error {
	return c.writeSnapshotLayout(ctx, moves, reserve, false, func(emit func(string, Location) (io.Writer, error), _ reuseSnapshot) error {
		return scan(emit)
	})
}

// A source-order caller supplies bounds from its verified scan. Key-ordered
// delta reuse may instead use the existing ordered-base proof.
type reuseSnapshot func(format.CatalogEntry, *scannedSnapshot) error
type snapshotLayoutScan func(emit func(string, Location) (io.Writer, error), reuse reuseSnapshot) error

func (c *DataCheckpoint) writeOrderedSnapshot(ctx context.Context, moves *Relocations, reserve func() (*DataSegment, error), scan func(func(string, Location) (io.Writer, error), func(format.CatalogEntry) error) error) error {
	return c.writeSnapshotLayout(ctx, moves, reserve, true, func(emit func(string, Location) (io.Writer, error), reuse reuseSnapshot) error {
		return scan(emit, func(entry format.CatalogEntry) error { return reuse(entry, nil) })
	})
}

func (c *DataCheckpoint) writeSnapshotLayout(ctx context.Context, moves *Relocations, reserve func() (*DataSegment, error), requireOrder bool, scan snapshotLayoutScan) (result error) {
	var segment *DataSegment
	var bounds snapshotBoundsBuilder
	var previousKey string
	var previousCarried format.CatalogEntry
	lastCarriedPos := -1
	ordered := true
	var fd *os.File
	var buffered *bufio.Writer
	var offset int64
	digest := sha256.New()
	fs := c.store.IO
	finish := func() error {
		if fd == nil {
			return nil
		}
		if buffered != nil {
			if err := buffered.Flush(); err != nil {
				return err
			}
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
			fd, err = os.OpenFile(segment.Path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
			if err != nil {
				return nil, fault("output-create", publication.NotApplied, false, err)
			}
			c.segments = append(c.segments, segment)
			if err := segment.initializeFile(fd, c.capture.Current.UUID, fs, "compact.segment", appendData{}); err != nil {
				return nil, fault("output-create", publication.NotApplied, false, err)
			}
			offset = format.SegmentHeaderBytes
			digest.Reset()
			digest.Write(format.EncodeSegmentHeader(format.SegmentHeader{UUID: c.capture.Current.UUID, ID: segment.ID, Kind: segment.Kind}))
			if requireOrder {
				// Key-ordered records are emitted consecutively. Amortize small
				// physical writes while retaining the exact record offsets.
				disk := &outputWriter{fs: fs, fd: fd, offset: offset, point: "compact.write", hash: digest}
				out := c.paceOutput(disk)
				if buffered == nil {
					buffered = bufio.NewWriterSize(out, 32<<10)
				} else {
					buffered.Reset(out)
				}
			}
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
		var writer io.Writer = buffered
		if buffered == nil {
			writer = c.paceOutput(&outputWriter{fs: fs, fd: fd, offset: offset, point: "compact.write", hash: digest})
		}
		next := old
		next.Segment, next.RecordOffset = segment, offset
		offset += old.RecordBytes
		return writer, moves.append(key, old, next)
	}
	reuse := func(entry format.CatalogEntry, checked *scannedSnapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		src, pos, err := c.findSnapshot(entry)
		if err != nil {
			return err
		}
		if pos <= lastCarriedPos || requireOrder && (checked != nil || !c.HasOrderedBase() || src.Segment.snapshotRange == nil) ||
			!requireOrder && (checked == nil || checked.recordCount == 0) {
			return fmt.Errorf("%w: invalid snapshot reuse", ErrCorrupt)
		}
		if damage := src.Segment.UnavailableData(); damage != nil {
			return damage
		}
		if requireOrder && previousCarried.ID == 0 && previousKey != "" {
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
		var recordCount int64
		if requireOrder {
			recordCount = src.Segment.snapshotRange.recordCount
			previousCarried, previousKey = entry, ""
		} else {
			recordCount = checked.recordCount
			if ordered && !checked.unordered && previousKey < checked.first {
				if src.Segment.snapshotRange == nil {
					checked.complete(src.Segment, &c.store.fences)
				}
				previousKey = checked.last
			} else {
				ordered, previousKey = false, ""
			}
		}
		if c.reused == nil {
			c.reused = make(map[uint64]int64)
		}
		c.reused[entry.ID] = recordCount
		c.entries = append(c.entries, entry)
		lastCarriedPos = pos
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
	var yield func() error
	if c.yield != nil {
		yield = func() error { return c.pace(format.DataBlocksPerStripe * format.DataBlockBytes) }
	}
	for i, entry := range c.privateEntries {
		src := CapturedSegment{Segment: c.segments[i], Entry: entry}
		if err := c.store.buildParity(ctx, c.capture.Current.UUID, src, false, yield, "compact.parity"); err != nil {
			return err
		}
	}
	if err := c.store.IO.SyncDir(c.store.Dir, "compact.parity.dirsync"); err != nil {
		return err
	}
	for _, f := range c.segments {
		f.parityNeedsSync = false
		f.parityReady.Store(true)
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
		s.catalogEntries.Len() < len(c.entries) || !slices.Equal(c.entries, c.capture.Catalog.Entries) ||
		!slices.Equal(c.entries, s.catalogEntries.Slice(0, len(c.entries))) {
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
		if !f.ParityReady() {
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
	catalog.Entries = append(catalog.Entries, s.catalogEntries.Slice(len(c.capture.Catalog.Entries), s.catalogEntries.Len())...)
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
