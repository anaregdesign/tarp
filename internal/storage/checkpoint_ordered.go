package storage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

// The sorter charges keys and addresses. A separate window charges buffered
// encoded records, their keys and descriptors. A larger record streams alone.
const orderedWindowBytes = 4 << 20

// CopyByKey builds a checkpoint in strict encoded-key order. A nil view is
// permitted only when the capture already certifies the snapshot base.
func (c *DataCheckpoint) CopyByKey(ctx context.Context, live index.View, count int, moves *Relocations, reserve func() (*DataSegment, error)) (result error) {
	if err := c.prepareOrderedScratch(moves); err != nil {
		return err
	}
	base := c.HasOrderedBase()
	if !base && live == nil {
		return fmt.Errorf("%w: key-ordered delta requires a certified base", ErrCorrupt)
	}
	sources := make(map[uint64]CapturedSegment, len(c.capture.Segments))
	var snapshots []CapturedSegment
	for _, src := range c.capture.Segments {
		sources[src.Entry.ID] = src
		if src.Entry.Kind == format.SnapshotKind {
			snapshots = append(snapshots, src)
		}
	}
	// Forcing full validation of a known base (e.g. after a damaged read)
	// must visit every selected input, including distinct unaffected ranges.
	delta := base && live == nil
	if !base && len(snapshots) > 0 {
		var err error
		if delta, err = c.certifyCapturedBase(ctx, live); err != nil {
			return err
		}
		if delta && c.capture.Current.LastUserSequence == c.capture.Catalog.CheckpointCutoffSequence {
			// Certification checked each address against this same-cutoff view.
			live = nil
		}
	}
	dir, err := c.store.WorkspaceDir()
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	sorter := sortedReplay{ctx: ctx, dir: dir, limit: orderedWindowBytes, budget: c.scratch}
	defer func() { result = errors.Join(result, replayWorkError(sorter.close())) }()
	for _, src := range c.capture.Segments {
		if delta && src.Entry.Kind == format.SnapshotKind {
			continue
		}
		if err := c.scanByKey(ctx, src, func(off int64, r format.RecordHeader, key string) error {
			return sorter.apply(src.Segment, off, r, key)
		}); err != nil {
			return err
		}
	}
	return c.writeOrderedSnapshot(ctx, moves, reserve, func(emit func(string, Location) (io.Writer, error), reuse func(format.CatalogEntry) error) error {
		copy := orderedCopy{c: c, ctx: ctx, sources: sources, live: live, emit: emit, reuse: reuse, buf: make([]byte, 32<<10)}
		if delta {
			if err := copy.mergeDelta(&sorter, snapshots); err != nil {
				return err
			}
		} else {
			var emitErr error
			err := sorter.reduceEvents(c.capture.Catalog.PrunedThroughSequence, func(e replayEvent) error {
				emitErr = copy.add(e)
				return emitErr
			})
			if err != nil {
				if emitErr != nil {
					return emitErr
				}
				return replayWorkError(err)
			}
		}
		if copy.rows != int64(count) {
			return fmt.Errorf("%w: key-ordered checkpoint has %d keys, captured index has %d", ErrCorrupt, copy.rows, count)
		}
		return copy.flush()
	})
}

// orderedInput applies the same byte allowance used for output and parity to
// physical source reads. Keep the single ReadAt when pacing is disabled.
func (c *DataCheckpoint) orderedInput(src io.ReaderAt) io.ReaderAt {
	if c.orderedYield == nil {
		return src
	}
	return orderedInput{c: c, src: src}
}

type orderedInput struct {
	c   *DataCheckpoint
	src io.ReaderAt
}

func (r orderedInput) ReadAt(p []byte, off int64) (int, error) {
	var read int
	for len(p) != 0 {
		n := min(len(p), orderedPaceBytes)
		if err := r.c.paceOrdered(n); err != nil {
			// A scheduling failure occurred before reading authoritative bytes.
			// Cancellation and errors already classed as private retain their cause.
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

// Input errors must identify authoritative DATA even when permission or
// other non-repairable failures preclude a parity retry. Callers classify
// visitor, index and private-output failures separately.
func orderedSourceError(id uint64, err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var work *IndexWorkError
	if errors.As(err, &work) {
		return err
	}
	var source *SourceError
	if !errors.As(err, &source) {
		err = &SourceError{Err: err}
	}
	return selectedReadFailure(id, err)
}

func (c *DataCheckpoint) scanByKey(ctx context.Context, src CapturedSegment, visit func(int64, format.RecordHeader, string) error) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	fd, _, _, err := openSelected(src.Segment.Path, c.capture.Current.UUID, src.Entry, src.Entry.ID == c.capture.Current.ActiveID)
	if err != nil {
		return orderedSourceError(src.Entry.ID, err)
	}
	defer func() { result = errors.Join(result, orderedSourceError(src.Entry.ID, fd.Close())) }()
	var visitorErr error
	err = c.store.IO.Do("compact.scan", func() error {
		return scanSelectedRecords(ctx, c.orderedInput(fd), src.Entry, c.capture.Catalog.CheckpointCutoffSequence, func(off int64, r format.RecordHeader, key string) error {
			visitorErr = visit(off, r, key)
			return visitorErr
		})
	})
	if visitorErr != nil {
		return visitorErr
	}
	return orderedSourceError(src.Entry.ID, err)
}

type orderedWinner struct {
	payloadCRC uint32
	key        string
	loc        Location
	data       []byte
}

type orderedCopy struct {
	c             *DataCheckpoint
	ctx           context.Context
	sources       map[uint64]CapturedSegment
	live          index.View
	emit          func(string, Location) (io.Writer, error)
	reuse         func(format.CatalogEntry) error
	rows          int64
	window        []orderedWinner
	used, payload int
	buf, arena    []byte
	order         []int
}

func (w *orderedCopy) verify(key string, addr index.Address) error {
	if w.live == nil {
		return nil
	}
	got, ok, err := w.live.Lookup(key)
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	if !ok || got != addr {
		return fmt.Errorf("%w: captured index disagrees with ordered checkpoint for DATA %d offset %d", ErrCorrupt, addr.FileID, addr.RecordOffset)
	}
	return nil
}

func (w *orderedCopy) add(e replayEvent) error {
	key, addr := e.key, e.addr
	if err := w.verify(key, addr); err != nil {
		return err
	}
	src, ok := w.sources[addr.FileID]
	if !ok || addr.RecordOffset < format.SegmentHeaderBytes || addr.RecordBytes < format.RecordHeaderBytes+1 ||
		addr.RecordOffset > src.Entry.Bytes-addr.RecordBytes || addr.ValueBytes < 0 ||
		addr.RecordBytes-format.RecordHeaderBytes-addr.ValueBytes != int64(len(key)) {
		return &IndexWorkError{Err: errors.New("invalid winning address in checkpoint workspace")}
	}
	row := orderedWinner{key: key, loc: At(src.Segment, addr), payloadCRC: e.payloadCRC}
	const descriptorCharge = 128
	if addr.RecordBytes > int64(orderedWindowBytes-descriptorCharge)-int64(len(key)) {
		if err := w.flush(); err != nil {
			return err
		}
		dest, err := w.emit(key, row.loc)
		if err != nil {
			return err
		}
		if err := w.readOne(src, &row, dest); err != nil {
			return err
		}
		if err := w.c.store.IO.Check("compact.record:after"); err != nil {
			return err
		}
		w.rows++
		return nil
	}
	charge := int(addr.RecordBytes) + len(key) + descriptorCharge
	if charge > orderedWindowBytes-w.used {
		if err := w.flush(); err != nil {
			return err
		}
	}
	w.window = append(w.window, row)
	w.payload += int(addr.RecordBytes)
	w.used += charge
	w.rows++
	return nil
}

func (w *orderedCopy) openSource(src CapturedSegment) (*os.File, error) {
	fd, _, _, err := openSelected(src.Segment.Path, w.c.capture.Current.UUID, src.Entry, src.Entry.ID == w.c.capture.Current.ActiveID)
	if err == nil {
		if err = w.c.store.IO.Check("compact.source.open:after"); err == nil {
			return fd, nil
		}
		err = errors.Join(err, fd.Close())
	}
	return nil, orderedSourceError(src.Entry.ID, err)
}

func (w *orderedCopy) readOne(src CapturedSegment, row *orderedWinner, dst io.Writer) (result error) {
	fd, err := w.openSource(src)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, orderedSourceError(src.Entry.ID, fd.Close())) }()
	return w.readRecord(fd, row, dst)
}

func (w *orderedCopy) readRecord(fd *os.File, row *orderedWinner, dst io.Writer) error {
	a := row.loc
	rec, err := format.CopyRecord(w.ctx, w.c.orderedInput(fd), dst, a.RecordOffset, a.RecordBytes, &format.Identity{Key: row.key, Sequence: a.Sequence}, w.buf)
	if err == nil && (rec.EncodedRecordBytes() != a.RecordBytes || rec.ValueBytes != a.ValueBytes || rec.PayloadCRC != row.payloadCRC) {
		err = fmt.Errorf("%w: ordered record changed after validation", ErrCorrupt)
	}
	var destination *destinationError
	if errors.As(err, &destination) {
		return err
	}
	return orderedSourceError(a.Segment.ID, err)
}

func (w *orderedCopy) flush() (result error) {
	if len(w.window) == 0 {
		return nil
	}
	w.arena = slices.Grow(w.arena[:0], w.payload)[:w.payload]
	w.order = slices.Grow(w.order[:0], len(w.window))[:len(w.window)]
	for i := range w.order {
		w.order[i] = i
	}
	slices.SortFunc(w.order, func(i, j int) int {
		a, b := w.window[i].loc, w.window[j].loc
		if id := cmp.Compare(a.Segment.ID, b.Segment.ID); id != 0 {
			return id
		}
		return cmp.Compare(a.RecordOffset, b.RecordOffset)
	})
	var fd *os.File
	var input io.ReaderAt
	var fileID uint64
	defer func() {
		if fd != nil {
			result = errors.Join(result, orderedSourceError(fileID, fd.Close()))
		}
	}()
	// Consecutive addresses share one file read into the window. The arena
	// follows physical read order; each row's slice follows that row when
	// emitting the key-ordered output.
	offset := 0
	for i := 0; i < len(w.order); {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		row := &w.window[w.order[i]]
		if id := row.loc.Segment.ID; fileID != id {
			if fd != nil {
				err := orderedSourceError(fileID, fd.Close())
				fd = nil
				if err != nil {
					return err
				}
			}
			src := w.sources[id]
			var err error
			fd, err = w.openSource(src)
			if err != nil {
				return err
			}
			input = w.c.orderedInput(fd)
			fileID = id
		}
		start := offset
		fileStart := row.loc.RecordOffset
		for i < len(w.order) {
			r := &w.window[w.order[i]]
			if r.loc.Segment.ID != fileID || r.loc.RecordOffset-fileStart != int64(offset-start) {
				break
			}
			end := offset + int(r.loc.RecordBytes)
			r.data = w.arena[offset:end:end]
			offset = end
			i++
		}
		b := w.arena[start:offset]
		n, err := input.ReadAt(b, fileStart)
		if err != nil && !(err == io.EOF && n == len(b)) {
			return orderedSourceError(fileID, err)
		}
		if n != len(b) {
			return orderedSourceError(fileID, io.ErrUnexpectedEOF)
		}
	}
	for _, row := range w.window {
		sum, err := format.ValidateIndexedRecord(row.data, []byte(row.key), row.loc.Sequence)
		if err == nil && sum != row.payloadCRC {
			err = fmt.Errorf("%w: ordered record changed after validation", ErrCorrupt)
		}
		if err != nil {
			return orderedSourceError(row.loc.Segment.ID, err)
		}
		dst, err := w.emit(row.key, row.loc)
		if err != nil {
			return err
		}
		if _, err = dst.Write(row.data); err != nil {
			return err
		}
		if err = w.c.store.IO.Check("compact.record:after"); err != nil {
			return err
		}
	}
	clear(w.window)
	w.window = w.window[:0]
	w.used, w.payload = 0, 0
	return nil
}

type orderedChange struct {
	key string
	// Only a group's first Delete can depend on a Put in the base. The
	// remaining transitions have a known predecessor from this same stream.
	first, last replayEvent
}

// changes reduces even millions of overwrites of one key to constant state.
func changes(s *sortedReplay, pruned uint64) iter.Seq2[orderedChange, error] {
	return func(yield func(orderedChange, error) bool) {
		var group orderedChange
		stop := errors.New("stop ordered changes")
		err := s.each(func(e replayEvent) error {
			if e.key != group.key {
				if group.key != "" && !yield(group, nil) {
					return stop
				}
				group = orderedChange{key: e.key, first: e}
			} else if err := validateKey(uint32(e.fileKind), format.RecordHeader{Kind: e.recordKind, Sequence: e.addr.Sequence}, group.last.recordKind == format.Put, pruned); err != nil {
				return selectedReadFailure(e.addr.FileID, err)
			}
			group.last = e
			return nil
		})
		if err != nil {
			if err != stop {
				yield(orderedChange{}, replayWorkError(err))
			}
			return
		}
		if group.key != "" {
			yield(group, nil)
		}
	}
}

type changeCursor struct {
	pull   func() (orderedChange, error, bool)
	change orderedChange
	pruned uint64
}

func (r *changeCursor) advance() error {
	var err error
	r.change, err, _ = r.pull()
	return err
}

func (r *changeCursor) take(base bool) (replayEvent, bool, error) {
	g := r.change
	if err := validateKey(uint32(g.first.fileKind), format.RecordHeader{Kind: g.first.recordKind, Sequence: g.first.addr.Sequence}, base, r.pruned); err != nil {
		return replayEvent{}, false, selectedReadFailure(g.first.addr.FileID, err)
	}
	return g.last, g.last.recordKind == format.Put, r.advance()
}

func (w *orderedCopy) mergeDelta(s *sortedReplay, base []CapturedSegment) error {
	pruned := w.c.capture.Catalog.PrunedThroughSequence
	pull, stop := iter.Pull2(changes(s, pruned))
	defer stop()
	r := changeCursor{pull: pull, pruned: pruned}
	if err := r.advance(); err != nil {
		return err
	}
	for len(base) > 0 && r.change.key != "" {
		// Find the first segment whose last key can reach this change. Reuse
		// the skipped intervals directly without probing each file's fences.
		lo, hi := 0, len(base)
		for lo < hi {
			mid := lo + (hi-lo)/2
			_, last, err := w.c.snapshotBounds(w.ctx, base[mid].Entry)
			if err != nil {
				return err
			}
			if last < r.change.key {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		if err := w.carry(base[:lo]); err != nil {
			return err
		}
		base = base[lo:]
		if len(base) == 0 {
			break
		}
		first, last, err := w.c.snapshotBounds(w.ctx, base[0].Entry)
		if err != nil {
			return err
		}
		if err := w.gap(&r, first); err != nil {
			return err
		}
		if r.change.key == "" {
			break
		}
		if r.change.key > last {
			continue
		}
		if err := w.mergeFile(s.dir, base[0], first, last, &r); err != nil {
			return err
		}
		base = base[1:]
	}
	if err := w.gap(&r, ""); err != nil {
		return err
	}
	return w.carry(base)
}

func (w *orderedCopy) gap(r *changeCursor, upper string) error {
	for k := r.change.key; k != "" && (upper == "" || k < upper); k = r.change.key {
		addr, put, err := r.take(false)
		if err != nil {
			return err
		}
		if put {
			if err := w.add(addr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *orderedCopy) carry(base []CapturedSegment) error {
	if len(base) == 0 {
		return nil
	}
	if err := w.flush(); err != nil {
		return err
	}
	for _, src := range base {
		if err := w.ctx.Err(); err != nil {
			return err
		}
		if w.live != nil {
			// Bootstrap adoption must independently check the captured index,
			// including files with no mutations. Certified delta calls omit this.
			if err := w.c.scanByKey(w.ctx, src, func(off int64, h format.RecordHeader, key string) error {
				return w.verify(key, Location{Segment: src.Segment, RecordOffset: off, RecordBytes: h.EncodedRecordBytes(), ValueBytes: h.ValueBytes, Sequence: h.Sequence}.Address())
			}); err != nil {
				return err
			}
		}
		if err := w.reuse(src.Entry); err != nil {
			return err
		}
		w.rows += w.c.snapshotRows(src)
	}
	return nil
}

// Delay copying the unmodified leading rows until the first actual difference.
// A checked, spillable address prefix makes an insert-then-delete inside a
// file eligible for reuse, without copying its values into private DATA.
func (w *orderedCopy) mergeFile(dir string, src CapturedSegment, first, last string, r *changeCursor) (result error) {
	// The base prefix is already ordered, so a spilled prefix appends to
	// one run and requires no merge.
	prefix := sortedReplay{ctx: w.ctx, dir: dir, limit: moveBatchBytes, budget: w.c.scratch}
	defer func() { result = errors.Join(result, replayWorkError(prefix.close())) }()
	changed := false
	startCopy := func() error {
		if changed {
			return nil
		}
		changed = true
		var visitErr error
		err := prefix.each(func(e replayEvent) error {
			visitErr = w.add(e)
			return visitErr
		})
		if visitErr != nil {
			return visitErr
		}
		return replayWorkError(err)
	}
	var previous string
	var rows int64
	err := w.c.scanByKey(w.ctx, src, func(off int64, h format.RecordHeader, key string) error {
		if key <= previous || rows == 0 && key != first || key > last {
			return selectedReadFailure(src.Entry.ID, fmt.Errorf("%w: unordered certified snapshot", ErrCorrupt))
		}
		previous = key
		rows++
		for k := r.change.key; k != "" && k < key; k = r.change.key {
			a, put, err := r.take(false)
			if err != nil {
				return err
			}
			if put {
				if err := startCopy(); err != nil {
					return err
				}
				if err := w.add(a); err != nil {
					return err
				}
			}
		}
		if r.change.key == key {
			a, put, err := r.take(true)
			if err != nil {
				return err
			}
			if err := startCopy(); err != nil {
				return err
			}
			if put {
				return w.add(a)
			}
			return nil
		}
		loc := Location{Segment: src.Segment, RecordOffset: off, RecordBytes: h.EncodedRecordBytes(), ValueBytes: h.ValueBytes, Sequence: h.Sequence}
		row := replayEvent{key: key, addr: loc.Address(), payloadCRC: h.PayloadCRC, fileKind: format.SnapshotKind, recordKind: format.Put}
		if changed {
			return w.add(row)
		}
		if err := w.verify(key, loc.Address()); err != nil {
			return err
		}
		return replayWorkError(prefix.add(row))
	})
	if err != nil {
		return err
	}
	if previous != last || rows != w.c.snapshotRows(src) {
		return selectedReadFailure(src.Entry.ID, fmt.Errorf("%w: certified snapshot bounds or count", ErrCorrupt))
	}
	if changed {
		return nil
	}
	if err := w.flush(); err != nil {
		return err
	}
	w.rows += rows
	return w.reuse(src.Entry)
}
