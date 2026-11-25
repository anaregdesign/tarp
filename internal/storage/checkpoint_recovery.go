package storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/platform"
)

// CopyRecovered runs only after the normal checkpoint encountered unavailable
// sealed DATA. Healthy inputs still require strict file validation. A private
// partial replay checks the pinned view against all readable history.
func (c *DataCheckpoint) CopyRecovered(ctx context.Context, history index.Backend, live index.View, moves, lost *Relocations, reserve func() (*DataSegment, error), cached func(string, Location) ([]byte, bool, error)) error {
	return c.copyRecovered(ctx, history, live, moves, lost, reserve, cached, false)
}

// CopyRecoveredByKey stages recovered bytes while their verified source is
// available. In particular a cached value can cease to be available as soon
// as a concurrent writer publishes its next version.
func (c *DataCheckpoint) CopyRecoveredByKey(ctx context.Context, history index.Backend, live index.View, moves, lost *Relocations, reserve func() (*DataSegment, error), cached func(string, Location) ([]byte, bool, error)) error {
	return c.copyRecovered(ctx, history, live, moves, lost, reserve, cached, true)
}

func (c *DataCheckpoint) copyRecovered(ctx context.Context, history index.Backend, live index.View, moves, lost *Relocations, reserve func() (*DataSegment, error), cached func(string, Location) ([]byte, bool, error), ordered bool) error {
	r := checkpointRecovery{c: c, ctx: ctx, live: live, lost: lost, cached: cached, buf: make([]byte, 32<<10)}
	// A readable Delete may have its predecessor in an unreadable region.
	r.history = newReplayBuilder(ctx, history, c.capture.Current.LastUserSequence)
	defer r.history.abort()
	if ordered {
		if err := c.prepareOrderedScratch(moves, lost); err != nil {
			return err
		}
		if r.history.sort != nil {
			r.history.sort.budget = c.scratch
		}
	}
	scan := func(emit func(string, Location) (io.Writer, error)) error {
		r.emit = emit
		for _, src := range c.capture.Segments {
			if err := ctx.Err(); err != nil {
				return err
			}
			r.keys, r.bytes = 0, 0
			if err := c.store.IO.Do("compact.recover.scan", func() error { return r.file(src) }); err != nil {
				return err
			}
			if r.keys != src.liveKeys || r.bytes != src.liveBytes {
				return fmt.Errorf("%w: recovered index totals disagree for DATA %d", ErrCorrupt, src.Entry.ID)
			}
		}
		return nil
	}
	var err error
	if ordered {
		err = c.writeRecoveredByKey(ctx, live, moves, reserve, scan)
	} else {
		err = c.writeSnapshot(ctx, moves, reserve, scan)
	}
	if err != nil {
		return err
	}
	if err := lost.seal(); err != nil {
		return err
	}
	if err := r.history.finishWith(func(key string, a index.Address) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		latest, ok, err := live.Lookup(key)
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		// A surviving Put absent from the view must have a later Delete.
		// If that Delete is unreadable, the pinned view is the evidence of
		// absence. A readable newer version always rules out a stale address.
		if !ok && a.Sequence > r.possibleDelete || ok && (latest.Sequence < a.Sequence || latest.Sequence == a.Sequence && latest != a) {
			return fmt.Errorf("%w: captured index disagrees with surviving history", ErrCorrupt)
		}
		return nil
	}); err != nil {
		return err
	}
	count := 0
	if err := live.All(func(_ string, _ index.Address) bool { count++; return ctx.Err() == nil }); err != nil {
		return &IndexWorkError{Err: err}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if count != moves.count+lost.count {
		return fmt.Errorf("%w: recovered index count disagrees with capture", ErrCorrupt)
	}
	return nil
}

// Recovery can supply bytes from reconstructed blocks, the original DATA,
// or the value cache. The scratch offset indexes that verified copy. The
// captured view supplies the original DATA address for conditional relocation.
func (c *DataCheckpoint) writeRecoveredByKey(ctx context.Context, live index.View, moves *Relocations, reserve func() (*DataSegment, error), scan func(func(string, Location) (io.Writer, error)) error) (result error) {
	dir, err := c.store.WorkspaceDir()
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	stage, err := platform.OpenScratch(dir, "tarp-checkpoint-recover-*")
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	runs := sortedReplay{ctx: ctx, dir: dir, limit: verifyWindowBytes, budget: c.scratch}
	var total int64
	defer func() { c.scratch.release(uint64(total)) }()
	defer func() {
		result = errors.Join(result, replayWorkError(errors.Join(stage.Close(), runs.close())))
	}()
	w := bufio.NewWriterSize(stage, 32<<10)
	recordWriter := recoveryScratchWriter{writer: w, runs: &runs}
	err = scan(func(key string, old Location) (io.Writer, error) {
		if recordWriter.left != 0 {
			return nil, &IndexWorkError{Err: io.ErrUnexpectedEOF}
		}
		if old.RecordBytes > math.MaxInt64-total {
			return nil, &IndexWorkError{Err: errors.New("recovery scratch exceeds file offset limit")}
		}
		if err := c.scratch.grow(uint64(old.RecordBytes)); err != nil {
			return nil, &IndexWorkError{Err: err}
		}
		a := old.Address()
		a.RecordOffset = total
		total += old.RecordBytes
		recordWriter.event = replayEvent{key: key, addr: a}
		recordWriter.left = a.RecordBytes
		recordWriter.headerBytes = 0
		return &recordWriter, nil
	})
	if err != nil {
		return err
	}
	if recordWriter.left != 0 {
		return &IndexWorkError{Err: io.ErrUnexpectedEOF}
	}
	if err := w.Flush(); err != nil {
		return &IndexWorkError{Err: err}
	}
	segments := make(map[uint64]*DataSegment, len(c.capture.Segments))
	for _, src := range c.capture.Segments {
		segments[src.Segment.ID] = src.Segment
	}
	return c.writeOrderedSnapshot(ctx, moves, reserve, func(emit func(string, Location) (io.Writer, error), _ func(format.CatalogEntry) error) error {
		buf := make([]byte, 32<<10)
		var previous string
		var visitErr error
		err := runs.each(func(e replayEvent) (err error) {
			defer func() { visitErr = err }()
			a, ok, err := live.Lookup(e.key)
			if err != nil {
				return &IndexWorkError{Err: err}
			}
			if !ok || e.key <= previous || a.FileID != e.addr.FileID || a.Sequence != e.addr.Sequence || a.RecordBytes != e.addr.RecordBytes || a.ValueBytes != e.addr.ValueBytes || segments[a.FileID] == nil {
				return &IndexWorkError{Err: errors.New("recovery scratch disagrees with captured record")}
			}
			previous = e.key
			dst, err := emit(e.key, At(segments[a.FileID], a))
			if err != nil {
				return err
			}
			rec, err := format.CopyRecord(ctx, stage, dst, e.addr.RecordOffset, a.RecordBytes, &format.Identity{Key: e.key, Sequence: a.Sequence}, buf)
			if err != nil {
				var output *destinationError
				if errors.As(err, &output) {
					return err
				}
				return replayWorkError(err)
			}
			if rec.EncodedRecordBytes() != a.RecordBytes || rec.ValueBytes != a.ValueBytes || rec.PayloadCRC != e.payloadCRC {
				return &IndexWorkError{Err: errors.New("recovery scratch record differs from staged Put")}
			}
			return nil
		})
		if visitErr != nil {
			return visitErr
		}
		return replayWorkError(err)
	})
}

// Capture the original payload checksum from the staged header, independently
// of the bytes later returned by the scratch file. Some recovered values are
// supplied by the immutable cache instead of by CopyRecord.
type recoveryScratchWriter struct {
	writer      io.Writer
	runs        *sortedReplay
	event       replayEvent
	header      [format.RecordHeaderBytes]byte
	headerBytes int
	left        int64
}

func (w *recoveryScratchWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > w.left {
		return 0, &IndexWorkError{Err: errors.New("recovery staging exceeds record length")}
	}
	n, err := w.writer.Write(p)
	if err != nil {
		return n, replayWorkError(err)
	}
	if n != len(p) {
		return n, &IndexWorkError{Err: io.ErrShortWrite}
	}
	w.headerBytes += copy(w.header[w.headerBytes:], p)
	w.left -= int64(n)
	if w.left == 0 {
		r, err := format.DecodeRecordHeader(w.header[:], w.event.addr.RecordBytes)
		if err != nil {
			return n, replayWorkError(err)
		}
		w.event.payloadCRC = r.PayloadCRC
		return n, replayWorkError(w.runs.add(w.event))
	}
	return n, nil
}

type checkpointRecovery struct {
	c              *DataCheckpoint
	ctx            context.Context
	live           index.View
	lost           *Relocations
	history        replayBuilder
	cached         func(string, Location) ([]byte, bool, error)
	emit           func(string, Location) (io.Writer, error)
	buf            []byte
	keys, bytes    int64
	possibleDelete uint64 // latest sequence of an unreadable potential tombstone
}

func (r *checkpointRecovery) file(src CapturedSegment) (result error) {
	c := r.c
	open := func() (*os.File, error) {
		fd, _, _, err := openSelected(src.Segment.Path, c.capture.Current.UUID, src.Entry, src.Entry.ID == c.capture.Current.ActiveID)
		if err != nil {
			return nil, err
		}
		if src.Entry.DataFileDigest != ([32]byte{}) {
			if err := verifySelectedDigest(r.ctx, fd, src.Entry); err != nil {
				return nil, errors.Join(err, fd.Close())
			}
		}
		return fd, nil
	}
	fd, err := open()
	if err != nil && src.Entry.DataFileDigest != ([32]byte{}) {
		// Additional damaged files may have been beyond the first failure
		// of the strict pass. Try complete repair independently for each.
		err = c.store.RepairRead(r.ctx, src.Segment, err)
		if IsDataUnavailable(err) && !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.EDQUOT) {
			repair := src.Segment.sealed.Load()
			repairGeneration := repair.repairGeneration.Load()
			stage, stageErr := c.stageBlocks(r.ctx, src)
			if stage != nil {
				defer func() { result = errors.Join(result, stage.Close()) }()
			}
			if stageErr != nil {
				if IsDataUnavailable(stageErr) {
					// Keep the original DATA failure as well as the reason a
					// fresh PARITY pass could not recover the captured file.
					return errors.Join(err, stageErr)
				}
				return stageErr
			}
			if err := r.scanDamaged(src, stage); err != nil {
				return err
			}
			before := r.lost.count
			if err := r.copyDamaged(src, stage); err != nil {
				return err
			}
			if before != r.lost.count {
				c.lossSources = append(c.lossSources, lossSource{repair, repairGeneration})
			}
			return nil
		}
		if err == nil {
			fd, err = open()
		}
	}
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, fd.Close()) }()
	return scanSelectedRecords(r.ctx, fd, src.Entry, c.capture.Catalog.CheckpointCutoffSequence, func(off int64, rec format.RecordHeader, key string) error {
		a, ok, err := r.observe(src, off, rec, key)
		if err != nil || !ok {
			return err
		}
		if err := r.copyRecord(fd, key, At(src.Segment, a), rec.PayloadCRC); err != nil {
			return err
		}
		r.keys++
		r.bytes += a.RecordBytes
		return nil
	})
}

// observe checks every readable event, including historical Deletes. An
// independently verified later event must never be replaced by an older Put.
func (r *checkpointRecovery) observe(src CapturedSegment, off int64, rec format.RecordHeader, key string) (index.Address, bool, error) {
	a, ok, err := r.live.Lookup(key)
	if err != nil {
		return a, false, &IndexWorkError{Err: err}
	}
	want := Location{Segment: src.Segment, RecordOffset: off, RecordBytes: rec.EncodedRecordBytes(), ValueBytes: rec.ValueBytes, Sequence: rec.Sequence}
	if ok && (a.Sequence < rec.Sequence || a.Sequence == rec.Sequence && (a != want.Address() || rec.Kind != format.Put)) {
		return a, false, fmt.Errorf("%w: index superseded by captured DATA", ErrCorrupt)
	}
	return a, ok && a == want.Address(), r.history.apply(src.Segment, off, rec, key)
}

func (r *checkpointRecovery) copyRecord(rd io.ReaderAt, key string, loc Location, payloadCRC uint32) error {
	w, err := r.emit(key, loc)
	if err != nil {
		return err
	}
	rec, err := format.CopyRecord(r.ctx, rd, w, loc.RecordOffset, loc.RecordBytes, &format.Identity{Key: key, Sequence: loc.Sequence}, r.buf)
	if err == nil && (rec.EncodedRecordBytes() != loc.RecordBytes || rec.ValueBytes != loc.ValueBytes || rec.PayloadCRC != payloadCRC) {
		return fmt.Errorf("%w: copied record changed during recovery", ErrCorrupt)
	}
	return err
}

func (r *checkpointRecovery) scanDamaged(src CapturedSegment, b *recoveredBlocks) error {
	span := format.NewRecordSequenceValidator(src.Entry, r.c.capture.Catalog.CheckpointCutoffSequence)
	afterGap := false
	for off := int64(format.SegmentHeaderBytes); off < b.end; {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		var header [format.RecordHeaderBytes]byte
		n := min(int64(len(header)), b.end-off)
		if _, err := b.ReadAt(header[:n], off); err != nil {
			return err
		}
		rec, err := format.DecodeRecordHeader(header[:n], b.end-off)
		if err != nil {
			gap, e := b.overlapsGap(off, n)
			if e != nil {
				return e
			}
			if !gap {
				return err
			}
			next, err := b.nextAnchor(r.ctx, off)
			if err != nil {
				return err
			}
			if err := b.addRange(off, next, rangeUnparsed); err != nil {
				return err
			}
			off = next
			afterGap = true
			continue
		}
		if afterGap {
			if src.Entry.Kind != format.SnapshotKind {
				r.possibleDelete = max(r.possibleDelete, rec.Sequence-1)
			}
			err = span.AcceptAfterGap(rec)
		} else {
			err = span.Accept(rec)
		}
		if err != nil {
			return err
		}
		afterGap = false
		_, key, err := format.ReadRecordKey(r.ctx, io.NewSectionReader(b, off, rec.EncodedRecordBytes()), off, rec.EncodedRecordBytes(), r.buf)
		if err == nil {
			a, current, err := r.observe(src, off, rec, key)
			if err != nil {
				return err
			}
			if current {
				if err := r.copyRecord(b, key, At(src.Segment, a), rec.PayloadCRC); err != nil {
					return err
				}
				r.keys++
				r.bytes += a.RecordBytes
			}
		} else {
			if !errors.Is(err, ErrCorrupt) {
				return err
			}
			gap, e := b.overlapsGap(off, rec.EncodedRecordBytes())
			if e != nil {
				return e
			}
			if !gap {
				return err
			}
			var flags byte
			// The header has its own checksum. If parity also verifies
			// the key bytes, the event is known even when its value is lost.
			keyGap, e := b.overlapsGap(off+format.RecordHeaderBytes, rec.KeyBytes)
			if e != nil {
				return e
			}
			if !keyGap {
				if _, _, err := r.observe(src, off, rec, key); err != nil {
					return err
				}
			} else {
				// A damaged key can equal another key by accident. Require
				// an exact owner of the newer Put before retaining the
				// other key's older, independently verified version.
				current, ok, err := r.live.Lookup(key)
				if err != nil {
					return &IndexWorkError{Err: err}
				}
				if rec.Kind == format.Put && ok && current.Sequence < rec.Sequence {
					flags = rangeNeedsOwner
				}
				if rec.Kind == format.Delete {
					r.possibleDelete = max(r.possibleDelete, rec.Sequence)
				}
			}
			if err := b.addRange(off, off+rec.EncodedRecordBytes(), flags); err != nil {
				return err
			}
		}
		off += rec.EncodedRecordBytes()
	}
	if !afterGap {
		return span.Finish()
	}
	if src.Entry.Kind != format.SnapshotKind {
		r.possibleDelete = max(r.possibleDelete, src.Entry.LastSequence)
	}
	return nil
}

func (r *checkpointRecovery) copyDamaged(src CapturedSegment, b *recoveredBlocks) error {
	var callbackErr error
	err := r.live.All(func(key string, a index.Address) bool {
		if callbackErr = r.ctx.Err(); callbackErr != nil {
			return false
		}
		if a.FileID != src.Entry.ID {
			return true
		}
		if !validCheckpointAddress(key, a, src.Entry, r.c.capture.Catalog.CheckpointCutoffSequence) {
			callbackErr = fmt.Errorf("%w: invalid recovery address", ErrCorrupt)
			return false
		}
		marked, err := b.claimDamage(a.RecordOffset, a.RecordOffset+a.RecordBytes)
		if err != nil {
			callbackErr = err
			return false
		}
		if !marked {
			return true
		}
		callbackErr = r.recoverRecord(b, key, At(src.Segment, a))
		r.keys++
		r.bytes += a.RecordBytes
		return callbackErr == nil
	})
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	if callbackErr == nil && b.unowned != 0 {
		return &UnavailableDataError{FileID: src.Entry.ID, Err: fmt.Errorf("%w: captured address may precede an unreadable update", ErrCorrupt)}
	}
	return callbackErr
}

func (r *checkpointRecovery) recoverRecord(b *recoveredBlocks, key string, old Location) error {
	rec, actual, err := format.ReadRecordKey(r.ctx, io.NewSectionReader(b, old.RecordOffset, old.RecordBytes), old.RecordOffset, b.end-old.RecordOffset, r.buf)
	if err == nil {
		if rec.Kind != format.Put || actual != key || rec.Sequence != old.Sequence || rec.EncodedRecordBytes() != old.RecordBytes || rec.ValueBytes != old.ValueBytes {
			return fmt.Errorf("%w: recovery address disagrees with intact record", ErrCorrupt)
		}
		return r.copyRecord(b, key, old, rec.PayloadCRC)
	}
	if !errors.Is(err, ErrCorrupt) {
		return err
	}
	var header [format.RecordHeaderBytes]byte
	if _, err := b.ReadAt(header[:], old.RecordOffset); err != nil {
		return err
	}
	decoded, headerErr := format.DecodeRecordHeader(header[:], b.end-old.RecordOffset)
	gapSize := int64(format.RecordHeaderBytes)
	if headerErr == nil {
		if decoded.Kind != format.Put || decoded.Sequence != old.Sequence || decoded.EncodedRecordBytes() != old.RecordBytes || decoded.ValueBytes != old.ValueBytes {
			return fmt.Errorf("%w: recovery address disagrees with checked header", ErrCorrupt)
		}
		if actual != key {
			keyGap, e := b.overlapsGap(old.RecordOffset+format.RecordHeaderBytes, decoded.KeyBytes)
			if e != nil {
				return e
			}
			if !keyGap {
				return fmt.Errorf("%w: recovery address disagrees with checked key", ErrCorrupt)
			}
		}
		gapSize = old.RecordBytes
	}
	gap, e := b.overlapsGap(old.RecordOffset, gapSize)
	if e != nil {
		return e
	}
	if !gap {
		return err
	}
	if headerErr == nil && r.cached != nil {
		value, ok, err := r.cached(key, old)
		if err != nil {
			return err
		}
		if ok && int64(len(value)) == old.ValueBytes {
			var recovered [format.RecordHeaderBytes]byte
			format.EncodeRecordHeader(&recovered, format.Put, old.Sequence, []byte(key), value)
			if recovered == header {
				w, err := r.emit(key, old)
				if err != nil {
					return err
				}
				for _, part := range [][]byte{header[:], []byte(key), value} {
					if _, err := io.Copy(w, bytes.NewReader(part)); err != nil {
						return err
					}
				}
				return nil
			}
		}
	}
	return r.lost.append(key, old, old)
}
