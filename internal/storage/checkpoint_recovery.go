package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

// CopyRecovered runs only after the normal checkpoint encountered unavailable
// sealed DATA. Healthy inputs still require strict file validation. A private
// partial replay checks the pinned view against all readable history.
func (c *Checkpoint) CopyRecovered(ctx context.Context, history index.Backend, live index.View, moves, lost *Relocations, reserve func() (*DataSegment, error), cached func(string, Location) ([]byte, bool, error)) error {
	r := checkpointRecovery{c: c, ctx: ctx, live: live, lost: lost, cached: cached, buf: make([]byte, 32<<10)}
	// A readable Delete may have its predecessor in an unreadable region.
	r.history = newReplayBuilder(ctx, history, c.capture.Current.LastUserSequence)
	defer r.history.abort()
	err := c.writeSnapshot(ctx, moves, reserve, func(emit func(string, Location) (io.Writer, error)) error {
		r.emit = emit
		for _, src := range c.capture.Files {
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
	})
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

type checkpointRecovery struct {
	c              *Checkpoint
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

func (r *checkpointRecovery) file(src CapturedFile) (result error) {
	c := r.c
	open := func() (*os.File, error) {
		fd, _, _, err := openSelected(src.File.Path, c.capture.Current.UUID, src.Entry, src.Entry.ID == c.capture.Current.ActiveID)
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
		err = c.store.RepairRead(r.ctx, src.File, err)
		if IsDataUnavailable(err) && !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, syscall.EDQUOT) {
			repair := src.File.sealed.Load()
			generation := repair.generation.Load()
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
				c.lossSources = append(c.lossSources, lossSource{repair, generation})
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
		if err := r.copyRecord(fd, key, At(src.File, a)); err != nil {
			return err
		}
		r.keys++
		r.bytes += a.Bytes
		return nil
	})
}

// observe checks every readable event, including historical Deletes. An
// independently verified later event must never be replaced by an older Put.
func (r *checkpointRecovery) observe(src CapturedFile, off int64, rec format.RecordHeader, key string) (index.Address, bool, error) {
	a, ok, err := r.live.Lookup(key)
	if err != nil {
		return a, false, &IndexWorkError{Err: err}
	}
	want := Location{File: src.File, Offset: off, Bytes: rec.EncodedRecordBytes(), ValueBytes: rec.ValueBytes, Sequence: rec.Sequence}
	if ok && (a.Sequence < rec.Sequence || a.Sequence == rec.Sequence && (a != want.Address() || rec.Kind != format.Put)) {
		return a, false, fmt.Errorf("%w: index superseded by captured DATA", ErrCorrupt)
	}
	return a, ok && a == want.Address(), r.history.apply(src.File, off, rec, key)
}

func (r *checkpointRecovery) copyRecord(rd io.ReaderAt, key string, loc Location) error {
	w, err := r.emit(key, loc)
	if err != nil {
		return err
	}
	rec, err := format.CopyRecord(r.ctx, rd, w, loc.Offset, loc.Bytes, &format.Identity{Key: key, Sequence: loc.Sequence}, r.buf)
	if err == nil && (rec.EncodedRecordBytes() != loc.Bytes || rec.ValueBytes != loc.ValueBytes) {
		return fmt.Errorf("%w: copied record length changed", ErrCorrupt)
	}
	return err
}

func (r *checkpointRecovery) scanDamaged(src CapturedFile, b *recoveredBlocks) error {
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
				if err := r.copyRecord(b, key, At(src.File, a)); err != nil {
					return err
				}
				r.keys++
				r.bytes += a.Bytes
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

func (r *checkpointRecovery) copyDamaged(src CapturedFile, b *recoveredBlocks) error {
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
		marked, err := b.claimDamage(a.Offset, a.Offset+a.Bytes)
		if err != nil {
			callbackErr = err
			return false
		}
		if !marked {
			return true
		}
		callbackErr = r.recoverRecord(b, key, At(src.File, a))
		r.keys++
		r.bytes += a.Bytes
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
	rec, actual, err := format.ReadRecordKey(r.ctx, io.NewSectionReader(b, old.Offset, old.Bytes), old.Offset, b.end-old.Offset, r.buf)
	if err == nil {
		if rec.Kind != format.Put || actual != key || rec.Sequence != old.Sequence || rec.EncodedRecordBytes() != old.Bytes || rec.ValueBytes != old.ValueBytes {
			return fmt.Errorf("%w: recovery address disagrees with intact record", ErrCorrupt)
		}
		return r.copyRecord(b, key, old)
	}
	if !errors.Is(err, ErrCorrupt) {
		return err
	}
	var header [format.RecordHeaderBytes]byte
	if _, err := b.ReadAt(header[:], old.Offset); err != nil {
		return err
	}
	decoded, headerErr := format.DecodeRecordHeader(header[:], b.end-old.Offset)
	gapSize := int64(format.RecordHeaderBytes)
	if headerErr == nil {
		if decoded.Kind != format.Put || decoded.Sequence != old.Sequence || decoded.EncodedRecordBytes() != old.Bytes || decoded.ValueBytes != old.ValueBytes {
			return fmt.Errorf("%w: recovery address disagrees with checked header", ErrCorrupt)
		}
		if actual != key {
			keyGap, e := b.overlapsGap(old.Offset+format.RecordHeaderBytes, decoded.KeyBytes)
			if e != nil {
				return e
			}
			if !keyGap {
				return fmt.Errorf("%w: recovery address disagrees with checked key", ErrCorrupt)
			}
		}
		gapSize = old.Bytes
	}
	gap, e := b.overlapsGap(old.Offset, gapSize)
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
