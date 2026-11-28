package engine

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"slices"
	"time"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/storage"
)

const (
	MaxStagedApplyEntries  = 1 << 20
	MaxStagedApplyBytes    = 64 << 30
	MaxStagedApplySegments = storage.MaxStagedAppendSegments
	stagedApplyOp          = "apply staged"
)

func (db *DB) ApplyStaged(ctx context.Context, mutations iter.Seq2[MixedOp, error]) (result error) {
	if ctx == nil {
		return validContextError(stagedApplyOp)
	}
	if err := ctx.Err(); err != nil {
		return operationError(stagedApplyOp, "context", err)
	}
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if mutations == nil {
		return nil
	}
	if db.opts.IndexMode != IndexBTree {
		return operationError(stagedApplyOp, "index", fmt.Errorf("%w: ApplyStaged requires IndexBTree", ErrInvalidOptions))
	}
	if !db.staging.CompareAndSwap(false, true) {
		return operationError(stagedApplyOp, "admission", ErrBusy)
	}
	defer db.staging.Store(false)
	input := &stagedInput{}
	defer func() {
		if input.file == nil {
			return
		}
		if err := input.file.Close(); err != nil {
			result = stagedCleanup(result, input.committed, err)
		}
	}()
	if err := db.prepareStagedInput(ctx, input, mutations); err != nil {
		return &OpError{Operation: stagedApplyOp, Phase: "input", Outcome: NotApplied, Err: publicCause(err)}
	}
	if input.count == 0 {
		return nil
	}
	return db.commitStagedInput(ctx, input)
}

type stagedInput struct {
	file      *os.File
	count     int
	bytes     uint64
	committed bool
}

func (db *DB) prepareStagedInput(ctx context.Context, input *stagedInput, mutations iter.Seq2[MixedOp, error]) error {
	var previous []byte
	var writer *bufio.Writer
	var allowance uint64
	for change, err := range mutations {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		position := input.count
		if err := CheckBatchCount(position+1, MaxStagedApplyEntries); err != nil {
			return err
		}
		if err := CheckBatchKey("ApplyStaged", position, change.Key); err != nil {
			return err
		}
		if position != 0 && bytes.Compare(previous, change.Key) >= 0 {
			return fmt.Errorf("tarp: ApplyStaged input[%d] keys must be strictly increasing", position)
		}
		kind := uint8(format.Put)
		if change.Delete {
			kind, change.Value = format.Delete, nil
		}
		if size := db.opts.checkEntryLimits(uint64(len(change.Key)), uint64(len(change.Value))); size != nil {
			return &BatchLimitError{Kind: BatchEntrySizeLimit, Index: position, Entry: size}
		}
		recordBytes := entrysize.RecordBytes(uint64(len(change.Key)), uint64(len(change.Value)))
		if input.bytes+recordBytes > MaxStagedApplyBytes {
			return &BatchLimitError{Kind: BatchRecordBytesLimit, Index: position, Actual: input.bytes + recordBytes, Limit: MaxStagedApplyBytes}
		}
		if writer == nil {
			allowance, err = db.stagedScratchAllowance()
			if err != nil {
				return err
			}
			if recordBytes > allowance {
				return ErrNoSpace
			}
			dir, err := db.store.WorkspaceDir()
			if err != nil {
				return err
			}
			input.file, err = platform.OpenScratch(dir, "tarp-staged-input-*")
			if err != nil {
				return err
			}
			writer = bufio.NewWriterSize(input.file, 32<<10)
		}
		if input.bytes+recordBytes > allowance {
			return ErrNoSpace
		}
		var header [format.RecordHeaderBytes]byte
		format.EncodeRecordHeader(&header, kind, uint64(position)+1, change.Key, change.Value)
		for _, part := range [3][]byte{header[:], change.Key, change.Value} {
			if _, err := writer.Write(part); err != nil {
				return err
			}
		}
		input.bytes += recordBytes
		input.count++
		previous = append(previous[:0], change.Key...)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if writer != nil {
		return writer.Flush()
	}
	return nil
}

func (db *DB) stagedScratchAllowance() (uint64, error) {
	available, err := platform.AvailableBytes(db.store.Dir)
	if err != nil {
		return 0, err
	}
	db.writer.Lock()
	db.state.Lock()
	reserve := db.cleanerWriteReserve()
	db.state.Unlock()
	db.writer.Unlock()
	return available - min(available, reserve), nil
}

// replay checks locally spooled bytes as it reads them. The callback borrows
// the current record until returning, so values are never retained per key.
func (input *stagedInput) replay(ctx context.Context, maxRecord int64, visit func(kind uint8, key, value []byte) error) error {
	stream := bufio.NewReaderSize(io.NewSectionReader(input.file, 0, int64(input.bytes)), 32<<10)
	var header [format.RecordHeaderBytes]byte
	var payload []byte
	left := int64(input.bytes)
	for i := range input.count {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(stream, header[:]); err != nil {
			return err
		}
		rec, err := format.DecodeRecordHeader(header[:], left)
		if err != nil {
			return err
		}
		total := rec.EncodedRecordBytes()
		if total > maxRecord || rec.Sequence != uint64(i)+1 {
			return errors.New("storage: invalid staged input record")
		}
		n := int(rec.KeyBytes + rec.ValueBytes)
		payload = slices.Grow(payload[:0], n)[:n]
		if _, err := io.ReadFull(stream, payload); err != nil {
			return err
		}
		key, value := payload[:rec.KeyBytes], payload[rec.KeyBytes:]
		if !rec.ValidPayload(key, value) {
			return errors.New("storage: staged input payload checksum")
		}
		if err := visit(rec.Kind, key, value); err != nil {
			return err
		}
		left -= total
	}
	if left != 0 {
		return errors.New("storage: trailing staged input bytes")
	}
	return nil
}

func (db *DB) commitStagedInput(ctx context.Context, input *stagedInput) (result error) {
	previousActive := db.lockMutation()
	defer func() { db.unlockMutation(previousActive, input.committed) }()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		return operationError(stagedApplyOp, "admission", err)
	}
	// The selected counts are incomplete while a checkpoint remaps its index.
	if db.layoutRemapping {
		return operationError(stagedApplyOp, "admission", ErrBusy)
	}
	appendBatch, err := db.store.BeginStagedAppend()
	if err != nil {
		return operationError(stagedApplyOp, "prepare", err)
	}
	defer func() {
		if err := appendBatch.Discard(); err != nil {
			result = stagedCleanup(result, input.committed, err)
		}
	}()
	tx := db.keys.Begin()
	defer func() {
		if !input.committed {
			tx.Abort()
		}
	}()
	stats := stagedWriteStats{segments: make(map[*storage.DataSegment]stagedSegmentStats)}
	metadata := db.store.StagedCatalogBytes()
	var outputAllowance uint64
	if err := input.replay(ctx, db.opts.MaxRecordBytes, func(kind uint8, key, value []byte) error {
		var old storage.Location
		if kind == format.Delete {
			prior, err := db.stage(tx, kind, string(key), storage.Location{})
			if errors.Is(err, ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			old = prior
		}
		if stats.records == 0 {
			available, err := platform.AvailableBytes(db.store.Dir)
			if err != nil {
				return err
			}
			headroom := spaceAdd(db.cleanerWriteReserve(), spaceAdd(metadata, 4*4096))
			outputAllowance = available - min(available, headroom)
		}
		// Charge at most one new DATA header per surviving record. A final
		// space sample and actual writes still handle changes on the volume.
		charge := entrysize.RecordBytes(uint64(len(key)), uint64(len(value))) + format.SegmentHeaderBytes
		if charge > outputAllowance {
			return ErrNoSpace
		}
		outputAllowance -= charge
		next, err := appendBatch.Append(kind, key, value)
		if err != nil {
			if errors.Is(err, storage.ErrStagedAppendSegmentLimit) {
				return errors.Join(ErrTooLarge, err)
			}
			return err
		}
		if kind == format.Put {
			old, err = db.stage(tx, kind, string(key), next)
			if err != nil {
				return err
			}
		}
		stats.add(kind, old, next)
		return nil
	}); err != nil {
		db.storageFailure(err)
		return operationError(stagedApplyOp, "prepare", err)
	}
	if stats.records == 0 {
		return nil
	}
	// The input spool and candidate DATA already consume free space. Reserve
	// metadata and foreground room against a fresh sample before publication.
	db.cleaner.space.checked = time.Time{}
	db.cleaner.space.debit = 0
	sealedBefore := db.store.CatalogEntries().Len()
	if err := db.checkWriteReserve(stagedApplyOp, metadata); err != nil {
		return err
	}
	addedSegments, publishedErr := appendBatch.Publish(ctx, db.health)
	if publishedErr != nil {
		fault := operationFault(publishedErr)
		if fault == nil || fault.Outcome != Committed {
			db.storageFailure(publishedErr)
			return operationError(stagedApplyOp, "publication", publishedErr)
		}
	}
	input.committed = true
	db.indexGate.Lock()
	defer db.indexGate.Unlock()
	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		tx.Abort()
		return &OpError{Operation: stagedApplyOp, Phase: "memory-publication", Outcome: Committed, Err: errors.Join(err, publishedErr)}
	}
	tx.Commit()
	stats.publish(db, addedSegments, sealedBefore)
	if publishedErr != nil {
		db.storageFailure(publishedErr)
		return operationError(stagedApplyOp, "publication", publishedErr)
	}
	return nil
}

func stagedCleanup(result error, committed bool, err error) error {
	if result != nil {
		return errors.Join(result, err)
	}
	outcome := NotApplied
	if committed {
		outcome = Committed
	}
	return &OpError{Operation: stagedApplyOp, Phase: "cleanup", Outcome: outcome, Err: publicCause(err)}
}

type stagedSegmentStats struct {
	keys, live, tombstone int64
}

type stagedWriteStats struct {
	segments           map[*storage.DataSegment]stagedSegmentStats
	oldKeys, newKeys   uint64
	oldBytes, newBytes uint64
	records, selected  uint64
	tombstone          uint64
	maxValue           int64
}

func (s *stagedWriteStats) add(kind uint8, old, next storage.Location) {
	if old.Segment != nil {
		delta := s.segments[old.Segment]
		delta.keys--
		delta.live -= old.RecordBytes
		s.segments[old.Segment] = delta
		s.oldKeys++
		s.oldBytes += uint64(old.RecordBytes)
	}
	delta := s.segments[next.Segment]
	if kind == format.Put {
		delta.keys++
		delta.live += next.RecordBytes
		s.newKeys++
		s.newBytes += uint64(next.RecordBytes)
		s.maxValue = max(s.maxValue, next.ValueBytes)
	} else {
		delta.tombstone += next.RecordBytes
		s.tombstone += uint64(next.RecordBytes)
	}
	s.segments[next.Segment] = delta
	s.records++
	s.selected += uint64(next.RecordBytes)
}

// publish is infallible and runs under the same locks as the index root swap.
func (s *stagedWriteStats) publish(db *DB, addedSegments, sealedBefore int) {
	q := &db.cleaner.candidates
	for segment, delta := range s.segments {
		segment.LiveBytes.Add(delta.live)
		segment.DeleteBytes.Add(delta.tombstone)
		current := segment.LiveKeys.Add(delta.keys)
		if current > 0 && current == delta.keys {
			db.segments[segment.ID] = segment
			db.readSegments.invalidate()
		} else if current == 0 && delta.keys < 0 {
			delete(db.segments, segment.ID)
			db.readSegments.invalidate()
		}
		q.changed(q.files[segment.ID], segment, db.opts.TargetSegmentBytes)
	}
	entries := db.store.CatalogEntries()
	for pos := sealedBefore; pos < entries.Len()-1; pos++ {
		q.track(db.store, entries.At(pos), pos, db.opts.TargetSegmentBytes)
	}
	db.stats.LiveKeys = db.stats.LiveKeys - s.oldKeys + s.newKeys
	db.stats.LiveRecordBytes = db.stats.LiveRecordBytes - s.oldBytes + s.newBytes
	db.stats.TombstoneBytes += s.tombstone
	db.stats.SelectedSegments += uint64(addedSegments)
	db.stats.SelectedDataBytes += s.selected + uint64(addedSegments)*format.SegmentHeaderBytes
	db.stats.WrittenRecords += s.records
	db.maxSeenValueBytes = max(db.maxSeenValueBytes, s.maxValue)
	db.cache.Clear()
	// Segment counts are already updated for the whole group, including
	// replacement of an empty previous active file.
	db.countWriteCommit(db.store.Current.ActiveID)
}
