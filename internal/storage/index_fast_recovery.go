package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
)

// FastIndexImporter opens an authenticated immutable base with a writable
// overlay and owns closeSource on success. Page reads are authenticated lazily.
type FastIndexImporter func(ctx context.Context, store *Store, pages io.ReaderAt, closeSource io.Closer, size int64, layer *btree.OverlayLayout) (index.Backend, error)

const maxIndexedReplayBytes = 4 << 30

// A failed fast attempt discards its temporary tree and accounting. Recovery
// then rebuilds from authoritative DATA at the same committed frontier.
func (s *Store) loadFastIndexCheckpoint(ctx context.Context, image *Image, load FastIndexImporter, report *RecoveryInfo) (keys index.Backend, result error) {
	chain, err := s.openIndexChain(ctx, image.Catalog)
	if IsIndexCheckpointUnusable(err) {
		report.noteCheckpointFailure(err)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	s.indexAncestors = chain.ids(1)
	file := chain.parts[0].file
	var backend index.Backend
	defer func() {
		if keys != nil {
			return
		}
		if backend == nil {
			result = errors.Join(result, chain.Close())
		} else {
			result = errors.Join(result, backend.Close())
		}
		if IsIndexCheckpointUnusable(result, btree.ErrInvalidPacked) {
			report.noteCheckpointFailure(result)
			result = nil
		}
	}()
	recovered, replay, err := prepareIndexReplay(image, file)
	if err != nil {
		return nil, err
	}
	report.noteChangedData(replay.sources, 0)
	changed := make(map[uint64]bool, len(replay.sources))
	for _, entry := range replay.sources {
		changed[entry.ID] = true
	}
	if err := recovered.checkSelectedFiles(ctx, func(_ int, entry format.CatalogEntry) bool {
		return !changed[entry.ID]
	}); err != nil {
		return nil, err
	}
	backend, err = chain.open(ctx, s, load)
	if err != nil {
		return nil, err
	}
	if err := replayIndexSources(ctx, &recovered, replay, backend, "log_suffix"); err != nil {
		return nil, err
	}
	// Upper bounds remain safe after a delete or overwrite. A caller with
	// stricter options must get the exact answer from authoritative replay.
	if s.Config.CheckLimits(recovered.LiveRecordSizes) != nil {
		return nil, errIndexCheckpointOptions
	}
	recovered.indexReplayed = true
	chain.stopMetering()
	*image = recovered
	return backend, nil
}

func replayIndexSources(ctx context.Context, recovered *Image, replay *indexSuffixReplay, backend index.Backend, area string) error {
	if uint64(backend.Count()) != recovered.LiveKeys {
		return ErrIndexCheckpointUnusable
	}
	replay.image = recovered
	replay.backend = backend
	defer replay.abort()
	if err := recovered.scanEntries(ctx, replay.sources, area, replay.apply, nil); err != nil {
		return err
	}
	replay.finish()
	for id, f := range replay.segments {
		if recovered.Segments[id] == nil && (f.LiveBytes.Load() != 0 || f.LiveKeys.Load() != 0) {
			return ErrIndexCheckpointUnusable
		}
	}
	if recovered.LiveKeys != uint64(backend.Count()) {
		return ErrIndexCheckpointUnusable
	}
	return nil
}

type indexSuffixReplay struct {
	image           *Image
	backend         index.Backend
	tx              index.Tx
	coveredSequence uint64
	segments        map[uint64]*DataSegment
	entries         map[uint64]format.CatalogEntry
	sources         []format.CatalogEntry
	count           int
	charge          int
}

func prepareIndexReplay(image *Image, file *selectedIndexFile) (Image, *indexSuffixReplay, error) {
	return prepareIndexReplayLimit(image, file, maxIndexedReplayBytes)
}

func prepareIndexReplayLimit(image *Image, file *selectedIndexFile, limit int64) (recovered Image, replay *indexSuffixReplay, result error) {
	current := image.Catalog
	covered := current.Index.CoveredSequence
	if covered > image.Current.LastUserSequence || covered < current.CheckpointCutoffSequence {
		return recovered, nil, ErrIndexCheckpointUnusable
	}
	// All accounting remains private until replay succeeds or falls back.
	recovered = *image
	recovered.Segments = make(map[uint64]*DataSegment, len(image.Segments))
	for id, f := range image.Segments {
		recovered.Segments[id] = &DataSegment{ID: f.ID, Kind: f.Kind, Path: f.Path}
	}
	replay = &indexSuffixReplay{
		coveredSequence: covered,
		segments:        make(map[uint64]*DataSegment),
		entries:         make(map[uint64]format.CatalogEntry),
	}
	var keysTotal uint64
	for i, old := range file.layout.Entries {
		info := file.counts[i]
		if !old.HasPhysicalFile() {
			if info != (indexCheckpointCounts{}) {
				return recovered, nil, ErrIndexCheckpointUnusable
			}
			continue
		}
		available := old.Bytes - format.SegmentHeaderBytes
		if info.deletes < 0 || info.deletes > available || info.liveBytes < 0 ||
			info.liveBytes > available-info.deletes || info.liveKeys < 0 ||
			info.liveKeys > info.liveBytes/(format.RecordHeaderBytes+1) ||
			old.Kind == format.SnapshotKind && info.deletes != 0 ||
			uint64(info.liveKeys) > math.MaxUint64-keysTotal {
			return recovered, nil, ErrIndexCheckpointUnusable
		}
		f := recovered.Segments[old.ID]
		if f == nil {
			f = &DataSegment{ID: old.ID, Kind: old.Kind}
		}
		f.LiveBytes.Store(info.liveBytes)
		f.LiveKeys.Store(info.liveKeys)
		f.DeleteBytes.Store(info.deletes)
		replay.segments[old.ID], replay.entries[old.ID] = f, old
		keysTotal += uint64(info.liveKeys)
	}
	u64 := binary.LittleEndian.Uint64
	largest, maxValue := u64(file.header[72:80]), u64(file.header[80:88])
	if keysTotal != u64(file.header[64:72]) || largest > math.MaxInt64 ||
		(keysTotal == 0 && (largest != 0 || maxValue != 0)) ||
		(keysTotal != 0 && (largest <= format.RecordHeaderBytes || maxValue >= largest-format.RecordHeaderBytes)) {
		return recovered, nil, ErrIndexCheckpointUnusable
	}
	recovered.LiveKeys = keysTotal
	recovered.LargestLiveValueBytes = int64(maxValue)
	if largest != 0 {
		charge := entrysize.CacheCharge(0, largest-format.RecordHeaderBytes)
		if charge > math.MaxInt64 {
			return recovered, nil, ErrIndexCheckpointUnusable
		}
		recovered.LiveRecordSizes = LiveRecordSizes{LargestLiveRecordBytes: int64(largest), LargestLiveCacheChargeBytes: int64(charge)}
	}
	active, err := current.ActiveEntry(image.Current)
	if err != nil {
		return recovered, nil, err
	}
	left := limit
	add := func(f format.CatalogEntry) error {
		if !f.HasPhysicalFile() {
			return nil
		}
		if old, exists := replay.entries[f.ID]; exists {
			if old != f {
				return ErrIndexCheckpointUnusable
			}
			return nil
		}
		if f.Bytes > left {
			return &indexReplayBudgetError{bytes: uint64(limit-left) + uint64(f.Bytes)}
		}
		left -= f.Bytes
		replay.sources = append(replay.sources, f)
		replay.segments[f.ID], replay.entries[f.ID] = recovered.Segments[f.ID], f
		return nil
	}
	for _, f := range current.Entries {
		if err := add(f); err != nil {
			return recovered, nil, err
		}
	}
	if err := add(active); err != nil {
		return recovered, nil, err
	}
	return recovered, replay, nil
}

func (r *indexSuffixReplay) abort() {
	if r.tx != nil {
		r.tx.Abort()
	}
}

func (r *indexSuffixReplay) finish() {
	if r.tx != nil {
		r.tx.Commit()
		r.tx = nil
	}
	r.count, r.charge = 0, 0
}

func (r *indexSuffixReplay) apply(segment *DataSegment, off int64, header format.RecordHeader, key string) error {
	relocation := header.Sequence <= r.coveredSequence
	if relocation && header.Kind == format.Delete {
		return nil
	}
	if r.tx == nil {
		r.tx = r.backend.Begin()
	}
	old, found, err := r.tx.Lookup(key)
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	if relocation && (!found || old.Sequence != header.Sequence) {
		return nil
	}
	if found {
		entry, ok := r.entries[old.FileID]
		if !ok || !validCheckpointAddress(key, old, entry, r.image.Catalog.CheckpointCutoffSequence) {
			return ErrIndexCheckpointUnusable
		}
	}
	if relocation {
		// Cleaner output can physically move a checkpoint record only from
		// a file that is no longer selected, without changing its size.
		if r.image.Segments[old.FileID] != nil || old.RecordBytes != header.EncodedRecordBytes() {
			return ErrIndexCheckpointUnusable
		}
	} else if err := validateKey(segment.Kind, header, found, r.image.Catalog.PrunedThroughSequence); err != nil {
		return &SourceError{Err: err}
	}
	if found {
		f := r.segments[old.FileID]
		if f.LiveBytes.Load() < old.RecordBytes || f.LiveKeys.Load() <= 0 || r.image.LiveKeys == 0 {
			return ErrIndexCheckpointUnusable
		}
		r.image.LiveKeys--
		f.LiveBytes.Add(-old.RecordBytes)
		f.LiveKeys.Add(-1)
	}
	if header.Kind == format.Delete {
		err = r.tx.Delete(key)
	} else {
		addr := index.Address{FileID: segment.ID, RecordOffset: off, RecordBytes: header.EncodedRecordBytes(), ValueBytes: header.ValueBytes, Sequence: header.Sequence}
		err = r.tx.Put(key, addr)
		segment.LiveBytes.Add(addr.RecordBytes)
		segment.LiveKeys.Add(1)
		if e := r.image.measure(key, addr.RecordBytes, addr.ValueBytes); e != nil {
			return e
		}
	}
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	r.count++
	if r.count >= 128 || len(key) >= (64<<10)-r.charge {
		r.finish()
	} else {
		r.charge += len(key)
	}
	return nil
}
