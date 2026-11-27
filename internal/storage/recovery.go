package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

type Image struct {
	Current      format.Frontier
	Catalog      format.Catalog
	catalogChain []uint64
	catalogBytes uint64
	Segments     map[uint64]*DataSegment
	LiveRecordSizes
	LiveKeys              uint64
	LargestLiveValueBytes int64
	openRead              func(string, int)
	indexReplayed         bool
	// Only an index build has a live capture whose pinned DATA can be repaired.
	liveCapture bool
}

// Validate checks and replays the selected image, measuring its required limits.
// It never changes a file and does not require/create LOCK.
func Validate(ctx context.Context, dir string) (*Image, error) {
	image, err := readImage(ctx, dir)
	if err != nil {
		return nil, err
	}
	backend := memory.New()
	defer backend.Close()
	if err := image.rebuildIndexFromData(ctx, backend); err != nil {
		return nil, err
	}
	return image, nil
}

// readImage decodes the exact frontier and catalog before any index is built.
func readImage(ctx context.Context, dir string) (*Image, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := readCurrent(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return nil, requiredFileError("CURRENT", err)
	}
	current, err := format.DecodeFrontier(b)
	if err != nil {
		return nil, fmt.Errorf("CURRENT: %w", err)
	}
	manifestName := ManifestName(current.UUID, current.ManifestID)
	catalog, catalogBytes, chain, err := readCatalog(ctx, dir, current)
	if err != nil {
		return nil, err
	}
	active, err := catalog.ActiveEntry(current)
	if err != nil {
		return nil, fmt.Errorf("%s and CURRENT: %w", manifestName, err)
	}
	image := &Image{Current: current, Catalog: catalog, catalogBytes: catalogBytes, catalogChain: chain, Segments: make(map[uint64]*DataSegment)}
	for _, entry := range append(append([]format.CatalogEntry(nil), catalog.Entries...), active) {
		if entry.HasPhysicalFile() {
			image.Segments[entry.ID] = &DataSegment{ID: entry.ID, Kind: entry.Kind, Path: filepath.Join(dir, DataName(current.UUID, entry.ID))}
		}
	}
	for _, id := range chain {
		if image.Segments[id] != nil || catalog.Index.ID == id {
			return nil, fmt.Errorf("%w: catalog object and selected file share ID %d", ErrCorrupt, id)
		}
	}
	return image, nil
}

// IndexImporter creates a private backend from the packed tree and visits each
// decoded live entry. It owns any partial backend until it returns successfully.
type IndexImporter func(context.Context, *Store, io.Reader, int64, func(string, index.Address) error) (index.Backend, error)

type recordVisitor func(*DataSegment, int64, format.RecordHeader, string) error

// scan validates each selected record through the committed end in replay order.
func (image *Image) scan(ctx context.Context, visit recordVisitor) error {
	return image.scanFrom(ctx, 0, "replay", visit, nil)
}

// start is a catalog entry boundary. The active segment always follows the
// sealed entries, even when the committed prefix ends at its header.
func (image *Image) scanFrom(ctx context.Context, start int, area string, visit recordVisitor, order *snapshotOrderCheck) error {
	active, err := image.Catalog.ActiveEntry(image.Current)
	if err != nil {
		return err
	}
	entries := append(append([]format.CatalogEntry(nil), image.Catalog.Entries[start:]...), active)
	return image.scanEntries(ctx, entries, area, visit, order)
}

func (image *Image) scanEntries(ctx context.Context, entries []format.CatalogEntry, area string, visit recordVisitor, order *snapshotOrderCheck) error {
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		segment := image.Segments[entry.ID]
		fd, _, physical, err := openSelectedForRead(segment.Path, image.Current.UUID, entry, entry.ID == image.Current.ActiveID, image.openRead, area)
		if err != nil {
			if image.liveCapture {
				return selectedReadFailure(entry.ID, err)
			}
			return err
		}
		segment.Size.Store(physical)
		var activeDigest hash.Hash
		if entry.ID == image.Current.ActiveID {
			segment.activeDigest = sha256.New()
			activeDigest = segment.activeDigest
		}
		var deletions int64
		var visitErr error
		var bounds scannedSnapshot
		checkOrder := order != nil && !order.unordered && entry.Kind == format.SnapshotKind
		err = scanSelectedRecords(ctx, observeDATA(fd, image.openRead, area), entry, image.Catalog.CheckpointCutoffSequence, func(off int64, r format.RecordHeader, key string) error {
			if r.Kind == format.Delete {
				deletions += r.EncodedRecordBytes()
			}
			if checkOrder {
				bounds.add(key, off)
			}
			visitErr = visit(segment, off, r, key)
			return visitErr
		}, activeDigest)
		if image.liveCapture && err != nil && visitErr == nil {
			err = selectedReadFailure(entry.ID, err)
		}
		if err = errors.Join(err, fd.Close()); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(segment.Path), err)
		}
		segment.DeleteBytes.Store(deletions)
		if checkOrder {
			order.accept(segment, &bounds)
		}
	}
	return ctx.Err()
}

func scanSelectedRecords(ctx context.Context, rd io.ReaderAt, entry format.CatalogEntry, cutoff uint64, visit func(int64, format.RecordHeader, string) error, activeDigest ...hash.Hash) error {
	var h hash.Hash
	if entry.DataFileDigest != ([sha256.Size]byte{}) {
		h = sha256.New()
	} else if len(activeDigest) != 0 {
		h = activeDigest[0]
	}
	if h != nil {
		stream := &digestReader{ReaderAt: rd, hash: h}
		var header [format.SegmentHeaderBytes]byte
		if _, err := io.ReadFull(io.NewSectionReader(stream, 0, int64(len(header))), header[:]); err != nil {
			return err
		}
		rd = stream
	}
	span := format.NewRecordSequenceValidator(entry, cutoff)
	if err := format.Scan(ctx, rd, entry.Bytes, func(off int64, r format.RecordHeader, key string) error {
		if err := span.Accept(r); err != nil {
			return err
		}
		return visit(off, r, key)
	}); err != nil {
		return err
	}
	if err := span.Finish(); err != nil {
		return err
	}
	if entry.DataFileDigest != ([sha256.Size]byte{}) {
		return checkSealedDigest(h, entry)
	}
	return nil
}

func (image *Image) measure(key string, recordBytes, valueBytes int64) error {
	image.LiveKeys++
	image.LargestLiveValueBytes = max(image.LargestLiveValueBytes, valueBytes)
	if recordBytes <= image.LargestLiveRecordBytes {
		return nil
	}
	charge := entrysize.CacheCharge(uint64(len(key)), uint64(valueBytes))
	if charge > math.MaxInt64 {
		return fmt.Errorf("%w: unrepresentable live cache charge", ErrCorrupt)
	}
	image.LiveRecordSizes = LiveRecordSizes{
		LargestLiveCacheChargeBytes: int64(charge),
		LargestLiveRecordBytes:      recordBytes,
	}
	return nil
}

func (image *Image) rebuildIndexFromData(ctx context.Context, backend index.Backend) error {
	return image.rebuildIndex(ctx, backend, nil)
}

func (image *Image) rebuildIndex(ctx context.Context, backend index.Backend, order *snapshotOrderCheck) error {
	builder := newReplayBuilder(ctx, backend, image.Catalog.PrunedThroughSequence)
	defer builder.abort()
	if err := image.scanFrom(ctx, 0, "replay", builder.apply, order); err != nil {
		return err
	}
	return builder.finishWith(image.measureLive(ctx))
}

func (image *Image) measureLive(ctx context.Context) func(string, index.Address) error {
	return func(key string, addr index.Address) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		segment := image.Segments[addr.FileID]
		segment.LiveBytes.Add(addr.RecordBytes)
		segment.LiveKeys.Add(1)
		return image.measure(key, addr.RecordBytes, addr.ValueBytes)
	}
}

type coveredSegment struct {
	entry               format.CatalogEntry
	liveKeys, liveBytes int64
}

// loadIndexCheckpoint falls back to DATA replay when the index checkpoint is unusable.
// Measurements from the import become visible once the complete checkpoint passes validation.
func (s *Store) loadIndexCheckpoint(ctx context.Context, image *Image, load IndexImporter) (index.Backend, error) {
	c := image.Catalog
	cp := c.Index
	fingerprint, err := c.CoveredLayoutFingerprint(cp.CoveredCatalogEntries, cp.CoveredSequence)
	if err != nil || fingerprint != cp.CoveredLayoutFingerprint {
		return nil, nil
	}
	if err := image.checkSelectedFiles(ctx, func(i int, _ format.CatalogEntry) bool {
		return uint64(i) < cp.CoveredCatalogEntries
	}); err != nil {
		return nil, err
	}
	covered := make(map[uint64]*coveredSegment)
	for _, entry := range c.Entries[:cp.CoveredCatalogEntries] {
		if entry.HasPhysicalFile() {
			covered[entry.ID] = &coveredSegment{entry: entry}
		}
	}
	var totals Image
	visit := func(key string, addr index.Address) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		segment := covered[addr.FileID]
		if segment == nil || !validCheckpointAddress(key, addr, segment.entry, c.CheckpointCutoffSequence) {
			return fmt.Errorf("%w: invalid covered DATA address", ErrIndexCheckpointUnusable)
		}
		if addr.RecordBytes > segment.entry.Bytes-format.SegmentHeaderBytes-segment.liveBytes {
			return fmt.Errorf("%w: live record bytes exceed sealed DATA length", ErrIndexCheckpointUnusable)
		}
		segment.liveBytes += addr.RecordBytes
		segment.liveKeys++
		if err := totals.measure(key, addr.RecordBytes, addr.ValueBytes); err != nil {
			return fmt.Errorf("%w: %v", ErrIndexCheckpointUnusable, err)
		}
		return nil
	}
	keys, deletes, err := s.readIndexCheckpoint(ctx, c, load, visit)
	if IsIndexCheckpointUnusable(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for i, entry := range c.Entries[:cp.CoveredCatalogEntries] {
		var maxDeleteBytes int64
		if entry.HasPhysicalFile() && entry.Kind != format.SnapshotKind {
			maxDeleteBytes = entry.Bytes - format.SegmentHeaderBytes - covered[entry.ID].liveBytes
		}
		if deletes[i] < 0 || deletes[i] > maxDeleteBytes {
			return nil, keys.Close()
		}
	}
	exact := cp.CoveredSequence == image.Current.LastUserSequence
	for i, entry := range c.Entries[:cp.CoveredCatalogEntries] {
		if entry.HasPhysicalFile() {
			segment := image.Segments[entry.ID]
			segment.DeleteBytes.Store(deletes[i])
			if exact {
				segment.LiveKeys.Store(covered[entry.ID].liveKeys)
				segment.LiveBytes.Store(covered[entry.ID].liveBytes)
			}
		}
	}
	if exact {
		image.LiveKeys, image.LargestLiveValueBytes, image.LiveRecordSizes = totals.LiveKeys, totals.LargestLiveValueBytes, totals.LiveRecordSizes
	}
	return keys, nil
}

func validCheckpointAddress(key string, addr index.Address, entry format.CatalogEntry, cutoff uint64) bool {
	if len(key) == 0 || addr.Sequence == 0 || addr.RecordOffset < format.SegmentHeaderBytes ||
		addr.RecordBytes < format.RecordHeaderBytes || addr.RecordOffset > entry.Bytes-addr.RecordBytes ||
		addr.ValueBytes < 0 || addr.RecordBytes-format.RecordHeaderBytes-int64(len(key)) != addr.ValueBytes {
		return false
	}
	if entry.Kind == format.SnapshotKind {
		return addr.Sequence <= cutoff
	}
	return addr.Sequence >= entry.FirstSequence && addr.Sequence <= entry.LastSequence
}

// Check every authoritative file before consulting the optional checkpoint.
func (image *Image) checkSelectedFiles(ctx context.Context, covered func(int, format.CatalogEntry) bool) error {
	active, err := image.Catalog.ActiveEntry(image.Current)
	if err != nil {
		return err
	}
	for i, entry := range append(append([]format.CatalogEntry(nil), image.Catalog.Entries...), active) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		area := "log_suffix"
		if covered(i, entry) {
			area = "covered"
		}
		segment := image.Segments[entry.ID]
		fd, _, physical, err := openSelectedForRead(segment.Path, image.Current.UUID, entry, entry.ID == active.ID, image.openRead, area)
		if err != nil {
			return err
		}
		segment.Size.Store(physical)
		if err := fd.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (image *Image) verifyCoveredFiles(ctx context.Context, entries uint64) error {
	for _, entry := range image.Catalog.Entries[:entries] {
		if !entry.HasPhysicalFile() {
			continue
		}
		segment := image.Segments[entry.ID]
		fd, err := OpenRegular(segment.Path)
		if err != nil {
			return requiredFileError(filepath.Base(segment.Path), err)
		}
		err = verifySelectedDigest(ctx, observeDATA(fd, image.openRead, "covered"), entry)
		if err = errors.Join(err, fd.Close()); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(segment.Path), err)
		}
	}
	return ctx.Err()
}

func (image *Image) replayAfterIndexCheckpoint(ctx context.Context, keys index.Backend, indexedOpen bool) error {
	cp := image.Catalog.Index
	if !indexedOpen {
		if err := image.verifyCoveredFiles(ctx, cp.CoveredCatalogEntries); err != nil {
			return err
		}
	}
	// Both optimized builders require an empty backend. Apply suffix records
	// to bounded private transactions on top of the imported checkpoint.
	builder := replayBuilder{backend: keys, pruned: image.Catalog.PrunedThroughSequence}
	defer builder.abort()
	if err := image.scanFrom(ctx, int(cp.CoveredCatalogEntries), "log_suffix", builder.apply, nil); err != nil {
		return err
	}
	if cp.CoveredSequence == image.Current.LastUserSequence {
		return builder.finish()
	}
	return builder.finishWith(image.measureLive(ctx))
}

func applyIndexedRecord(tx index.Mutations, segment *DataSegment, pruned uint64, off int64, r format.RecordHeader, key string) error {
	// Ordinary Put overwrites any prior location. Only snapshot and Delete
	// records need to read the previous state for validation.
	if segment.Kind == format.SnapshotKind || r.Kind == format.Delete {
		_, present, err := tx.Lookup(key)
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		if err := validateKey(segment.Kind, r, present, pruned); err != nil {
			return &SourceError{Err: err}
		}
	}
	var err error
	if r.Kind == format.Delete {
		err = tx.Delete(key)
	} else {
		err = tx.Put(key, index.Address{FileID: segment.ID, RecordOffset: off, RecordBytes: r.EncodedRecordBytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence})
	}
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	return nil
}

// openSelected checks the same committed file identity and bounds for reopen,
// checkpoint replay, and online backup. A captured active prefix can grow.
func openSelected(path string, uuid [16]byte, entry format.CatalogEntry, mayGrow bool) (fd *os.File, header []byte, physical int64, result error) {
	return openSelectedForRead(path, uuid, entry, mayGrow, nil, "")
}

type observedDATA struct {
	io.ReaderAt
	observe                       func(string, int)
	segmentHeader, encodedRecords string
}

func observeDATA(rd io.ReaderAt, observe func(string, int), area string) io.ReaderAt {
	if observe == nil {
		return rd
	}
	return observedDATA{rd, observe, area + "_segment_header", area + "_encoded_records"}
}

func (r observedDATA) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.ReaderAt.ReadAt(p, off)
	header := int(min(int64(n), max(0, format.SegmentHeaderBytes-off)))
	if header > 0 {
		r.observe(r.segmentHeader, header)
	}
	if n > header {
		r.observe(r.encodedRecords, n-header)
	}
	return n, err
}

func openSelectedForRead(path string, uuid [16]byte, entry format.CatalogEntry, mayGrow bool, observe func(string, int), area string) (fd *os.File, header []byte, physical int64, result error) {
	fd, err := OpenRegular(path)
	if err != nil {
		return nil, nil, 0, requiredFileError(filepath.Base(path), err)
	}
	opened := fd
	defer func() {
		if result != nil {
			result = fmt.Errorf("%s: %w", filepath.Base(path), errors.Join(result, opened.Close()))
		}
	}()
	st, err := fd.Stat()
	if err != nil {
		return nil, nil, 0, err
	}
	if st.Size() < entry.Bytes || !mayGrow && st.Size() != entry.Bytes {
		return nil, nil, 0, fmt.Errorf("%w: selected file length", ErrCorrupt)
	}
	header = make([]byte, format.SegmentHeaderBytes)
	if _, err := observeDATA(fd, observe, area).ReadAt(header, 0); err != nil {
		return nil, nil, 0, requiredFileError("segment header", err)
	}
	if err := verifySegmentHeader(header, uuid, entry); err != nil {
		return nil, nil, 0, err
	}
	return fd, header, st.Size(), nil
}

func verifySegmentHeader(header []byte, uuid [16]byte, entry format.CatalogEntry) error {
	decoded, err := format.DecodeSegmentHeader(header)
	if err != nil {
		return err
	}
	if decoded.UUID != uuid || decoded.ID != entry.ID || decoded.Kind != entry.Kind {
		return fmt.Errorf("%w: segment identity", ErrCorrupt)
	}
	return nil
}

func validateKey(kind uint32, record format.RecordHeader, present bool, prunedThrough uint64) error {
	switch {
	case kind == format.SnapshotKind && present:
		return fmt.Errorf("%w: duplicate snapshot key", ErrCorrupt)
	case record.Kind == format.Delete && !present && record.Sequence > prunedThrough:
		return fmt.Errorf("%w: tombstone without a live key", ErrCorrupt)
	}
	return nil
}

func readCurrent(path string) (b []byte, result error) {
	fd, err := OpenRegular(path)
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, fd.Close()) }()
	return io.ReadAll(io.LimitReader(fd, format.FrontierBytes+1))
}
