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
	Current       format.Frontier
	Catalog       format.Catalog
	Manifest      []byte
	Files         map[uint64]*File
	Required      RequiredLimits
	LiveKeys      uint64
	MaxValueBytes int64
	openRead      func(string, int)
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
	if err := image.replayIndexed(ctx, backend); err != nil {
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
	b, err = ReadRegular(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, requiredFileError(manifestName, err)
	}
	if sha256.Sum256(b) != current.ManifestDigest {
		return nil, fmt.Errorf("%s: %w: selected catalog digest", manifestName, ErrCorrupt)
	}
	catalog, err := format.DecodeCatalog(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", manifestName, err)
	}
	active, err := catalog.ActiveEntry(current)
	if err != nil {
		return nil, fmt.Errorf("%s and CURRENT: %w", manifestName, err)
	}
	image := &Image{Current: current, Catalog: catalog, Manifest: b, Files: make(map[uint64]*File)}
	for _, entry := range append(append([]format.CatalogEntry(nil), catalog.Entries...), active) {
		if entry.HasPhysicalFile() {
			image.Files[entry.ID] = &File{ID: entry.ID, Kind: entry.Kind, Path: filepath.Join(dir, DataName(current.UUID, entry.ID))}
		}
	}
	return image, nil
}

// IndexImporter creates a private backend from the packed tree and visits each
// decoded live entry. It owns any partial backend until it returns successfully.
type IndexImporter func(context.Context, *Store, io.Reader, int64, func(string, index.Address) error) (index.Backend, error)

type recordVisitor func(*File, int64, format.RecordHeader, string) error

// scan validates each selected record through the committed end in replay order.
func (image *Image) scan(ctx context.Context, visit recordVisitor) error {
	return image.scanFrom(ctx, 0, "replay", visit)
}

// start is a catalog entry boundary. The active segment always follows the
// sealed entries, even when the committed prefix ends at its header.
func (image *Image) scanFrom(ctx context.Context, start int, area string, visit recordVisitor) error {
	active, err := image.Catalog.ActiveEntry(image.Current)
	if err != nil {
		return err
	}
	for _, entry := range append(append([]format.CatalogEntry(nil), image.Catalog.Entries[start:]...), active) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		file := image.Files[entry.ID]
		fd, _, physical, err := openSelectedForRead(file.Path, image.Current.UUID, entry, entry.ID == image.Current.ActiveID, image.openRead, area)
		if err != nil {
			if image.liveCapture {
				return selectedReadFailure(entry.ID, err)
			}
			return err
		}
		file.Size.Store(physical)
		var activeDigest hash.Hash
		if entry.ID == image.Current.ActiveID {
			file.activeDigest = sha256.New()
			activeDigest = file.activeDigest
		}
		var deletions int64
		var visitErr error
		err = scanSelectedRecords(ctx, observeDATA(fd, image.openRead, area), entry, image.Catalog.CheckpointCutoffSequence, func(off int64, r format.RecordHeader, key string) error {
			if r.Kind == format.Delete {
				deletions += r.EncodedRecordBytes()
			}
			visitErr = visit(file, off, r, key)
			return visitErr
		}, activeDigest)
		if image.liveCapture && err != nil && visitErr == nil {
			err = selectedReadFailure(entry.ID, err)
		}
		if err = errors.Join(err, fd.Close()); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(file.Path), err)
		}
		file.DeleteBytes.Store(deletions)
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
	image.MaxValueBytes = max(image.MaxValueBytes, valueBytes)
	if recordBytes <= image.Required.RecordBytes {
		return nil
	}
	charge := entrysize.CacheCharge(uint64(len(key)), uint64(valueBytes))
	if charge > math.MaxInt64 {
		return fmt.Errorf("%w: unrepresentable live cache charge", ErrCorrupt)
	}
	image.Required = RequiredLimits{ValueCacheBudgetBytes: int64(charge), RecordBytes: recordBytes}
	return nil
}

func (image *Image) replayIndexed(ctx context.Context, backend index.Backend) error {
	builder := newReplayBuilder(ctx, backend, image.Catalog.PrunedThroughSequence)
	defer builder.abort()
	if err := image.scan(ctx, builder.apply); err != nil {
		return err
	}
	return builder.finishWith(image.measureLive(ctx))
}

func (image *Image) measureLive(ctx context.Context) func(string, index.Address) error {
	return func(key string, addr index.Address) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		file := image.Files[addr.FileID]
		file.LiveBytes.Add(addr.Bytes)
		file.LiveKeys.Add(1)
		return image.measure(key, addr.Bytes, addr.ValueBytes)
	}
}

type coveredFile struct {
	entry               format.CatalogEntry
	liveKeys, liveBytes int64
}

// loadCheckpoint treats malformed derived state as a miss. Nothing measured
// during import is published until the entire checkpoint has been accepted.
func (s *Store) loadCheckpoint(ctx context.Context, image *Image, load IndexImporter) (index.Backend, error) {
	c := image.Catalog
	cp := c.Index
	layout, err := c.CoveredLayoutFingerprint(cp.CoveredCatalogEntries, cp.CoveredSequence)
	if err != nil || layout != cp.CoveredLayoutFingerprint {
		return nil, nil
	}
	if err := image.checkSelectedFiles(ctx, cp.CoveredCatalogEntries); err != nil {
		return nil, err
	}
	covered := make(map[uint64]*coveredFile)
	for _, entry := range c.Entries[:cp.CoveredCatalogEntries] {
		if entry.HasPhysicalFile() {
			covered[entry.ID] = &coveredFile{entry: entry}
		}
	}
	var totals Image
	visit := func(key string, addr index.Address) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		file := covered[addr.FileID]
		if file == nil || !validCheckpointAddress(key, addr, file.entry, c.CheckpointCutoffSequence) {
			return fmt.Errorf("%w: invalid covered DATA address", ErrIndexMiss)
		}
		if addr.Bytes > file.entry.Bytes-format.SegmentHeaderBytes-file.liveBytes {
			return fmt.Errorf("%w: live record bytes exceed sealed DATA length", ErrIndexMiss)
		}
		file.liveBytes += addr.Bytes
		file.liveKeys++
		if err := totals.measure(key, addr.Bytes, addr.ValueBytes); err != nil {
			return fmt.Errorf("%w: %v", ErrIndexMiss, err)
		}
		return nil
	}
	keys, deletes, err := s.readIndexCheckpoint(ctx, c, load, visit)
	if IsIndexMiss(err) {
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
			file := image.Files[entry.ID]
			file.DeleteBytes.Store(deletes[i])
			if exact {
				file.LiveKeys.Store(covered[entry.ID].liveKeys)
				file.LiveBytes.Store(covered[entry.ID].liveBytes)
			}
		}
	}
	if exact {
		image.LiveKeys, image.MaxValueBytes, image.Required = totals.LiveKeys, totals.MaxValueBytes, totals.Required
	}
	return keys, nil
}

func validCheckpointAddress(key string, addr index.Address, entry format.CatalogEntry, cutoff uint64) bool {
	if len(key) == 0 || addr.Sequence == 0 || addr.Offset < format.SegmentHeaderBytes ||
		addr.Bytes < format.RecordHeaderBytes || addr.Offset > entry.Bytes-addr.Bytes ||
		addr.ValueBytes < 0 || addr.Bytes-format.RecordHeaderBytes-int64(len(key)) != addr.ValueBytes {
		return false
	}
	if entry.Kind == format.SnapshotKind {
		return addr.Sequence <= cutoff
	}
	return addr.Sequence >= entry.FirstSequence && addr.Sequence <= entry.LastSequence
}

// Check every authoritative file before consulting the optional checkpoint.
func (image *Image) checkSelectedFiles(ctx context.Context, covered uint64) error {
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
		area := "tail"
		if uint64(i) < covered {
			area = "covered"
		}
		file := image.Files[entry.ID]
		fd, _, physical, err := openSelectedForRead(file.Path, image.Current.UUID, entry, entry.ID == active.ID, image.openRead, area)
		if err != nil {
			return err
		}
		file.Size.Store(physical)
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
		file := image.Files[entry.ID]
		fd, err := OpenRegular(file.Path)
		if err != nil {
			return requiredFileError(filepath.Base(file.Path), err)
		}
		err = verifySelectedDigest(ctx, observeDATA(fd, image.openRead, "covered"), entry)
		if err = errors.Join(err, fd.Close()); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(file.Path), err)
		}
	}
	return ctx.Err()
}

func (image *Image) replayAfterCheckpoint(ctx context.Context, keys index.Backend, indexedOpen bool) error {
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
	if err := image.scanFrom(ctx, int(cp.CoveredCatalogEntries), "tail", builder.apply); err != nil {
		return err
	}
	if cp.CoveredSequence == image.Current.LastUserSequence {
		return builder.finish()
	}
	return builder.finishWith(image.measureLive(ctx))
}

func applyIndexedRecord(tx index.Mutations, file *File, pruned uint64, off int64, r format.RecordHeader, key string) error {
	// Ordinary Put overwrites any prior location. Only snapshot and Delete
	// records need to read the previous state for validation.
	if file.Kind == format.SnapshotKind || r.Kind == format.Delete {
		_, present, err := tx.Lookup(key)
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		if err := validateKey(file.Kind, r, present, pruned); err != nil {
			return &SourceError{Err: err}
		}
	}
	var err error
	if r.Kind == format.Delete {
		err = tx.Delete(key)
	} else {
		err = tx.Put(key, index.Address{FileID: file.ID, Offset: off, Bytes: r.EncodedRecordBytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence})
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
	observe         func(string, int)
	header, payload string
}

func observeDATA(rd io.ReaderAt, observe func(string, int), area string) io.ReaderAt {
	if observe == nil {
		return rd
	}
	return observedDATA{rd, observe, area + "-header", area + "-payload"}
}

func (r observedDATA) ReadAt(p []byte, off int64) (int, error) {
	n, err := r.ReaderAt.ReadAt(p, off)
	header := int(min(int64(n), max(0, format.SegmentHeaderBytes-off)))
	if header > 0 {
		r.observe(r.header, header)
	}
	if n > header {
		r.observe(r.payload, n-header)
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
