package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

type Image struct {
	Current       format.Frontier
	Catalog       format.Catalog
	Manifest      []byte
	Files         map[uint64]*File
	Index         map[string]Location
	Required      RequiredLimits
	LiveKeys      uint64
	MaxValueBytes int64
}

// Validate checks and replays the selected image, measuring its required limits.
// It never changes a file and does not require/create LOCK.
func Validate(ctx context.Context, dir string) (*Image, error) {
	image, err := readImage(ctx, dir)
	if err != nil {
		return nil, err
	}
	image.Index = make(map[string]Location)
	err = image.scan(ctx, func(file *File, off int64, r format.Record, key string) error {
		return applyRecord(image.Index, file, image.Catalog.PrunedThrough, off, r, key)
	})
	if err != nil {
		return nil, err
	}
	for key, loc := range image.Index {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := image.measure(key, loc.Bytes, loc.ValueBytes); err != nil {
			return nil, err
		}
	}
	return image, ctx.Err()
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
	if sha256.Sum256(b) != current.ManifestHash {
		return nil, fmt.Errorf("%s: %w: selected catalog digest", manifestName, ErrCorrupt)
	}
	catalog, err := format.DecodeCatalog(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", manifestName, err)
	}
	active, err := catalog.ActiveFile(current)
	if err != nil {
		return nil, fmt.Errorf("%s and CURRENT: %w", manifestName, err)
	}
	image := &Image{Current: current, Catalog: catalog, Manifest: b, Files: make(map[uint64]*File)}
	for _, entry := range append(append([]format.File(nil), catalog.Files...), active) {
		if entry.HasPhysicalFile() {
			image.Files[entry.ID] = &File{ID: entry.ID, Kind: entry.Kind, Path: filepath.Join(dir, DataName(current.UUID, entry.ID))}
		}
	}
	return image, nil
}

type recordVisitor func(*File, int64, format.Record, string) error

// scan validates each selected record through the committed end in replay order.
func (image *Image) scan(ctx context.Context, visit recordVisitor) error {
	active, err := image.Catalog.ActiveFile(image.Current)
	if err != nil {
		return err
	}
	for _, entry := range append(append([]format.File(nil), image.Catalog.Files...), active) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		file := image.Files[entry.ID]
		fd, _, physical, err := openSelected(file.Path, image.Current.UUID, entry, entry.ID == image.Current.ActiveID)
		if err != nil {
			return err
		}
		file.Size.Store(physical)
		var deletions int64
		err = scanSelectedRecords(ctx, fd, entry, image.Catalog.Cutoff, func(off int64, r format.Record, key string) error {
			if r.Kind == format.Delete {
				deletions += r.Bytes()
			}
			return visit(file, off, r, key)
		})
		if err = errors.Join(err, fd.Close()); err != nil {
			return fmt.Errorf("%s: %w", filepath.Base(file.Path), err)
		}
		file.DeleteBytes.Store(deletions)
	}
	return ctx.Err()
}

func scanSelectedRecords(ctx context.Context, rd io.ReaderAt, entry format.File, cutoff uint64, visit func(int64, format.Record, string) error) error {
	span := format.NewRecordRange(entry, cutoff)
	if err := format.Scan(ctx, rd, entry.Bytes, func(off int64, r format.Record, key string) error {
		if err := span.Accept(r); err != nil {
			return err
		}
		return visit(off, r, key)
	}); err != nil {
		return err
	}
	return span.Finish()
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

func validateIndexed(ctx context.Context, dir string, backend index.Backend) (*Image, error) {
	image, err := readImage(ctx, dir)
	if err != nil {
		return nil, err
	}
	builder := newReplayBuilder(ctx, backend, image.Catalog.PrunedThrough)
	defer builder.abort()
	if err := image.scan(ctx, builder.apply); err != nil {
		return nil, err
	}
	err = builder.finishWith(func(key string, addr index.Address) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		file := image.Files[addr.FileID]
		file.LiveBytes.Add(addr.Bytes)
		file.LiveKeys.Add(1)
		return image.measure(key, addr.Bytes, addr.ValueBytes)
	})
	if err != nil {
		return nil, err
	}
	return image, nil
}

func applyIndexedRecord(tx index.Mutations, file *File, pruned uint64, off int64, r format.Record, key string) error {
	// Ordinary Put overwrites any prior location. Only snapshot and Delete
	// records need to read the previous state for validation.
	if file.Kind == format.Snapshot || r.Kind == format.Delete {
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
		err = tx.Put(key, index.Address{FileID: file.ID, Offset: off, Bytes: r.Bytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence})
	}
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	return nil
}

// openSelected checks the same committed file identity and bounds for reopen,
// checkpoint replay, and online backup. A captured active prefix can grow.
func openSelected(path string, uuid [16]byte, entry format.File, mayGrow bool) (fd *os.File, header []byte, physical int64, result error) {
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
	if _, err := fd.ReadAt(header, 0); err != nil {
		return nil, nil, 0, requiredFileError("segment header", err)
	}
	decoded, err := format.DecodeSegment(header)
	if err != nil {
		return nil, nil, 0, err
	}
	if decoded.UUID != uuid || decoded.ID != entry.ID || decoded.Kind != entry.Kind {
		return nil, nil, 0, fmt.Errorf("%w: segment identity", ErrCorrupt)
	}
	return fd, header, st.Size(), nil
}

func validateKey(kind uint32, record format.Record, present bool, prunedThrough uint64) error {
	switch {
	case kind == format.Snapshot && present:
		return fmt.Errorf("%w: duplicate snapshot key", ErrCorrupt)
	case record.Kind == format.Delete && !present && record.Sequence > prunedThrough:
		return fmt.Errorf("%w: tombstone without a live key", ErrCorrupt)
	}
	return nil
}

func applyRecord(index map[string]Location, file *File, prunedThrough uint64, offset int64, r format.Record, key string) error {
	_, present := index[key]
	if err := validateKey(file.Kind, r, present, prunedThrough); err != nil {
		return err
	}
	if r.Kind == format.Delete {
		delete(index, key)
	} else {
		index[key] = Location{File: file, Offset: offset, Bytes: r.Bytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence}
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
