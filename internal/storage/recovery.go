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
)

type Image struct {
	Current  format.Frontier
	Catalog  format.Catalog
	Manifest []byte
	Files    map[uint64]*File
	Index    map[string]Location
	Required RequiredLimits
}

// Validate checks and replays the selected image, measuring its required limits.
// It never changes a file and does not require/create LOCK.
func Validate(ctx context.Context, dir string) (*Image, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b, err := readCurrent(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return nil, fmt.Errorf("%w: missing or unreadable CURRENT: %w", ErrCorrupt, err)
	}
	current, err := format.DecodeFrontier(b)
	if err != nil {
		return nil, fmt.Errorf("CURRENT: %w", err)
	}
	manifestName := ManifestName(current.UUID, current.ManifestID)
	b, err = ReadRegular(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, fmt.Errorf("%w: selected catalog: %w", ErrCorrupt, err)
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
	image := &Image{Current: current, Catalog: catalog, Manifest: b, Files: make(map[uint64]*File), Index: make(map[string]Location)}
	for _, entry := range append(append([]format.File(nil), catalog.Files...), active) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.HasPhysicalFile() {
			continue // This cleaned interval has no physical records.
		}
		path := filepath.Join(dir, DataName(current.UUID, entry.ID))
		fd, _, physical, err := openSelected(path, current.UUID, entry, entry.ID == current.ActiveID)
		if err != nil {
			return nil, err
		}
		file := &File{ID: entry.ID, Kind: entry.Kind, Path: path}
		file.Size.Store(physical)
		span := format.NewRecordRange(entry, catalog.Cutoff)
		scanErr := format.Scan(ctx, fd, entry.Bytes, func(offset int64, r format.Record, key string) error {
			if err := span.Accept(r); err != nil {
				return err
			}
			if r.Kind == format.Delete {
				file.DeleteBytes.Add(r.Bytes())
			}
			return applyRecord(image.Index, file, catalog.PrunedThrough, offset, r, key)
		})
		// The sequence range is only complete after a successful full scan.
		if scanErr == nil {
			scanErr = span.Finish()
		}
		if err := errors.Join(scanErr, fd.Close()); err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(path), err)
		}
		image.Files[file.ID] = file
	}
	remaining := len(image.Index)
	for key, loc := range image.Index {
		if remaining%256 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		remaining--
		// Both limits grow with payload length, so the largest live record
		// determines both minima.
		if loc.Bytes <= image.Required.RecordBytes {
			continue
		}
		charge := entrysize.CacheCharge(uint64(len(key)), uint64(loc.ValueBytes))
		if charge > math.MaxInt64 {
			return nil, fmt.Errorf("%w: unrepresentable live cache charge", ErrCorrupt)
		}
		image.Required = RequiredLimits{CacheBytes: int64(charge), RecordBytes: loc.Bytes}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return image, nil
}

// openSelected checks the same committed file identity and bounds for reopen,
// checkpoint replay, and online backup. A captured active prefix can grow.
func openSelected(path string, uuid [16]byte, entry format.File, mayGrow bool) (fd *os.File, header []byte, physical int64, result error) {
	fd, err := OpenRegular(path)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%w: selected segment: %w", ErrCorrupt, err)
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
		return nil, nil, 0, fmt.Errorf("%w: segment header: %w", ErrCorrupt, err)
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
