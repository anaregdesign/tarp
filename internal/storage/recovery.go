package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
)

type Image struct {
	Current  format.Frontier
	Catalog  format.Catalog
	Manifest []byte
	Files    map[uint64]*File
	Index    map[string]Location
	Required CompatibilityError
}

// Validate never changes a file and does not require/create LOCK.
func Validate(dir string, cfg Config) (*Image, error) {
	b, err := readCurrent(filepath.Join(dir, "CURRENT"))
	if err != nil {
		return nil, fmt.Errorf("%w: missing or unreadable CURRENT: %w", ErrCorrupt, err)
	}
	current, err := format.DecodeFrontier(b)
	if err != nil {
		return nil, err
	}
	b, err = ReadRegular(filepath.Join(dir, ManifestName(current.UUID, current.ManifestID)))
	if err != nil {
		return nil, fmt.Errorf("%w: selected catalog: %w", ErrCorrupt, err)
	}
	if sha256.Sum256(b) != current.ManifestHash {
		return nil, fmt.Errorf("%w: selected catalog digest", ErrCorrupt)
	}
	catalog, err := format.DecodeCatalog(b)
	if err != nil {
		return nil, err
	}
	if catalog.UUID != current.UUID || catalog.ActiveID != current.ActiveID || catalog.Cutoff > current.Sequence {
		return nil, fmt.Errorf("%w: catalog/frontier identity", ErrCorrupt)
	}
	image := &Image{Current: current, Catalog: catalog, Manifest: b, Files: make(map[uint64]*File), Index: make(map[string]Location)}
	files := append(append([]format.File(nil), catalog.Files...), format.File{ID: current.ActiveID, Bytes: current.ActiveEnd, Kind: format.Log})
	last := catalog.Cutoff
	for _, entry := range files {
		path := filepath.Join(dir, DataName(current.UUID, entry.ID))
		fd, err := OpenRegular(path)
		if err != nil {
			return nil, fmt.Errorf("%w: selected segment: %w", ErrCorrupt, err)
		}
		file := &File{ID: entry.ID, Kind: entry.Kind, Path: path}
		var scanErr error
		st, err := fd.Stat()
		if err != nil {
			scanErr = err
		} else if st.Size() < entry.Bytes || entry.ID != current.ActiveID && st.Size() != entry.Bytes {
			scanErr = fmt.Errorf("%w: selected file length", ErrCorrupt)
		} else {
			file.Size.Store(st.Size())
			image.Required.FileBytes = max(image.Required.FileBytes, st.Size())
			header := make([]byte, format.SegmentHeaderBytes)
			if _, err := fd.ReadAt(header, 0); err != nil {
				scanErr = fmt.Errorf("%w: segment header: %w", ErrCorrupt, err)
			} else if decoded, err := format.DecodeSegment(header); err != nil {
				scanErr = err
			} else if decoded.UUID != current.UUID || decoded.ID != entry.ID || decoded.Kind != entry.Kind {
				scanErr = fmt.Errorf("%w: segment identity", ErrCorrupt)
			} else {
				scanErr = format.Scan(fd, entry.Bytes, func(offset int64, r format.Record, key string) error {
					return applyRecord(image.Index, file, catalog.Cutoff, &last, offset, r, key)
				})
			}
		}
		if err := errors.Join(scanErr, fd.Close()); err != nil {
			return nil, err
		}
		image.Files[file.ID] = file
	}
	if last != current.Sequence {
		return nil, fmt.Errorf("%w: incomplete committed sequence", ErrCorrupt)
	}
	for _, loc := range image.Index {
		if loc.Bytes > math.MaxInt64-80 {
			return nil, fmt.Errorf("%w: unrepresentable live cache charge", ErrCorrupt)
		}
		image.Required.CacheBytes = max(image.Required.CacheBytes, loc.Bytes+80)
		image.Required.RecordBytes = max(image.Required.RecordBytes, loc.Bytes)
	}
	if image.Required.CacheBytes > cfg.CacheBytes || image.Required.RecordBytes > cfg.RecordBytes || image.Required.FileBytes > cfg.FileBytes {
		return nil, &image.Required
	}
	return image, nil
}

func applyRecord(index map[string]Location, file *File, cutoff uint64, last *uint64, offset int64, r format.Record, key string) error {
	if file.Kind == format.Snapshot {
		if r.Kind != format.Put || r.Sequence > cutoff {
			return fmt.Errorf("%w: snapshot record", ErrCorrupt)
		}
		if _, exists := index[key]; exists {
			return fmt.Errorf("%w: duplicate snapshot key", ErrCorrupt)
		}
	} else {
		if *last == math.MaxUint64 || r.Sequence != *last+1 {
			return fmt.Errorf("%w: log sequence", ErrCorrupt)
		}
		*last = r.Sequence
	}
	if r.Kind == format.Delete {
		if _, exists := index[key]; !exists {
			return fmt.Errorf("%w: tombstone without a live key", ErrCorrupt)
		}
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
	st, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != format.FrontierBytes {
		return nil, fmt.Errorf("%w: CURRENT length", ErrCorrupt)
	}
	b = make([]byte, format.FrontierBytes)
	_, err = io.ReadFull(fd, b)
	return b, err
}
