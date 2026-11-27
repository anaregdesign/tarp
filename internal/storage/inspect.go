package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

// InspectKey performs full read-only replay under the existing directory lock.
func InspectKey(ctx context.Context, dir, key string) (value []byte, found bool, result error) {
	dir, err := existingDirectoryPath(ctx, dir)
	if err != nil {
		return nil, false, err
	}
	if _, err := os.Lstat(filepath.Join(dir, "BACKUP")); err == nil {
		return nil, false, ErrBackupDirectory
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, false, err
	}
	lock, err := platform.LockExisting(dir)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if err := lock.Close(); err != nil {
			value, found, result = nil, false, errors.Join(result, err)
		}
	}()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, false, err
	}
	if len(entries) == 1 && entries[0].Name() == "LOCK" {
		return nil, false, &os.PathError{Op: "inspect", Path: filepath.Join(dir, "CURRENT"), Err: os.ErrNotExist}
	}
	image, err := readImage(ctx, dir)
	if err != nil {
		return nil, false, err
	}
	if err := (&Store{Current: image.Current}).inspectNames(ctx, entries, nil); err != nil {
		return nil, false, err
	}
	var loc Location
	_, err = image.validateWindows(ctx, dir, verifyWindowBytes, nil, func(segment *DataSegment, off int64, r format.RecordHeader, candidate string) {
		if candidate == key {
			if r.Kind == format.Delete {
				loc = Location{}
			} else {
				loc = Location{Segment: segment, RecordOffset: off, RecordBytes: r.EncodedRecordBytes(), Sequence: r.Sequence}
			}
		}
	})
	if err != nil {
		return nil, false, err
	}
	if loc.Segment == nil {
		return nil, false, nil
	}
	fd, err := OpenRegular(loc.Segment.Path)
	if err != nil {
		return nil, false, requiredFileError(filepath.Base(loc.Segment.Path), err)
	}
	value, err = format.ReadValue(fd, loc.RecordOffset, loc.RecordBytes, key, loc.Sequence)
	if err := errors.Join(err, fd.Close(), ctx.Err()); err != nil {
		return nil, false, loc.readError(err)
	}
	return value, true, nil
}
