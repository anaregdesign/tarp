package storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

// Small segments normally fit in one buffered write. Larger sidecars batch
// several stripes of metadata and parity into each filesystem request.
func parityIOBufferBytes(bytes int64) int {
	if bytes <= format.DataBlockBytes*format.DataBlocksPerStripe {
		return int(min(bytes, format.DataBlockBytes)) + 256
	}
	return 32 << 10
}

// EnsureParitySidecar verifies or creates the sidecar for one pinned immutable DATA
// file. The caller serializes builders and holds the DATA pin through publication.
// No store-wide metadata or writer lock is needed during these operations.
func (s *Store) EnsureParitySidecar(ctx context.Context, uuid [16]byte, source CapturedFile, recheck bool, yield func() error) error {
	f := source.File
	if unavailable := f.UnavailableData(); unavailable != nil {
		if !recheck {
			return unavailable
		}
		// An explicit recheck must establish that DATA is readable before
		// using an already-valid sidecar as evidence of protection.
		if err := s.RepairRead(ctx, f, &SourceError{Err: ErrCorrupt}); err != nil {
			return err
		}
	}
	if f.parityReady && !recheck {
		return nil
	}
	f.parityReady = false
	path := filepath.Join(s.Dir, parityName(uuid, f.ID))
	err := checkParity(ctx, path, uuid, source.Entry)
	missing := errors.Is(err, os.ErrNotExist)
	switch {
	case err == nil:
	case missing || errors.Is(err, format.ErrInvalidParity):
		if err := s.RetryMaintenance(ctx, []CapturedFile{source}, func() error {
			return s.buildParity(ctx, uuid, source, !missing, yield, "parity")
		}, nil); err != nil {
			return err
		}
	default:
		return err
	}
	if f.parityNeedsSync {
		if err := s.IO.SyncDir(s.Dir, "parity.dirsync"); err != nil {
			return err
		}
		f.parityNeedsSync = false
	}
	f.parityReady = true
	return nil
}

func checkParity(ctx context.Context, path string, uuid [16]byte, entry format.CatalogEntry) error {
	fd, err := OpenRegular(path)
	if err != nil {
		return err
	}
	err = format.ValidateParitySidecar(ctx, bufio.NewReaderSize(fd, parityIOBufferBytes(entry.Bytes)), uuid, entry)
	if closeErr := fd.Close(); closeErr != nil {
		return closeErr
	}
	return err
}

type paritySource struct {
	io.ReaderAt
	yield func() error
}

func (r paritySource) ReadAt(b []byte, off int64) (int, error) {
	// Pace stripe reads. The codec also performs small header lookaheads
	// and one final EOF probe, which need no separate delay.
	if len(b) > 1 && off%(format.DataBlocksPerStripe*format.DataBlockBytes) == 0 {
		if err := r.yield(); err != nil {
			return 0, err
		}
	}
	return r.ReaderAt.ReadAt(b, off)
}

func (s *Store) buildParity(ctx context.Context, uuid [16]byte, source CapturedFile, replace bool, yield func() error, point string) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, _, _, err := openSelected(source.File.Path, uuid, source.Entry, false)
	if err != nil {
		return selectedReadFailure(source.Entry.ID, &SourceError{Err: err})
	}
	defer func() {
		if err := data.Close(); err != nil {
			result = errors.Join(result, &SourceError{Err: err})
		}
	}()
	dir, err := s.WorkspaceDir()
	if err != nil {
		return err
	}
	fd, err := os.CreateTemp(dir, "tarp-parity-*")
	if err != nil {
		return err
	}
	temp := fd.Name()
	defer func() {
		if fd != nil {
			result = errors.Join(result, fd.Close())
		}
		if temp != "" {
			if err := os.Remove(temp); !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
	}()

	var input io.ReaderAt = data
	var paceErr error
	if yield != nil {
		input = paritySource{data, func() error {
			paceErr = yield()
			return paceErr
		}}
	}
	writer := bufio.NewWriterSize(&outputWriter{fs: s.IO, fd: fd, point: point + ".write"}, parityIOBufferBytes(source.Entry.Bytes))
	err = s.IO.Do(point+".scan", func() error {
		err := format.WriteParitySidecar(ctx, input, writer, uuid, source.Entry)
		if err != nil && paceErr == nil {
			return selectedReadFailure(source.Entry.ID, err)
		}
		return err
	})
	if err != nil {
		var dst *destinationError
		if !errors.As(err, &dst) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return &SourceError{Err: fmt.Errorf("DATA %d: %w", source.Entry.ID, err)}
		}
		return err
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	// Read back the actual staged bytes. A write may have reported success
	// while producing an incomplete or corrupted optional file.
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := format.ValidateParitySidecar(ctx, bufio.NewReaderSize(fd, parityIOBufferBytes(source.Entry.Bytes)), uuid, source.Entry); err != nil {
		return err
	}
	if err := s.IO.Sync(fd, point+".sync"); err != nil {
		return err
	}
	err = fd.Close()
	fd = nil
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(s.Dir, parityName(uuid, source.Entry.ID))
	return s.IO.Do(point+".rename", func() error {
		// An I/O error from rename can follow an applied replacement.
		if replace {
			err = os.Rename(temp, path)
		} else {
			err = platform.RenameNoReplace(temp, path)
		}
		source.File.parityNeedsSync = !errors.Is(err, os.ErrExist)
		if err == nil {
			temp = ""
		}
		return err
	})
}

// Remove only names owned by this unpublished output. An after-rename error
// can leave an installed sidecar even though preparation did not finish.
func (f *DataSegment) discardReplacement(fs *IO, uuid [16]byte, point string) error {
	remove := func(path string) error {
		err := fs.Do(point, func() error { return os.Remove(path) })
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	var parityErr error
	if f.parityNeedsSync || f.parityReady {
		parityErr = remove(filepath.Join(filepath.Dir(f.Path), parityName(uuid, f.ID)))
	}
	return errors.Join(parityErr, remove(f.Path))
}
