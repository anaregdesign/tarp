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
func parityBufferSize(bytes int64) int {
	if bytes <= format.ParityBlockBytes*format.ParityDataBlocks {
		return int(min(bytes, format.ParityBlockBytes)) + 256
	}
	return 32 << 10
}

// EnsureParity verifies or creates the sidecar for one pinned immutable DATA
// file. The caller serializes builders and holds the DATA pin through publication.
// No store-wide metadata or writer lock is needed during these operations.
func (s *Store) EnsureParity(ctx context.Context, uuid [16]byte, source CapturedFile, recheck bool, yield func() error) error {
	f := source.File
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
		if err := s.buildParity(ctx, path, uuid, source, !missing, yield); err != nil {
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

func checkParity(ctx context.Context, path string, uuid [16]byte, entry format.File) error {
	fd, err := OpenRegular(path)
	if err != nil {
		return err
	}
	err = format.ValidateParity(ctx, bufio.NewReaderSize(fd, parityBufferSize(entry.Bytes)), uuid, entry)
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
	if len(b) > 1 && off%(format.ParityDataBlocks*format.ParityBlockBytes) == 0 {
		if err := r.yield(); err != nil {
			return 0, err
		}
	}
	return r.ReaderAt.ReadAt(b, off)
}

func (s *Store) buildParity(ctx context.Context, path string, uuid [16]byte, source CapturedFile, replace bool, yield func() error) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, _, _, err := openSelected(source.File.Path, uuid, source.Entry, false)
	if err != nil {
		return &SourceError{Err: err}
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
	if yield != nil {
		input = paritySource{data, yield}
	}
	writer := bufio.NewWriterSize(&outputWriter{fs: s.IO, fd: fd, point: "parity.write"}, parityBufferSize(source.Entry.Bytes))
	err = s.IO.Do("parity.scan", func() error {
		return format.WriteParity(ctx, input, writer, uuid, source.Entry)
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
	if err := s.IO.Sync(fd, "parity.sync"); err != nil {
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
	return s.IO.Do("parity.rename", func() error {
		// An I/O error from rename can follow an applied replacement.
		source.File.parityNeedsSync = true
		if replace {
			err = os.Rename(temp, path)
		} else {
			err = platform.RenameNoReplace(temp, path)
		}
		if err == nil {
			temp = ""
		}
		return err
	})
}
