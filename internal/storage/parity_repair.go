package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
)

// RepairParity takes ordinary exclusive database ownership without running
// Open's recovery or cleanup. Each selected sealed file is a separate repair.
func RepairParity(ctx context.Context, dir string, fs *IO) (result error) {
	s, image, err := lockParityImage(ctx, dir, fs)
	if err != nil {
		return err
	}
	repaired := false
	defer func() {
		result = errors.Join(result, s.Close())
		var uncertain *Fault
		if repaired && result != nil && !errors.As(result, &uncertain) {
			result = fault("repair.partial", publication.Committed, false, result)
		}
	}()
	var failures error
	for _, entry := range image.Catalog.Files {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, failures)
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		changed, err := s.repairSealed(ctx, entry)
		repaired = repaired || changed
		if err != nil {
			err = fmt.Errorf("%s: %w", DataName(s.Current.UUID, entry.ID), err)
			var f *Fault
			if errors.As(err, &f) && f.Outcome == publication.Unknown {
				return errors.Join(err, failures)
			}
			failures = errors.Join(failures, err)
		}
	}
	if failures != nil {
		return failures
	}
	// Verify the unchanged active prefix and logical replay as well. Never use
	// INDEX or repair an active tail, even if its bytes resemble valid records.
	_, err = validateReadOnly(ctx, s.Dir, verifyWindowBytes)
	return err
}

// lockParityImage authenticates the selected metadata before either offline
// reader inspects damaged files. The caller owns the store lock on success.
func lockParityImage(ctx context.Context, dir string, fs *IO) (_ *Store, _ *Image, result error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	s, err := lockStore(ctx, dir, Config{}, true, fs)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, s.Close())
		}
	}()
	if _, err := os.Lstat(filepath.Join(s.Dir, "BACKUP")); err == nil {
		return nil, nil, ErrBackupDirectory
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	image, err := readImage(ctx, s.Dir)
	if err != nil {
		return nil, nil, err
	}
	s.Current = image.Current
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, nil, err
	}
	if _, err := s.inspectNames(ctx, entries); err != nil {
		return nil, nil, err
	}
	return s, image, nil
}

func (s *Store) repairSealed(ctx context.Context, entry format.File) (bool, error) {
	path := filepath.Join(s.Dir, DataName(s.Current.UUID, entry.ID))
	// A completely missing short segment can still be reconstructed when
	// each stripe is missing only one block. The codec decides from lengths.
	var data io.ReaderAt = bytes.NewReader(nil)
	var damage error
	fd, err := OpenRegular(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		damage = requiredFileError(filepath.Base(path), err)
	} else {
		info, statErr := fd.Stat()
		if statErr != nil {
			return false, errors.Join(statErr, fd.Close())
		}
		data = fd
		if info.Size() == entry.Bytes {
			damage = verifySelectedDigest(ctx, fd, entry)
			if damage == nil || (!errors.Is(damage, ErrCorrupt) && !errors.Is(damage, syscall.EIO)) {
				return false, errors.Join(damage, fd.Close())
			}
		} else {
			damage = fmt.Errorf("%w: expected %d bytes, found %d", ErrCorrupt, entry.Bytes, info.Size())
		}
	}
	stage, err := s.stageParityRepair(ctx, data, entry)
	if fd != nil {
		err = errors.Join(err, fd.Close())
	}
	if err != nil {
		return false, errors.Join(damage, err, discardRepair(stage))
	}
	// Sync and close the complete, verified candidate before a single atomic
	// replacement. A later offline retry rechecks the authoritative digest.
	if err := errors.Join(ctx.Err(), s.IO.Check("repair.rename:before")); err != nil {
		return false, errors.Join(err, discardRepair(stage))
	}
	if err := os.Rename(stage, path); err != nil {
		return false, fault("repair.rename", publication.Unknown, false, errors.Join(err, discardRepair(stage)))
	}
	if err := s.IO.Check("repair.rename:after"); err != nil {
		return false, fault("repair.rename", publication.Unknown, false, err)
	}
	if err := s.IO.SyncDir(s.Dir, "repair.dirsync"); err != nil {
		return false, fault("repair.dirsync", publication.Unknown, false, err)
	}
	return true, nil
}

func discardRepair(name string) error {
	if name == "" {
		return nil
	}
	err := os.Remove(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *Store) stageParityRepair(ctx context.Context, data io.ReaderAt, entry format.File) (stage string, result error) {
	sidecar, err := OpenRegular(filepath.Join(s.Dir, parityName(s.Current.UUID, entry.ID)))
	if err != nil {
		return "", err
	}
	defer func() { result = errors.Join(result, sidecar.Close()) }()
	pattern := fmt.Sprintf(".tarp-current-%x-*.tmp", s.Current.UUID)
	fd, err := os.CreateTemp(s.Dir, pattern)
	if err != nil {
		return "", err
	}
	defer func() { result = errors.Join(result, fd.Close()) }()
	stage = fd.Name()
	out := &outputWriter{fs: s.IO, fd: fd, point: "repair.write"}
	if err := format.RecoverParity(ctx, data, sidecar, out, s.Current.UUID, entry); err != nil {
		return stage, err
	}
	info, err := fd.Stat()
	if err != nil {
		return stage, err
	}
	if info.Size() != entry.Bytes {
		return stage, fmt.Errorf("%w: wrong length after parity repair", ErrCorrupt)
	}
	// Read back the staged file; a successful write count alone cannot prove
	// that the destination contains the bytes validated by the decoder.
	if err := verifySelectedDigest(ctx, fd, entry); err != nil {
		return stage, err
	}
	return stage, s.IO.Sync(fd, "repair.sync")
}
