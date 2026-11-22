package storage

import (
	"bufio"
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

// RepairDataFromParity takes ordinary exclusive database ownership without running
// Open's recovery or cleanup. Each selected sealed file is a separate repair.
func RepairDataFromParity(ctx context.Context, dir string, fs *IO) (result error) {
	s, image, err := lockParityImage(ctx, dir, fs)
	if err != nil {
		return err
	}
	repaired := false
	defer func() {
		result = errors.Join(result, s.Close())
		var uncertain *Fault
		if repaired && result != nil && !errors.As(result, &uncertain) {
			result = fault("data-repair.partial", publication.Committed, false, result)
		}
	}()
	var failures error
	for _, entry := range image.Catalog.Entries {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, failures)
		}
		if !entry.HasPhysicalFile() {
			continue
		}
		changed, err := s.repairSealedData(ctx, s.Current.UUID, entry, false)
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
	if err := s.inspectNames(ctx, entries, nil); err != nil {
		return nil, nil, err
	}
	return s, image, nil
}

func (s *Store) repairSealedData(ctx context.Context, uuid [16]byte, entry format.CatalogEntry, online bool) (bool, error) {
	path := filepath.Join(s.Dir, DataName(uuid, entry.ID))
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
	stage, err := s.stageDataRepair(ctx, data, uuid, entry, online)
	if fd != nil {
		if closeErr := fd.Close(); closeErr != nil {
			stage.unavailable = false
			err = errors.Join(err, closeErr)
		}
	}
	if err != nil {
		removeErr := discardRepair(stage.path)
		if onlyCauses(err, context.Canceled, context.DeadlineExceeded) {
			return false, errors.Join(err, removeErr)
		}
		err = errors.Join(damage, err, removeErr)
		if online && stage.unavailable && removeErr == nil {
			// Missing DATA and PARITY can also signal a missing database
			// directory. Confirm the live root before isolating this file.
			// A concurrent publication may select a different generation.
			if rootErr := s.checkRepairRoot(uuid); rootErr != nil {
				return false, errors.Join(err, rootErr)
			}
			return false, &UnavailableDataError{FileID: entry.ID, Err: err}
		}
		return false, err
	}
	// Sync and close the complete, verified candidate before a single atomic
	// replacement. A later offline retry rechecks the authoritative digest.
	if err := errors.Join(ctx.Err(), s.IO.Check("data-repair.rename:before")); err != nil {
		return false, errors.Join(err, discardRepair(stage.path))
	}
	if err := os.Rename(stage.path, path); err != nil {
		return false, fault("data-repair.rename", publication.Unknown, false, errors.Join(err, discardRepair(stage.path)))
	}
	if err := s.IO.Check("data-repair.rename:after"); err != nil {
		return false, fault("data-repair.rename", publication.Unknown, false, err)
	}
	if err := s.IO.SyncDir(s.Dir, "data-repair.dirsync"); err != nil {
		return false, fault("data-repair.dirsync", publication.Unknown, false, err)
	}
	return true, nil
}

func (s *Store) checkRepairRoot(uuid [16]byte) error {
	b, err := readCurrent(filepath.Join(s.Dir, "CURRENT"))
	if err != nil {
		return err
	}
	current, err := format.DecodeFrontier(b)
	if err == nil && current.UUID != uuid {
		return fmt.Errorf("%w: database identity changed during DATA repair", ErrCorrupt)
	}
	return err
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

type repairStage struct {
	path        string
	unavailable bool // Unusable inputs or insufficient private staging space, without another failure.
}

func repairSpaceError(err error) bool {
	return onlyCauses(err, syscall.ENOSPC, syscall.EDQUOT)
}

// The decoder forwards errors from the opened inputs and the private output.
// A media read failure is local to this recovery attempt. An output I/O
// failure can indicate a broader storage fault.
func unavailableReconstruction(err error) bool {
	var output *destinationError
	if errors.As(err, &output) {
		return repairSpaceError(err)
	}
	return onlyCauses(err, ErrCorrupt, syscall.EIO)
}

func (s *Store) stageDataRepair(ctx context.Context, data io.ReaderAt, uuid [16]byte, entry format.CatalogEntry, online bool) (stage repairStage, result error) {
	sidecar, err := OpenRegular(filepath.Join(s.Dir, parityName(uuid, entry.ID)))
	if err != nil {
		stage.unavailable = errors.Is(err, os.ErrNotExist)
		return stage, err
	}
	closeFile := func(fd *os.File) {
		if err := fd.Close(); err != nil {
			stage.unavailable = false
			result = errors.Join(result, err)
		}
	}
	defer closeFile(sidecar)
	// Live cleanup enumerates the database root while an online repair can be
	// writing. The private workspace is swept only after Open validation or
	// after all admitted operations have drained on Close.
	dir, pattern := s.Dir, fmt.Sprintf(".tarp-current-%x-*.tmp", uuid)
	if online {
		dir, err = s.WorkspaceDir()
		if err != nil {
			stage.unavailable = repairSpaceError(err)
			return stage, err
		}
		pattern = "tarp-repair-*"
	}
	fd, err := os.CreateTemp(dir, pattern)
	if err != nil {
		stage.unavailable = repairSpaceError(err)
		return stage, err
	}
	defer closeFile(fd)
	stage.path = fd.Name()
	out := bufio.NewWriterSize(&outputWriter{fs: s.IO, fd: fd, point: "data-repair.write"}, parityIOBufferBytes(entry.Bytes))
	if err := format.RecoverDataFromParity(ctx, data, sidecar, out, uuid, entry); err != nil {
		stage.unavailable = unavailableReconstruction(err)
		return stage, err
	}
	if err := out.Flush(); err != nil {
		stage.unavailable = repairSpaceError(err)
		return stage, err
	}
	info, err := fd.Stat()
	if err != nil {
		return stage, err
	}
	if info.Size() != entry.Bytes {
		return stage, fmt.Errorf("%w: wrong length after DATA repair", ErrCorrupt)
	}
	// Read back the staged file; a successful write count alone cannot prove
	// that the destination contains the bytes validated by the decoder.
	if err := verifySelectedDigest(ctx, fd, entry); err != nil {
		return stage, err
	}
	err = s.IO.Sync(fd, "data-repair.sync")
	stage.unavailable = repairSpaceError(err)
	return stage, err
}
