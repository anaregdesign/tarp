package storage

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

func (s *Store) retireData(f *File) {
	if s.retired == nil {
		s.retired = make(map[uint64]*File)
	}
	s.retired[f.ID] = f
}

// QueueFailedCleanDiscard runs under the writer gate after an unpublished
// output failed to discard. An unlink can have succeeded before the error,
// so the directory still needs syncing even if the path is already gone.
func (s *Store) QueueFailedCleanDiscard(c *CleanSegment) {
	s.retireData(c.output)
	s.retireSyncPending = true
}

func (s *Store) retireManifest(id uint64) {
	if id != 0 {
		if s.retiredCatalogs == nil {
			s.retiredCatalogs = make(map[uint64]struct{})
		}
		s.retiredCatalogs[id] = struct{}{}
	}
}

func (s *Store) retireIndex(id uint64) {
	if s.retiredIndexes == nil {
		s.retiredIndexes = make(map[uint64]struct{})
	}
	s.retiredIndexes[id] = struct{}{}
}

// Manual cleanup and the automatic worker can finish each other's pending sync.
// Both callers hold the writer gate.
func (s *Store) syncRetirements(point string) error {
	if !s.retireSyncPending {
		return nil
	}
	err := s.IO.SyncDir(s.Dir, point)
	s.retireSyncPending = err != nil
	return err
}

// RetireCleaned removes only known obsolete objects. The caller holds the
// writer gate and checks health. Pin release itself never performs file I/O.
func (s *Store) RetireCleaned() (unlinked, pinned int64, pending bool, result error) {
	const batch = 16
	remaining := batch
	remove := func(path string) (removed bool, err error) {
		st, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if !st.Mode().IsRegular() {
			return false, fmt.Errorf("%w: retired object is not a regular file", ErrCorrupt)
		}
		// Return exact logical byte counts for every batch, including when
		// extremely large sparse files would overflow a single result.
		if st.Size() > math.MaxInt64-unlinked {
			remaining = 0
			return false, nil
		}
		err = s.IO.Do("clean.retire.unlink", func() error {
			err := os.Remove(path)
			if err == nil {
				removed = true
				unlinked += st.Size()
				s.retireSyncPending = true
				remaining--
			}
			return err
		})
		if errors.Is(err, os.ErrNotExist) {
			// Manual cleanup may already have removed this known object.
			return true, nil
		}
		return removed, err
	}
	for id, f := range s.retired {
		if f.References() != 0 {
			pinned += min(f.Size.Load(), math.MaxInt64-pinned)
			continue
		}
		if remaining == 0 || result != nil {
			continue
		}
		if err := s.readers.forget(f); err != nil {
			result = err
			continue
		}
		// Release recovery information before its source. Keep the registry
		// entry until both names are gone so a partial retirement can retry.
		removedParity, err := remove(filepath.Join(s.Dir, parityName(s.Current.UUID, id)))
		if err != nil || !removedParity || remaining == 0 {
			result = err
			continue
		}
		removed, err := remove(f.Path)
		if removed {
			delete(s.retired, id)
			delete(s.Files, id)
		}
		result = err
	}
	for id := range s.retiredCatalogs {
		if remaining == 0 || result != nil {
			break
		}
		if id == s.Current.ManifestID {
			result = fmt.Errorf("%w: selected catalog scheduled for retirement", ErrCorrupt)
			break
		}
		path := filepath.Join(s.Dir, ManifestName(s.Current.UUID, id))
		removed, err := remove(path)
		if removed {
			delete(s.retiredCatalogs, id)
		}
		result = err
	}
	for id := range s.retiredIndexes {
		if remaining == 0 || result != nil {
			break
		}
		if id == s.Catalog.Index.ID {
			result = fmt.Errorf("%w: retiring selected index", ErrCorrupt)
			break
		}
		removed, err := remove(filepath.Join(s.Dir, IndexName(s.Current.UUID, id)))
		if removed {
			delete(s.retiredIndexes, id)
		}
		result = err
	}
	result = errors.Join(result, s.syncRetirements("clean.retire.dirsync"))
	return unlinked, pinned, len(s.retired)+len(s.retiredCatalogs)+len(s.retiredIndexes) != 0 || s.retireSyncPending, result
}
