package storage

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
)

const autoRetirementVisits = 32

// Automatic retirement walks a FIFO cycle across bounded writer-gate passes.
// Keep the last completed pin observation while the next cycle is in flight.
type retiredDataScan struct {
	ids       list.List
	remaining int
	added     bool
	pinned    int64
	last      int64
}

func (s *Store) retireData(f *DataSegment) {
	if s.retired == nil {
		s.retired = make(map[uint64]*DataSegment)
	}
	if s.retired[f.ID] == nil {
		work := &s.retiredData
		work.ids.PushBack(f.ID)
		work.added = work.added || work.remaining != 0
	}
	s.retired[f.ID] = f
}

// QueueFailedCleanDiscard runs under the writer gate after an unpublished
// output failed to discard. An unlink can have succeeded before the error,
// so the directory still needs syncing even if the path is already gone.
func (s *Store) QueueFailedCleanDiscard(c *SegmentCleaning) {
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
// Pinned bytes reflect the most recent completed sweep of known DATA files.
func (s *Store) RetireCleaned() (unlinked, pinned int64, pending bool, result error) {
	return s.retireKnown(16, autoRetirementVisits)
}

// RetirementSweepPending reports unvisited DATA in the current automatic
// sweep. A pinned file already visited can wait for a later notification.
func (s *Store) RetirementSweepPending() bool {
	return s.retiredData.remaining != 0 || s.retiredData.added
}

// RetireAfterCheckpoint visits the complete set of known retirements once and
// syncs successful unlinks together, as manual directory cleanup does.
func (s *Store) RetireAfterCheckpoint() (unlinked, pinned int64, err error) {
	s.retiredData.remaining = 0
	unlinked, pinned, _, err = s.retireKnown(math.MaxInt, math.MaxInt)
	return unlinked, pinned, err
}

func (s *Store) retireKnown(remaining, visits int) (unlinked, pinned int64, pending bool, result error) {
	manual := visits == math.MaxInt
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
	work := &s.retiredData
	if work.remaining == 0 {
		work.remaining, work.added, work.pinned = work.ids.Len(), false, 0
	}
	for observed := 0; work.remaining > 0 && observed < visits; observed++ {
		if !manual && (remaining == 0 || result != nil) {
			break
		}
		item := work.ids.Front()
		id := item.Value.(uint64)
		f := s.retired[id]
		work.remaining--
		if f == nil {
			// Directory cleanup can retire this ID while a sweep is parked.
			work.ids.Remove(item)
			continue
		}
		work.ids.MoveToBack(item)
		if s.DataReferenced(f) {
			work.pinned += min(f.Size.Load(), math.MaxInt64-work.pinned)
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
		// A staged output whose header failed may own only its DATA name.
		// A separately existing sidecar never becomes ours on EEXIST.
		if f.sealed.Load() != nil || f.parityNeedsSync || f.ParityReady() {
			removedParity, err := remove(filepath.Join(s.Dir, parityName(s.Current.UUID, id)))
			if err != nil || !removedParity || remaining == 0 {
				result = err
				continue
			}
		}
		removed, err := remove(f.Path)
		if removed {
			work.ids.Remove(item)
			delete(s.retired, id)
			delete(s.Segments, id)
		}
		result = err
	}
	if work.remaining == 0 {
		work.last = work.pinned
	}
	if len(s.retired) == 0 {
		*work = retiredDataScan{}
	}
	pinned = work.last
	metadataVisits := visits
	for id := range s.retiredCatalogs {
		if remaining == 0 || result != nil || metadataVisits == 0 {
			break
		}
		metadataVisits--
		if slices.Contains(s.catalogChain, id) {
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
		if remaining == 0 || result != nil || metadataVisits == 0 {
			break
		}
		metadataVisits--
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
