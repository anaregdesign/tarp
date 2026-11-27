package engine

import (
	"maps"
	"sync"

	"github.com/anaregdesign/tarp/internal/storage"
)

// A set describes the DATA membership captured with an index view. Its files
// never change. Multiple index revisions can have the same DATA membership.
type scanSegmentSet struct {
	files map[uint64]*storage.DataSegment
	refs  int // guarded by scanSegmentSets.mu
}

type scanSegmentSets struct {
	mu      sync.Mutex
	current *scanSegmentSet // reusable even when no readers hold it
	older   map[*scanSegmentSet]struct{}
}

// Call while holding indexGate so the set matches the captured index view.
func (db *DB) pinScanSegments() *scanSegmentSet {
	sets := &db.readSegments
	sets.mu.Lock()
	defer sets.mu.Unlock()
	if sets.current == nil {
		sets.current = &scanSegmentSet{files: maps.Clone(db.segments)}
	}
	sets.current.refs++
	return sets.current
}

// Index publication invalidates the reusable map only when membership changes.
// The next capture, if any, pays for cloning the map. Existing views retain
// their set until their last reader leaves.
func (sets *scanSegmentSets) invalidate() {
	sets.mu.Lock()
	defer sets.mu.Unlock()
	if sets.current != nil && sets.current.refs != 0 {
		if sets.older == nil {
			sets.older = make(map[*scanSegmentSet]struct{})
		}
		sets.older[sets.current] = struct{}{}
	}
	sets.current = nil
}

// Close has drained admitted work and stopped maintenance. Outstanding views
// still own their set pointers and can release them after the registry is gone.
func (sets *scanSegmentSets) clear() {
	sets.mu.Lock()
	sets.current = nil
	sets.older = nil
	sets.mu.Unlock()
}

func (db *DB) unpinScanSegments(set *scanSegmentSet) {
	if set == nil {
		return
	}
	sets := &db.readSegments
	sets.mu.Lock()
	set.refs--
	releasedOlder := set.refs == 0 && set != sets.current
	if releasedOlder {
		delete(sets.older, set)
	}
	sets.mu.Unlock()
	if releasedOlder {
		db.notifyCleaner()
	}
}

// A retired file with zero live index references cannot enter a future view.
// A view that captured it earlier registered here under indexGate before it
// could lose its final index reference. Storage can therefore check live
// references first, then ask whether any view still retains the file.
func (sets *scanSegmentSets) retains(id uint64) bool {
	sets.mu.Lock()
	defer sets.mu.Unlock()
	if sets.current != nil && sets.current.refs != 0 && sets.current.files[id] != nil {
		return true
	}
	for set := range sets.older {
		if set.files[id] != nil {
			return true
		}
	}
	return false
}
