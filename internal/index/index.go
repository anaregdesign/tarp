// Package index defines the disposable key-to-record index shared by the
// engine, verified DATA replay and maintenance.
package index

// Address identifies one checked DATA record independently of process-local
// file handles. A compaction may change its physical address at the same sequence.
type Address struct {
	FileID                    uint64
	Offset, Bytes, ValueBytes int64
	Sequence                  uint64
}

// View captures a single index state. The owner releases it after iteration.
// Keys use exact binary equality. All does not promise an iteration order.
type View interface {
	Lookup(key string) (Address, bool, error)
	All(yield func(string, Address) bool) error
	Close()
}

// BatchView is implemented by views that can share page reads across keys.
type BatchView interface {
	LookupMany(keys []string, set func(int, Address, bool)) error
}

// LookupMany preserves each input index even when the backend processes keys
// in a different order. Callers discard partial results after an error.
func LookupMany(v View, keys []string, set func(int, Address, bool)) error {
	if grouped, ok := v.(BatchView); ok {
		return grouped.LookupMany(keys, set)
	}
	for i, key := range keys {
		address, found, err := v.Lookup(key)
		if err != nil {
			return err
		}
		set(i, address, found)
	}
	return nil
}

// Tx stages operations for a single writer. Lookup observes earlier staged
// mutations. A writer must call exactly one of Commit or Abort. Commit makes
// the prepared state visible without further fallible I/O.
type Tx interface {
	Mutations
	Commit()
	Abort()
}

// ResidentLookup optionally serves an infallible memory-only lookup while the
// caller excludes Commit, Relocate and Close. Borrowing key bytes avoids copies for misses.
type ResidentLookup interface {
	LookupResident(key []byte) (Address, bool)
}

// ResidentRelocator conditionally changes a published physical address in memory.
// The caller excludes commits, close, resident reads and staged mutations of key.
type ResidentRelocator interface {
	Relocate(key string, old, next Address) bool
}

// Backend has one writer and permits concurrent readers. Lookup reads the
// current state without constructing a full enumeration view.
type Backend interface {
	Lookup(key string) (Address, bool, error)
	View() View
	Begin() Tx
	Count() int
	Close() error
}

// Meter exposes the physical activity of a paged backend.
type Meter interface{ Metrics() Metrics }

type Metrics struct {
	PageReads, BranchReads, LeafReads, OverflowReads uint64
	// CacheBytes charges page buffers and their cache entries. ScratchBytes
	// sums logical file sizes, including sparse regions and allocator journals.
	CacheBytes, CachePeakBytes, ScratchBytes, ScratchPeakBytes uint64
	// Retired pages await the next writer or the oldest pinned generation.
	ActiveViews, PinnedGenerations, RetiredPages uint64
	// SpillNanoseconds measures elapsed synchronous dirty-page write calls.
	Evictions, SpillWrites, SpillNanoseconds uint64
	// Each active read reserves room for page validation, overflow comparison
	// and bounded routing. Suspended cursors release their reservation.
	ReadWorkspaceBytes, ReadWorkspacePeakBytes, ReadWorkspaceLimitBytes uint64
	ReadWaits, ReadWaitNanoseconds                                      uint64
}
