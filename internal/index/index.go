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

// Snapshot enumerates one captured index state in unspecified order.
// Its owner releases it after iteration.
type Snapshot interface {
	All(yield func(string, Address) bool) error
	Close()
}

// View also supports lookup of an encoded key by exact binary equality.
type View interface {
	Snapshot
	Lookup(key string) (Address, bool, error)
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

// Mutation replaces the address of Key. Delete removes Key and ignores Address.
type Mutation struct {
	Key     string
	Address Address
	Delete  bool
}

// LookupError identifies a failure to read the previous transaction state.
type LookupError struct{ Err error }

func (e *LookupError) Error() string { return e.Err.Error() }
func (e *LookupError) Unwrap() error { return e.Err }

// Tx stages operations for a single writer. Lookup and Apply observe earlier
// staged mutations. A writer must call exactly one of Commit or Abort. Commit
// makes the prepared state visible without further fallible I/O.
type Tx interface {
	Mutations
	// Apply returns the previous address and presence. Deleting a missing
	// key is a no-op. On LookupError the previous state is unavailable.
	// An error preparing the mutation still returns the previous state.
	Apply(Mutation) (Address, bool, error)
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
