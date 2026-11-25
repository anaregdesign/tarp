// Package index defines the disposable key-to-record index shared by the
// engine, verified DATA replay and maintenance.
package index

// Address identifies one DATA record by file ID, segment-relative byte offset
// and encoded record length. Compaction can move it at the same user sequence.
type Address struct {
	FileID                                uint64
	RecordOffset, RecordBytes, ValueBytes int64
	Sequence                              uint64
}

// Snapshot enumerates one captured index state. All uses unspecified order,
// Range uses ascending order, and RangeDesc uses descending order. Both ranges
// are half-open with an empty upper bound denoting the end of the index.
type Snapshot interface {
	All(yield func(string, Address) bool) error
	Range(lower, upper string, yield func(string, Address) bool) error
	RangeDesc(lower, upper string, yield func(string, Address) bool) error
	Close()
}

// KeyLookup resolves an encoded key by exact binary equality.
type KeyLookup interface {
	Lookup(key string) (Address, bool, error)
}

// View supports point lookups and enumeration at the same captured state.
type View interface {
	Snapshot
	KeyLookup
}

// BatchView is implemented by views that can share page reads across keys.
type BatchView interface {
	LookupMany(keys []string, set func(int, Address, bool)) error
}

// LookupMany preserves each input index even when the backend processes keys
// in a different order. Callers discard partial results after an error.
func LookupMany(v KeyLookup, keys []string, set func(int, Address, bool)) error {
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
	KeyLookup
	View() View
	Begin() Tx
	Count() int
	Close() error
}

// Meter reports B+tree pager activity. A file read may hit the OS page cache.
type Meter interface{ Metrics() Metrics }

type Metrics struct {
	PageReads, BranchReads, LeafReads, OverflowReads uint64
	// CacheBytes charges page buffers and their cache entries. ScratchBytes
	// sums logical file sizes, including sparse regions and allocator journals.
	CacheBytes, CachePeakBytes, ScratchBytes, ScratchPeakBytes uint64
	// Retired pages await the next writer or the oldest pinned root generation.
	ActiveViews, PinnedRootGenerations, RetiredPages uint64
	// SpillNanoseconds measures elapsed synchronous dirty-page write calls.
	Evictions, SpillWrites, SpillNanoseconds uint64
	// Each active read reserves room for page validation, overflow comparison
	// and bounded routing. Suspended cursors release their reservation.
	ReadWorkspaceBytes, ReadWorkspacePeakBytes, ReadWorkspaceLimitBytes uint64
	ReadWaits, ReadWaitNanoseconds                                      uint64
}
