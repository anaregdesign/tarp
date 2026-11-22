package engine

import (
	"strings"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Observer gives in-repository measurement drivers access to a DB they own.
// The caller coordinates Close with all observation calls.
type Observer struct {
	db *DB
}

func Observe(db *DB) Observer { return Observer{db} }

func (o Observer) Options() Options             { return o.db.opts }
func (o Observer) Dir() string                  { return o.db.store.Dir }
func (o Observer) CleanerDone() <-chan struct{} { return o.db.cleaner.done }

// CleanerGatedBy verifies that this measurement owns the worker's start gate.
func (o Observer) CleanerGatedBy(start <-chan struct{}) bool {
	return start != nil && o.db.hooks != nil && o.db.hooks.CleanerStart == start
}

func (o Observer) Frontier() format.Frontier {
	o.db.writer.Lock()
	defer o.db.writer.Unlock()
	return o.db.store.Current
}

func (o Observer) LiveKeys() int {
	o.db.state.Lock()
	defer o.db.state.Unlock()
	return int(o.db.stats.LiveKeys)
}

// Store observes a consistent catalog, frontier and DATA segment registry, excluding
// foreground writes and checkpoint remaps. The callback may sample Stats.
// It must not otherwise reenter the DB, mutate storage or retain live references.
func (o Observer) Store(inspect func(*storage.Store)) {
	o.db.writer.Lock()
	defer o.db.writer.Unlock()
	if o.db.memoryRemap != nil {
		o.db.indexGate.RLock()
		defer o.db.indexGate.RUnlock()
	}
	inspect(o.db.store)
}

// Cache runs a Value LRU measurement while holding the usual state lock.
func (o Observer) Cache(inspect func(*cache.Cache)) {
	o.db.state.Lock()
	defer o.db.state.Unlock()
	inspect(o.db.cache)
}

// IndexMetrics observes the backend's own counters without enumerating keys.
func (o Observer) IndexMetrics() index.Metrics {
	if m, ok := o.db.keys.(index.Meter); ok {
		return m.Metrics()
	}
	return index.Metrics{}
}

// CloneMemoryIndex reproduces the historical clone-and-GC experiment on the
// actual representation of the memory backend. Other modes return nil.
func (o Observer) CloneMemoryIndex() map[string]index.Address {
	o.db.writer.Lock()
	defer o.db.writer.Unlock()
	o.db.indexGate.RLock()
	defer o.db.indexGate.RUnlock()
	entries := o.memoryEntries()
	if entries == nil {
		return nil
	}
	copied := make(map[string]index.Address, len(entries))
	for key, addr := range entries {
		copied[strings.Clone(key)] = addr
	}
	return copied
}

// memoryEntries borrows the index map. Its caller must exclude publications
// while iterating it.
func (o Observer) memoryEntries() map[string]index.Address {
	m, ok := o.db.keys.(interface {
		RetainedEntries() map[string]index.Address
	})
	if !ok {
		return nil
	}
	return m.RetainedEntries()
}

func (o Observer) ClearCache() {
	o.db.state.Lock()
	defer o.db.state.Unlock()
	o.db.cache.Clear()
}

// CloseAndRetainMemoryIndex transfers the memory index and DATA segment registry to
// the heap experiment. The measurement coordinates Close with all other users.
func (o Observer) CloseAndRetainMemoryIndex() (map[string]index.Address, map[uint64]*storage.DataSegment, error) {
	o.db.writer.Lock()
	entries := o.memoryEntries()
	o.db.writer.Unlock()
	if err := o.db.Close(); err != nil {
		return nil, nil, err
	}
	segments := o.db.store.Segments
	o.db.store.Segments = nil
	return entries, segments, nil
}
