package engine

import (
	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/format"
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

func (o Observer) Frontier() format.Frontier {
	o.db.writer.Lock()
	defer o.db.writer.Unlock()
	return o.db.store.Current
}

func (o Observer) LiveKeys() int {
	o.db.state.Lock()
	defer o.db.state.Unlock()
	return len(o.db.index)
}

// Store observes a consistent catalog, frontier and file registry under the
// writer gate. The callback may nest State. It must not mutate store state
// or retain live references after returning.
func (o Observer) Store(inspect func(*storage.Store)) {
	o.db.writer.Lock()
	defer o.db.writer.Unlock()
	inspect(o.db.store)
}

// State observes the index and cache under the state lock. The callback may
// probe LRU hits. It must not change the index or retain live references.
func (o Observer) State(inspect func(map[string]storage.Location, *cache.Cache)) {
	o.db.state.Lock()
	defer o.db.state.Unlock()
	inspect(o.db.index, o.db.cache)
}

func (o Observer) ClearCache() {
	o.db.state.Lock()
	defer o.db.state.Unlock()
	o.db.cache = cache.New(o.db.opts.CacheBytes)
}

// CloseAndRetainIndex transfers ownership of the actual index and file
// registry to a heap probe. Close joins the worker and releases pins and open
// handles before the probe receives either map.
func (o Observer) CloseAndRetainIndex() (map[string]storage.Location, map[uint64]*storage.File, error) {
	o.db.state.Lock()
	index := o.db.index
	o.db.state.Unlock()
	if err := o.db.Close(); err != nil {
		return nil, nil, err
	}
	files := o.db.store.Files
	o.db.store.Files = nil
	return index, files, nil
}
