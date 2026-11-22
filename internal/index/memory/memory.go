// Package memory implements the in-memory index for datasets that fit in RAM.
package memory

import (
	"context"
	"maps"
	"os"
	"sync"
	"sync/atomic"

	"github.com/anaregdesign/tarp/internal/index"
)

type store struct {
	writer  sync.Mutex
	mu      sync.RWMutex
	entries map[string]index.Address
	closed  atomic.Bool
}

func New() index.Backend { return &store{entries: make(map[string]index.Address)} }

func (s *store) Lookup(key string) (index.Address, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed.Load() {
		return index.Address{}, false, os.ErrClosed
	}
	addr, ok := s.entries[key]
	return addr, ok, nil
}

func (s *store) LookupResident(key []byte) (index.Address, bool) {
	addr, ok := s.entries[string(key)]
	return addr, ok
}

func (s *store) Relocate(key string, old, next index.Address) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.entries[key]; !ok || a != old {
		return false
	}
	s.entries[key] = next
	return true
}

func (s *store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

func (s *store) View() index.View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &view{owner: s, entries: maps.Clone(s.entries)}
}

// BoundedView captures matching addresses under the same lock as View.
// Iteration over the copy permits user code to mutate or close the store.
func (s *store) BoundedView(ctx context.Context, bounds index.Bounds) (index.View, error) {
	done := ctx.Done()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries := make(map[string]index.Address)
	for key, addr := range s.entries {
		select {
		case <-done:
			return nil, ctx.Err()
		default:
		}
		if bounds.Contains(key) {
			entries[key] = addr
		}
	}
	return &view{owner: s, entries: entries}, nil
}

func (s *store) Begin() index.Tx {
	s.writer.Lock()
	return &transaction{owner: s}
}

func (s *store) Close() error {
	s.writer.Lock()
	defer s.writer.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed.Store(true)
	s.entries = nil
	return nil
}

type view struct {
	owner   *store
	mu      sync.RWMutex
	entries map[string]index.Address
}

func (v *view) Lookup(key string) (index.Address, bool, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.owner.closed.Load() || v.entries == nil {
		return index.Address{}, false, os.ErrClosed
	}
	addr, ok := v.entries[key]
	return addr, ok, nil
}

func (v *view) All(yield func(string, index.Address) bool) error {
	v.mu.RLock()
	entries := v.entries
	v.mu.RUnlock()
	if entries == nil || v.owner.closed.Load() {
		return os.ErrClosed
	}
	for key, addr := range entries {
		if v.owner.closed.Load() {
			return os.ErrClosed
		}
		if !yield(key, addr) {
			break
		}
	}
	return nil
}

func (v *view) Close() {
	v.mu.Lock()
	v.entries = nil
	v.mu.Unlock()
}

type transaction struct {
	owner *store
	// Single-key writes do not allocate a change map.
	first    index.Mutation
	hasFirst bool
	rest     map[string]index.Mutation
}

func (tx *transaction) Lookup(key string) (index.Address, bool, error) {
	if tx.owner.closed.Load() {
		return index.Address{}, false, os.ErrClosed
	}
	if tx.hasFirst && tx.first.Key == key {
		return tx.first.Address, !tx.first.Delete, nil
	}
	if c, ok := tx.rest[key]; ok {
		return c.Address, !c.Delete, nil
	}
	return tx.owner.Lookup(key)
}

func (tx *transaction) Put(key string, addr index.Address) error {
	return tx.stage(index.Mutation{Key: key, Address: addr})
}
func (tx *transaction) Delete(key string) error {
	return tx.stage(index.Mutation{Key: key, Delete: true})
}

func (tx *transaction) Apply(m index.Mutation) (index.Address, bool, error) {
	old, found, err := tx.Lookup(m.Key)
	if err != nil {
		return index.Address{}, false, &index.LookupError{Err: err}
	}
	if m.Delete {
		if !found {
			return old, false, nil
		}
		m.Address = index.Address{}
	}
	return old, found, tx.stage(m)
}

func (tx *transaction) stage(c index.Mutation) error {
	if tx.owner.closed.Load() {
		return os.ErrClosed
	}
	if !tx.hasFirst || tx.first.Key == c.Key {
		tx.first = c
		tx.hasFirst = true
		return nil
	}
	if tx.rest == nil {
		tx.rest = make(map[string]index.Mutation)
	}
	tx.rest[c.Key] = c
	return nil
}

func (tx *transaction) Commit() {
	s := tx.owner
	s.mu.Lock()
	if tx.hasFirst {
		s.apply(tx.first)
	}
	for _, c := range tx.rest {
		s.apply(c)
	}
	s.mu.Unlock()
	s.writer.Unlock()
}

func (s *store) apply(c index.Mutation) {
	if c.Delete {
		delete(s.entries, c.Key)
	} else {
		s.entries[c.Key] = c.Address
	}
}

func (tx *transaction) Abort() { tx.owner.writer.Unlock() }

// RetainedEntries allows the historical heap probe to retain the actual map
// across Close. The probe owns the DB and never accesses the map before Close.
func (s *store) RetainedEntries() map[string]index.Address {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries
}
