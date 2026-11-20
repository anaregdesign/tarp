// Package memory implements the in-memory index for datasets that fit in RAM.
package memory

import (
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

type change struct {
	key     string
	value   index.Address
	deleted bool
}

type transaction struct {
	owner *store
	// Single-key writes do not allocate a change map.
	first    change
	hasFirst bool
	rest     map[string]change
}

func (tx *transaction) Lookup(key string) (index.Address, bool, error) {
	if tx.owner.closed.Load() {
		return index.Address{}, false, os.ErrClosed
	}
	if tx.hasFirst && tx.first.key == key {
		return tx.first.value, !tx.first.deleted, nil
	}
	if c, ok := tx.rest[key]; ok {
		return c.value, !c.deleted, nil
	}
	return tx.owner.Lookup(key)
}

func (tx *transaction) Put(key string, addr index.Address) error {
	return tx.stage(change{key: key, value: addr})
}
func (tx *transaction) Delete(key string) error { return tx.stage(change{key: key, deleted: true}) }

func (tx *transaction) stage(c change) error {
	if tx.owner.closed.Load() {
		return os.ErrClosed
	}
	if !tx.hasFirst || tx.first.key == c.key {
		tx.first = c
		tx.hasFirst = true
		return nil
	}
	if tx.rest == nil {
		tx.rest = make(map[string]change)
	}
	tx.rest[c.key] = c
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

func (s *store) apply(c change) {
	if c.deleted {
		delete(s.entries, c.key)
	} else {
		s.entries[c.key] = c.value
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
