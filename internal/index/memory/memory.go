// Package memory implements the in-memory index for datasets that fit in RAM.
package memory

import (
	"context"
	"maps"
	"os"
	"slices"
	"strings"
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

type capturedEntry struct {
	key     string
	address index.Address
}

// BoundedView copies matching addresses into small slices. The existing
// slices stay in place as the snapshot grows.
func (s *store) BoundedView(ctx context.Context, bounds index.Bounds) (index.Snapshot, error) {
	done := ctx.Done()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var chunks [][]capturedEntry
	var chunk []capturedEntry
	for key, addr := range s.entries {
		select {
		case <-done:
			return nil, ctx.Err()
		default:
		}
		if !bounds.Contains(key) {
			continue
		}
		if len(chunk) == cap(chunk) {
			if len(chunk) > 0 {
				chunks = append(chunks, chunk)
			}
			// Begin with one entry for sparse ranges. Limit slack in the
			// last allocation to fewer than 128 entries (7 KiB).
			chunk = make([]capturedEntry, 0, min(128, max(1, 2*len(chunk))))
		}
		chunk = append(chunk, capturedEntry{key: key, address: addr})
	}
	if len(chunk) > 0 {
		chunks = append(chunks, chunk)
	}
	v := &boundedView{owner: s}
	v.chunks.Store(&chunks)
	return v, nil
}

type boundedView struct {
	owner  *store
	chunks atomic.Pointer[[][]capturedEntry]
}

func (v *boundedView) All(yield func(string, index.Address) bool) error {
	chunks := v.chunks.Load()
	if chunks == nil || v.owner.closed.Load() {
		return os.ErrClosed
	}
	for _, chunk := range *chunks {
		for _, row := range chunk {
			if !yield(row.key, row.address) {
				return nil
			}
			if v.owner.closed.Load() || v.chunks.Load() == nil {
				return os.ErrClosed
			}
		}
	}
	return nil
}

func (v *boundedView) flatten(lower, upper string) ([]capturedEntry, error) {
	chunks := v.chunks.Load()
	if chunks == nil || v.owner.closed.Load() {
		return nil, os.ErrClosed
	}
	if upper != "" && lower >= upper {
		return nil, nil
	}
	total := 0
	for _, chunk := range *chunks {
		total += len(chunk)
	}
	b := index.Bounds{Lower: lower, Upper: upper}
	rows := make([]capturedEntry, 0, total)
	for _, chunk := range *chunks {
		for _, row := range chunk {
			if b.Contains(row.key) {
				rows = append(rows, row)
			}
		}
	}
	return rows, nil
}

func (v *boundedView) Range(lower, upper string, yield func(string, index.Address) bool) error {
	rows, err := v.flatten(lower, upper)
	if err != nil || len(rows) == 0 {
		return err
	}
	slices.SortFunc(rows, func(a, b capturedEntry) int {
		return strings.Compare(a.key, b.key)
	})
	for _, row := range rows {
		if v.owner.closed.Load() || v.chunks.Load() == nil {
			return os.ErrClosed
		}
		if !yield(row.key, row.address) {
			return nil
		}
	}
	if v.owner.closed.Load() || v.chunks.Load() == nil {
		return os.ErrClosed
	}
	return nil
}

func (v *boundedView) RangeDesc(lower, upper string, yield func(string, index.Address) bool) error {
	rows, err := v.flatten(lower, upper)
	if err != nil || len(rows) == 0 {
		return err
	}
	slices.SortFunc(rows, func(a, b capturedEntry) int {
		return strings.Compare(b.key, a.key)
	})
	for _, row := range rows {
		if v.owner.closed.Load() || v.chunks.Load() == nil {
			return os.ErrClosed
		}
		if !yield(row.key, row.address) {
			return nil
		}
	}
	if v.owner.closed.Load() || v.chunks.Load() == nil {
		return os.ErrClosed
	}
	return nil
}

func (v *boundedView) Close() {
	v.chunks.Store(nil)
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
	sorted  []capturedEntry
	hasSort bool
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

func (v *view) LookupMany(keys []string, set func(int, index.Address, bool)) error {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.owner.closed.Load() || v.entries == nil {
		return os.ErrClosed
	}
	for i, key := range keys {
		addr, ok := v.entries[key]
		set(i, addr, ok)
	}
	return nil
}

func (v *view) isClosed() bool {
	if v.owner.closed.Load() {
		return true
	}
	v.mu.RLock()
	closed := v.entries == nil
	v.mu.RUnlock()
	return closed
}

func (v *view) sortedEntries() ([]capturedEntry, error) {
	v.mu.RLock()
	if v.owner.closed.Load() || v.entries == nil {
		v.mu.RUnlock()
		return nil, os.ErrClosed
	}
	if v.hasSort {
		sorted := v.sorted
		v.mu.RUnlock()
		return sorted, nil
	}
	v.mu.RUnlock()

	v.mu.Lock()
	defer v.mu.Unlock()
	if v.owner.closed.Load() || v.entries == nil {
		return nil, os.ErrClosed
	}
	if !v.hasSort {
		sorted := make([]capturedEntry, 0, len(v.entries))
		for k, a := range v.entries {
			sorted = append(sorted, capturedEntry{key: k, address: a})
		}
		slices.SortFunc(sorted, func(a, b capturedEntry) int {
			return strings.Compare(a.key, b.key)
		})
		v.sorted = sorted
		v.hasSort = true
	}
	return v.sorted, nil
}

func sortedSliceBounds(sorted []capturedEntry, lower, upper string) (start, end int) {
	if lower != "" {
		start, _ = slices.BinarySearchFunc(sorted, lower, func(e capturedEntry, target string) int {
			return strings.Compare(e.key, target)
		})
	}
	end = len(sorted)
	if upper != "" {
		rel, _ := slices.BinarySearchFunc(sorted[start:], upper, func(e capturedEntry, target string) int {
			return strings.Compare(e.key, target)
		})
		end = start + rel
	}
	return start, end
}

func (v *view) Range(lower, upper string, yield func(string, index.Address) bool) error {
	if upper != "" && lower >= upper {
		if v.isClosed() {
			return os.ErrClosed
		}
		return nil
	}
	sorted, err := v.sortedEntries()
	if err != nil {
		return err
	}
	start, end := sortedSliceBounds(sorted, lower, upper)
	for i := start; i < end; i++ {
		if v.isClosed() {
			return os.ErrClosed
		}
		if !yield(sorted[i].key, sorted[i].address) {
			return nil
		}
	}
	if start < end && v.isClosed() {
		return os.ErrClosed
	}
	return nil
}

func (v *view) RangeDesc(lower, upper string, yield func(string, index.Address) bool) error {
	if upper != "" && lower >= upper {
		if v.isClosed() {
			return os.ErrClosed
		}
		return nil
	}
	sorted, err := v.sortedEntries()
	if err != nil {
		return err
	}
	start, end := sortedSliceBounds(sorted, lower, upper)
	for i := end - 1; i >= start; i-- {
		if v.isClosed() {
			return os.ErrClosed
		}
		if !yield(sorted[i].key, sorted[i].address) {
			return nil
		}
	}
	if start < end && v.isClosed() {
		return os.ErrClosed
	}
	return nil
}

func (v *view) All(yield func(string, index.Address) bool) error {
	v.mu.RLock()
	entries := v.entries
	v.mu.RUnlock()
	if entries == nil || v.owner.closed.Load() {
		return os.ErrClosed
	}
	for key, addr := range entries {
		if !yield(key, addr) {
			return nil
		}
		if v.isClosed() {
			return os.ErrClosed
		}
	}
	return nil
}

func (v *view) Close() {
	v.mu.Lock()
	v.entries = nil
	v.sorted = nil
	v.hasSort = false
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
