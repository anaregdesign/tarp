package memory

import (
	"errors"
	"os"

	"github.com/anaregdesign/tarp/internal/index"
)

// initialBuild owns a single private map, which becomes the runtime map.
// writer excludes mutations and Close. Concurrent readers can still observe
// the original empty map while the storage owner replays and measures DATA.
type initialBuild struct {
	store   *store
	entries map[string]index.Address
}

func (s *store) BeginBuild() (index.InitialBuild, error) {
	s.writer.Lock()
	// Every mutation, including Close, owns writer.
	if s.closed.Load() {
		s.writer.Unlock()
		return nil, os.ErrClosed
	}
	if len(s.entries) != 0 {
		s.writer.Unlock()
		return nil, errors.New("memory: initial build requires an empty index")
	}
	return &initialBuild{store: s, entries: make(map[string]index.Address)}, nil
}

func (b *initialBuild) Lookup(key string) (index.Address, bool, error) {
	a, ok := b.entries[key]
	return a, ok, nil
}

func (b *initialBuild) Put(key string, a index.Address) error {
	b.entries[key] = a
	return nil
}

func (b *initialBuild) Delete(key string) error {
	delete(b.entries, key)
	return nil
}

func (b *initialBuild) Finish(measure func(string, index.Address) error) error {
	defer b.Abort()
	if measure != nil {
		for key, addr := range b.entries {
			if err := measure(key, addr); err != nil {
				return err
			}
		}
	}
	s := b.store
	s.mu.Lock()
	s.entries = b.entries
	s.mu.Unlock()
	return nil
}

func (b *initialBuild) Abort() {
	b.entries = nil
	b.store.writer.Unlock()
}
