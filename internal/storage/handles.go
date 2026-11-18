package storage

import (
	"container/list"
	"errors"
	"os"
	"sync"
)

type handle struct {
	file *File
	fd   *os.File
	used int
	node *list.Element
}

type handles struct {
	mu    sync.Mutex
	cond  *sync.Cond
	limit int
	items map[*File]*handle
	lru   list.List
}

func newHandles(limit int) *handles {
	h := &handles{limit: limit, items: make(map[*File]*handle)}
	h.cond = sync.NewCond(&h.mu)
	return h
}

func (h *handles) acquire(f *File) (*handle, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		if got := h.items[f]; got != nil {
			got.used++
			h.lru.MoveToFront(got.node)
			return got, nil
		}
		if len(h.items) < h.limit {
			fd, err := OpenRegular(f.Path)
			if err != nil {
				return nil, err
			}
			got := &handle{file: f, fd: fd, used: 1}
			got.node = h.lru.PushFront(got)
			h.items[f] = got
			return got, nil
		}
		var idle *handle
		for n := h.lru.Back(); n != nil; n = n.Prev() {
			if candidate := n.Value.(*handle); candidate.used == 0 {
				idle = candidate
				break
			}
		}
		if idle != nil {
			delete(h.items, idle.file)
			h.lru.Remove(idle.node)
			if err := idle.fd.Close(); err != nil {
				return nil, err
			}
			continue
		}
		h.cond.Wait()
	}
}

func (h *handles) release(got *handle) {
	h.mu.Lock()
	got.used--
	h.cond.Broadcast()
	h.mu.Unlock()
}

func (h *handles) forget(f *File) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	got := h.items[f]
	if got == nil {
		return nil
	}
	if got.used != 0 {
		return errors.New("storage: attempted to close a leased file")
	}
	delete(h.items, f)
	h.lru.Remove(got.node)
	return got.fd.Close()
}

func (h *handles) close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var err error
	for _, got := range h.items {
		if got.used != 0 {
			err = errors.Join(err, errors.New("storage: active lease at close"))
		}
		err = errors.Join(err, got.fd.Close())
	}
	h.items = nil
	h.lru.Init()
	return err
}
