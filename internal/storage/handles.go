package storage

import (
	"container/list"
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/anaregdesign/tarp/internal/format"
)

type handle struct {
	file    *File
	fd      *os.File
	used    int
	retired atomic.Bool
	node    *list.Element
}

// Once sealed DATA is known to be damaged, a valid record checksum alone
// cannot identify a foreign inode with the same key and sequence. Check the
// header on the descriptor that supplied the bytes. A lease can outlive both
// replacement of the path and clearing the unavailable marker.
func (h *handle) verifyReadIdentity() error {
	r := h.file.sealed.Load()
	if r == nil || r.unavailable.Load() == nil && !h.retired.Load() {
		return nil
	}
	var header [format.SegmentHeaderBytes]byte
	if _, err := h.fd.ReadAt(header[:], 0); err != nil {
		return requiredFileError("segment header", err)
	}
	return verifySegmentHeader(header[:], r.uuid, r.entry)
}

type handles struct {
	mu       sync.Mutex
	cond     *sync.Cond
	limit    int
	items    map[*File]*handle
	lru      list.List
	closeErr error // deferred errors from returning invalidated descriptors
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
		if h.lru.Len() < h.limit {
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
	if got.retired.Load() && got.used == 0 {
		h.lru.Remove(got.node)
		if err := got.fd.Close(); err != nil {
			h.closeErr = errors.Join(h.closeErr, err)
		}
	}
	h.cond.Broadcast()
	h.mu.Unlock()
}

// invalidate detaches a descriptor from the lookup table immediately. Existing
// positional reads retain their lease on that inode. It still counts toward the
// descriptor limit until the last lease closes it.
func (h *handles) invalidate(f *File) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	got := h.items[f]
	if got == nil {
		return nil
	}
	delete(h.items, f)
	got.retired.Store(true)
	if got.used != 0 {
		return nil
	}
	h.lru.Remove(got.node)
	h.cond.Broadcast()
	return got.fd.Close()
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
	err := h.closeErr
	for n := h.lru.Front(); n != nil; n = n.Next() {
		got := n.Value.(*handle)
		if got.used != 0 {
			err = errors.Join(err, errors.New("storage: active lease at close"))
		}
		err = errors.Join(err, got.fd.Close())
	}
	h.items = nil
	h.lru.Init()
	return err
}
