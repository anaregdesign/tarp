package btree

import (
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/anaregdesign/tarp/internal/index"
)

type tree struct {
	p                *pager
	writer           sync.Mutex
	mu               sync.Mutex
	root, generation uint64
	count            int
	views            map[uint64]int
	activeViews      uint64
	retiredPages     atomic.Uint64
	closed           bool
	stopped          chan struct{}
	reads            readWorkspace
	pending          *transaction
	packed           packedHeader // dense bulk-built image, protected by writer
	retired          retireQueue
	allocErr         error
}

// NewIn puts all disk workspace for the derived index on the chosen filesystem.
func NewIn(dir string, budget int64) (index.Backend, error) {
	p, err := newPager(dir, budget)
	if err != nil {
		return nil, err
	}
	return &tree{p: p, generation: 1, views: make(map[uint64]int), stopped: make(chan struct{}), reads: newReadWorkspace(p.budget)}, nil
}
func (t *tree) Metrics() index.Metrics {
	m := t.p.metrics()
	t.mu.Lock()
	m.ActiveViews = t.activeViews
	m.PinnedGenerations = uint64(len(t.views))
	t.mu.Unlock()
	m.RetiredPages = t.retiredPages.Load()
	t.reads.metrics(&m)
	return m
}
func (t *tree) Count() int { t.mu.Lock(); defer t.mu.Unlock(); return t.count }
func (t *tree) View() index.View {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := &view{tree: t, root: t.root, gen: t.generation}
	if t.closed {
		v.gen = 0
	} else {
		t.views[v.gen]++
		t.activeViews++
	}
	return v
}
func (t *tree) Lookup(key string) (index.Address, bool, error) {
	v := t.View()
	defer v.Close()
	return v.Lookup(key)
}
func (t *tree) Begin() index.Tx {
	t.writer.Lock()
	t.mu.Lock()
	root, count, gen := t.root, t.count, t.generation
	minGen := gen
	for g := range t.views {
		minGen = min(minGen, g)
	}
	closed := t.closed
	t.mu.Unlock()
	var err error
	if closed {
		err = os.ErrClosed
	} else if gen == math.MaxUint64 {
		err = errors.New("btree: snapshot generation exhausted")
	} else if t.allocErr != nil {
		err = t.allocErr
	} else {
		err = t.reclaim(minGen)
		if err != nil {
			t.allocErr = err
		}
	}
	return &transaction{tree: t, root: root, count: count, gen: gen, err: err}
}
func (t *tree) reclaim(minGen uint64) error {
	err := t.retired.collect(t.p, minGen)
	if prev := t.pending; prev != nil {
		if err == nil {
			if prev.gen < minGen {
				err = prev.old.each(t.p.release)
			} else {
				err = t.retired.append(t.p, prev.gen, &prev.old)
			}
		}
		err = errors.Join(err, prev.old.close(), prev.allocated.close())
		t.pending = nil
	}
	t.retiredPages.Store(uint64((t.retired.end - t.retired.start) / 16))
	return err
}
func (t *tree) Close() error {
	t.writer.Lock()
	defer t.writer.Unlock()
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		close(t.stopped)
	}
	t.mu.Unlock()
	var err error
	if prev := t.pending; prev != nil {
		err = errors.Join(prev.allocated.close(), prev.old.close())
		t.pending = nil
	}
	if f := t.retired.file; f != nil {
		err = errors.Join(err, f.Close())
		t.retired.file = nil
	}
	t.retiredPages.Store(0)
	return errors.Join(err, t.p.close())
}

type view struct {
	tree      *tree
	root, gen uint64
}

func (v *view) Close() {
	t := v.tree
	t.mu.Lock()
	defer t.mu.Unlock()
	if v.gen != 0 {
		g := v.gen
		v.gen = 0
		t.activeViews--
		if n := t.views[g]; n <= 1 {
			delete(t.views, g)
		} else {
			t.views[g] = n - 1
		}
	}
}
func (v *view) valid() bool {
	v.tree.mu.Lock()
	defer v.tree.mu.Unlock()
	return v.gen != 0 && !v.tree.closed
}
func (v *view) Lookup(key string) (index.Address, bool, error) {
	if err := v.beginRead(); err != nil {
		return index.Address{}, false, err
	}
	defer v.tree.reads.release()
	return lookup(v.tree.p, v.root, key)
}
func lookup(s *pager, id uint64, key string) (index.Address, bool, error) {
	for depth := 0; id != 0 && depth < maxDepth; depth++ {
		p, err := s.read(id)
		if err != nil {
			return index.Address{}, false, err
		}
		i, found, err := p.search(s, key)
		if err != nil {
			return index.Address{}, false, err
		}
		if p[4] == leaf {
			if !found {
				return index.Address{}, false, nil
			}
			c, err := p.raw(i)
			return c.address, err == nil, err
		}
		id = p.child(i)
	}
	if id != 0 {
		return index.Address{}, false, errPage
	}
	return index.Address{}, false, nil
}

// LookupMany shares one immutable root across the requested keys.
func (t *tree) LookupMany(keys []string, set func(int, index.Address, bool)) error {
	v := t.View().(*view)
	defer v.Close()
	return v.LookupMany(keys, set)
}
