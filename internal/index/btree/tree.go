package btree

import (
	"errors"
	"math"
	"os"
	"sort"
	"sync"

	"github.com/anaregdesign/tarp/internal/index"
)

type tree struct {
	p                *pager
	writer           sync.Mutex
	mu               sync.Mutex
	root, generation uint64
	count            int
	views            map[uint64]int
	closed           bool
	pending          *transaction
	retired          retireQueue
	allocErr         error
}

// New builds an empty process-private index in the OS scratch directory.
func New(budget int64) (index.Backend, error) { return NewIn("", budget) }

// NewIn puts all disk workspace for the derived index on the chosen filesystem.
func NewIn(dir string, budget int64) (index.Backend, error) {
	p, err := newPager(dir, budget)
	if err != nil {
		return nil, err
	}
	return &tree{p: p, generation: 1, views: make(map[uint64]int)}, nil
}
func (t *tree) Metrics() index.Metrics { return t.p.metrics() }
func (t *tree) Count() int             { t.mu.Lock(); defer t.mu.Unlock(); return t.count }
func (t *tree) View() index.View {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := &view{tree: t, root: t.root, gen: t.generation}
	if t.closed {
		v.gen = 0
	} else {
		t.views[v.gen]++
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
	return err
}
func (t *tree) Close() error {
	t.writer.Lock()
	defer t.writer.Unlock()
	t.mu.Lock()
	t.closed = true
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
	if !v.valid() {
		return index.Address{}, false, os.ErrClosed
	}
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
func (v *view) All(yield func(string, index.Address) bool) error {
	if !v.valid() {
		return os.ErrClosed
	}
	type frame struct {
		id  uint64
		pos int
	}
	if v.root == 0 {
		return nil
	}
	stack := []frame{{id: v.root}}
	for len(stack) > 0 {
		if !v.valid() {
			return os.ErrClosed
		}
		f := &stack[len(stack)-1]
		p, err := v.tree.p.read(f.id)
		if err != nil {
			return err
		}
		switch p[4] {
		case leaf:
			if f.pos < p.count() {
				c, _ := p.cell(f.pos)
				f.pos++
				key, err := c.key.string(v.tree.p)
				if err != nil {
					return err
				}
				// The cursor retains only page numbers and slot offsets over
				// user code. Its leaf may be evicted while iteration is paused.
				if !yield(key, c.address) {
					return nil
				}
				continue
			}
		case branch:
			if f.pos <= p.count() {
				child := p.child(f.pos)
				f.pos++
				if len(stack) >= maxDepth {
					return errPage
				}
				stack = append(stack, frame{id: child})
				continue
			}
		default:
			return errPage
		}
		stack = stack[:len(stack)-1]
	}
	return nil
}

// LookupMany visits the request in byte order so all keys routed to the
// same child consume one page load. Results are identified by input position.
func (v *view) LookupMany(keys []string, set func(int, index.Address, bool)) error {
	if !v.valid() {
		return os.ErrClosed
	}
	positions := make([]int, len(keys))
	for i := range positions {
		positions[i] = i
	}
	sort.Slice(positions, func(i, j int) bool { return keys[positions[i]] < keys[positions[j]] })
	var group func(uint64, []int, int) error
	group = func(id uint64, ids []int, depth int) error {
		if len(ids) == 0 {
			return nil
		}
		if depth >= maxDepth {
			return errPage
		}
		if id == 0 {
			for _, i := range ids {
				set(i, index.Address{}, false)
			}
			return nil
		}
		p, err := v.tree.p.read(id)
		if err != nil {
			return err
		}
		if p[4] == leaf {
			for _, i := range ids {
				at, found, err := p.search(v.tree.p, keys[i])
				if err != nil {
					return err
				}
				var addr index.Address
				if found {
					c, _ := p.raw(at)
					addr = c.address
				}
				set(i, addr, found)
			}
			return nil
		}
		start, prev := 0, -1
		for j, i := range ids {
			slot, _, err := p.search(v.tree.p, keys[i])
			if err != nil {
				return err
			}
			if prev >= 0 && prev != slot {
				if err := group(p.child(prev), ids[start:j], depth+1); err != nil {
					return err
				}
				start = j
			}
			prev = slot
		}
		return group(p.child(prev), ids[start:], depth+1)
	}
	return group(v.root, positions, 0)
}

// LookupMany shares one immutable root across the requested keys.
func (t *tree) LookupMany(keys []string, set func(int, index.Address, bool)) error {
	v := t.View().(*view)
	defer v.Close()
	return v.LookupMany(keys, set)
}
