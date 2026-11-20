package btree

import (
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/anaregdesign/tarp/internal/index"
)

// A reader may borrow the current node, two overflow comparison buffers and
// a newly decoded page. Allow two more pages for bounded routing/validation.
// The writer has its own workspace and never takes a reader permit.
const readCharge = 6 * pageSize
const lookupGroupSize = 128

type readWorkspace struct {
	slots                     chan struct{}
	used, peak, waits, waitNS atomic.Uint64
}

func newReadWorkspace(budget int64) readWorkspace {
	return readWorkspace{slots: make(chan struct{}, max(1, int(budget/4/readCharge)))}
}

func (v *view) beginRead() error {
	t := v.tree
	w := &t.reads
	select {
	case w.slots <- struct{}{}:
	default:
		start := time.Now()
		w.waits.Add(1)
		select {
		case w.slots <- struct{}{}:
		case <-t.stopped:
			w.waitNS.Add(uint64(time.Since(start)))
			return os.ErrClosed
		}
		w.waitNS.Add(uint64(time.Since(start)))
	}
	current := w.used.Add(readCharge)
	for peak := w.peak.Load(); current > peak; peak = w.peak.Load() {
		if w.peak.CompareAndSwap(peak, current) {
			break
		}
	}
	if !v.valid() {
		w.release()
		return os.ErrClosed
	}
	return nil
}

func (w *readWorkspace) release() {
	w.used.Add(^uint64(readCharge - 1))
	<-w.slots
}

func (w *readWorkspace) metrics(m *index.Metrics) {
	m.ReadWorkspaceBytes = w.used.Load()
	m.ReadWorkspacePeakBytes = w.peak.Load()
	m.ReadWorkspaceLimitBytes = uint64(cap(w.slots)) * readCharge
	m.ReadWaits = w.waits.Load()
	m.ReadWaitNanoseconds = w.waitNS.Load()
}

type frame struct {
	id  uint64
	pos int
}
type cursor struct{ stack []frame }

// Advance only while admitted. No page buffer or reader workspace is held
// over user code, including a nested read, mutation, or Close.
func (c *cursor) next(s *pager) (string, index.Address, bool, error) {
	for len(c.stack) > 0 {
		f := &c.stack[len(c.stack)-1]
		p, err := s.read(f.id)
		if err != nil {
			return "", index.Address{}, false, err
		}
		switch p[4] {
		case leaf:
			if f.pos < p.count() {
				item, _ := p.cell(f.pos)
				f.pos++
				key, err := item.key.string(s)
				return key, item.address, true, err
			}
		case branch:
			if f.pos <= p.count() {
				id := p.child(f.pos)
				f.pos++
				if len(c.stack) >= maxDepth {
					return "", index.Address{}, false, errPage
				}
				c.stack = append(c.stack, frame{id: id})
				continue
			}
		default:
			return "", index.Address{}, false, errPage
		}
		c.stack = c.stack[:len(c.stack)-1]
	}
	return "", index.Address{}, false, nil
}

func (v *view) All(yield func(string, index.Address) bool) error {
	if v.root == 0 {
		if !v.valid() {
			return os.ErrClosed
		}
		return nil
	}
	c := cursor{stack: []frame{{id: v.root}}}
	for {
		if err := v.beginRead(); err != nil {
			return err
		}
		key, a, ok, err := c.next(v.tree.p)
		v.tree.reads.release()
		if err != nil || !ok {
			return err
		}
		if !yield(key, a) {
			return nil
		}
	}
}

type lookupResult struct {
	addr  index.Address
	found bool
}
type lookupRange struct {
	id            uint64
	lo, hi, depth int
}

// Every pending range owns at least one distinct input position; the total
// number of ranges never exceeds the fixed group size. Carrying page IDs
// across descents permits eviction of the entire path including ancestors.
func (v *view) lookupGroup(keys []string, results []lookupResult) error {
	if v.root == 0 {
		return nil
	}
	var order [lookupGroupSize]int
	positions := order[:len(keys)]
	for i := range positions {
		positions[i] = i
	}
	sort.Slice(positions, func(i, j int) bool { return keys[positions[i]] < keys[positions[j]] })
	var pending [lookupGroupSize]lookupRange
	jobs := pending[:1]
	jobs[0] = lookupRange{id: v.root, hi: len(keys)}
	for len(jobs) > 0 {
		job := jobs[len(jobs)-1]
		jobs = jobs[:len(jobs)-1]
		if job.depth >= maxDepth {
			return errPage
		}
		p, err := v.tree.p.read(job.id)
		if err != nil {
			return err
		}
		start, prev := job.lo, -1
		for j := job.lo; j < job.hi; j++ {
			i := positions[j]
			at, found, err := p.search(v.tree.p, keys[i])
			if err != nil {
				return err
			}
			if p[4] == leaf {
				if found {
					item, _ := p.raw(at)
					results[i] = lookupResult{item.address, true}
				}
				continue
			}
			if prev >= 0 && prev != at {
				jobs = append(jobs, lookupRange{id: p.child(prev), lo: start, hi: j, depth: job.depth + 1})
				start = j
			}
			prev = at
		}
		if prev >= 0 {
			jobs = append(jobs, lookupRange{id: p.child(prev), lo: start, hi: job.hi, depth: job.depth + 1})
		}
	}
	return nil
}

func (v *view) LookupMany(keys []string, set func(int, index.Address, bool)) error {
	if len(keys) == 0 && !v.valid() {
		return os.ErrClosed
	}
	for offset := 0; offset < len(keys); offset += lookupGroupSize {
		if err := v.beginRead(); err != nil {
			return err
		}
		group := keys[offset:min(len(keys), offset+lookupGroupSize)]
		results := make([]lookupResult, len(group))
		err := v.lookupGroup(group, results)
		v.tree.reads.release()
		if err != nil {
			return err
		}
		for i, r := range results {
			set(offset+i, r.addr, r.found)
		}
	}
	return nil
}
