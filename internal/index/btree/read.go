package btree

import (
	"os"
	"sort"
	"strings"
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

// leaf returns the next nonempty leaf with the cursor positioned at its next
// entry. On the initial descent, lower seeks through branch separators to its
// insertion point. The saved ancestor positions resume at the next sibling.
func (c *cursor) leaf(s *pager, lower string) (*page, error) {
	for len(c.stack) > 0 {
		f := &c.stack[len(c.stack)-1]
		p, err := s.read(f.id)
		if err != nil {
			return nil, err
		}
		if lower != "" {
			f.pos, _, err = p.search(s, lower)
			if err != nil {
				return nil, err
			}
		}
		switch p[4] {
		case leaf:
			if f.pos < p.count() {
				return p, nil
			}
		case branch:
			if f.pos <= p.count() {
				if len(c.stack) >= maxDepth {
					return nil, errPage
				}
				id := p.child(f.pos)
				f.pos++
				c.stack = append(c.stack, frame{id: id})
				continue
			}
		default:
			return nil, errPage
		}
		lower = ""
		c.stack = c.stack[:len(c.stack)-1]
	}
	return nil, nil
}

func (v *view) All(yield func(string, index.Address) bool) error {
	return v.Range("", "", yield)
}

const rangeGroupSize = 16

type rangeEntry struct {
	key  string
	addr index.Address
}

// readGroup copies entries from one leaf while the caller holds a read permit.
// Inline keys are at most maxInline bytes. Read an overflow key on its own to
// bound retained bytes and avoid reading its chain before preceding callbacks.
func (c *cursor) readGroup(s *pager, lower, upper string, out []rangeEntry) (int, error) {
	p, err := c.leaf(s, lower)
	if err != nil || p == nil {
		return 0, err
	}
	f := &c.stack[len(c.stack)-1]
	n, reachedUpper, err := p.rangeEntries(s, f.pos, upper, out)
	f.pos += n
	if reachedUpper {
		c.stack = nil
	} else if f.pos == p.count() {
		c.stack = c.stack[:len(c.stack)-1]
	}
	return n, err
}

// rangeEntries stores an inline group's reconstructed keys in a single owned
// string. Grow exactly once for matching keys, so retaining a result never
// pins a page or unused capacity for long neighboring keys.
func (p *page) rangeEntries(s *pager, start int, upper string, out []rangeEntry) (n int, reachedUpper bool, err error) {
	end := min(start+len(out), p.count())
	if upper != "" && end-start > 1 {
		// A short final candidate below upper proves the whole group fits.
		// Leave overflow keys and any errors to the sequential walk.
		last, e := p.raw(end - 1)
		if e == nil && last.tail == 0 {
			if cmp, _ := last.compare(s, upper); cmp < 0 {
				upper = ""
			}
		}
	}
	var suffixes [rangeGroupSize][]byte
	prefix := p[header : header+int(p[5])]
	total := 0
	for start+n < end {
		var raw rawCell
		raw, err = p.raw(start + n)
		if err != nil || raw.tail != 0 && n != 0 {
			break
		}
		if upper != "" {
			var cmp int
			cmp, err = raw.compare(s, upper)
			if err != nil {
				break
			}
			if cmp >= 0 {
				reachedUpper = true
				break
			}
		}
		if len(out) == 1 || raw.tail != 0 {
			key, err := raw.key().string(s)
			if err != nil {
				return 0, false, err
			}
			out[0] = rangeEntry{key, raw.address}
			return 1, false, nil
		}
		suffixes[n] = raw.suffix
		total += len(prefix) + len(raw.suffix)
		out[n].addr = raw.address
		n++
	}
	var keys strings.Builder
	keys.Grow(total)
	for i, suffix := range suffixes[:n] {
		offset := keys.Len()
		keys.Write(prefix)
		keys.Write(suffix)
		out[i].key = keys.String()[offset:]
	}
	return n, reachedUpper, err
}

// Range visits keys in [lower, upper) at this view's root. An empty upper
// bound extends through the last key.
func (v *view) Range(lower, upper string, yield func(string, index.Address) bool) error {
	if v.root == 0 || upper != "" && lower >= upper {
		if !v.valid() {
			return os.ErrClosed
		}
		return nil
	}
	c := cursor{stack: []frame{{id: v.root}}}
	var group [rangeGroupSize]rangeEntry
	// Fetch just the first entry so an early break never decodes a whole group.
	limit := 1
	bounded := lower != "" || upper != ""
	for {
		if err := v.beginRead(); err != nil {
			return err
		}
		n, err := c.readGroup(v.tree.p, lower, upper, group[:limit])
		v.tree.reads.release()
		for i, entry := range group[:n] {
			// beginRead checked the first entry. Each callback may close the
			// view or tree, including when further entries are already buffered.
			if i > 0 && !v.valid() {
				return os.ErrClosed
			}
			if !yield(entry.key, entry.addr) {
				return nil
			}
		}
		if err != nil || len(c.stack) == 0 {
			if n != 0 && !v.valid() {
				return os.ErrClosed
			}
			return err
		}
		lower = ""
		if bounded {
			limit = len(group)
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
