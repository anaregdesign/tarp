package btree

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/platform"
)

const MinBudget int64 = 256 << 10
const DefaultBudget int64 = 8 << 20
const pageCharge = pageSize + 192

// countedFile measures logical file lengths (including holes), rather than
// inferring host-device writes from a scratch file's apparent size.
type countedFile struct {
	*os.File
	size        int64
	total, peak *atomic.Int64
}

func (f *countedFile) resize(size int64) {
	if size == f.size {
		return
	}
	total := f.total.Add(size - f.size)
	f.size = size
	for peak := f.peak.Load(); total > peak; peak = f.peak.Load() {
		if f.peak.CompareAndSwap(peak, total) {
			break
		}
	}
}
func (f *countedFile) Write(b []byte) (int, error) {
	n, err := f.File.Write(b)
	f.resize(f.size + int64(n))
	return n, err
}
func (f *countedFile) WriteAt(b []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(b, off)
	if n > 0 && off+int64(n) > f.size {
		f.resize(off + int64(n))
	}
	return n, err
}
func (f *countedFile) Truncate(size int64) error {
	if err := f.File.Truncate(size); err != nil {
		return err
	}
	f.resize(size)
	return nil
}
func (f *countedFile) Close() error { err := f.File.Close(); f.resize(0); return err }
func (s *pager) temp() (*countedFile, error) {
	file, err := platform.OpenScratch(s.dir, "tarp-index-*")
	if err != nil {
		return nil, err
	}
	return &countedFile{File: file, total: &s.scratchBytes, peak: &s.scratchPeak}, nil
}

type cachedPage struct {
	id    uint64
	p     *page
	owner uint64
}
type pageFile interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type pager struct {
	budget                    int64
	scratchBytes, scratchPeak atomic.Int64
	mu                        sync.Mutex
	file                      pageFile
	dir                       string
	capacity                  int
	cache                     map[uint64]*list.Element
	branches, ordinary        list.List
	root, maxKey              atomic.Uint64
	top                       uint64
	stats                     index.Metrics

	// The writer alone owns the allocator. Freed page numbers use a bounded
	// stack in memory with a private file for older free entries.
	free      [128]uint64
	freeN     int
	freeFile  *countedFile
	freeBytes int64
}

func newPager(dir string, budget int64) (*pager, error) {
	if budget == 0 {
		budget = DefaultBudget
	}
	if budget < MinBudget {
		return nil, fmt.Errorf("btree: index budget %d below %d", budget, MinBudget)
	}
	// Reserve half for descent/encoding, rollback and scratch buffers.
	s := &pager{budget: budget, dir: dir, capacity: int(budget / 2 / pageCharge), cache: make(map[uint64]*list.Element)}
	f, err := s.temp()
	if err != nil {
		return nil, err
	}
	s.file = f
	return s, nil
}
func (s *pager) queue(p *page) *list.List {
	if p[4] == branch {
		return &s.branches
	}
	return &s.ordinary
}

// Dirty pages are the byte-bounded write buffer. Pinned root generations can
// spill to disk without changing their page IDs or retaining extra heap.
func (s *pager) cachePage(id uint64, p *page, owner uint64) error {
	if item := s.cache[id]; item != nil {
		e := item.Value.(cachedPage)
		s.queue(e.p).Remove(item)
		delete(s.cache, id)
	}
	for len(s.cache) >= s.capacity {
		item := s.ordinary.Back()
		if item == nil {
			item = s.branches.Back()
		}
		entry := item.Value.(cachedPage)
		if entry.id == s.root.Load() {
			other := item.Prev()
			if other == nil {
				other = s.branches.Back()
				if other == item {
					other = s.ordinary.Back()
				}
			}
			if other != nil {
				item = other
				entry = item.Value.(cachedPage)
			}
		}
		if entry.owner != 0 {
			if err := s.flush(entry.id, entry.p); err != nil {
				return err
			}
		}
		delete(s.cache, entry.id)
		s.queue(entry.p).Remove(item)
		s.stats.Evictions++
	}
	s.cache[id] = s.queue(p).PushFront(cachedPage{id: id, p: p, owner: owner})
	s.stats.CachePeakBytes = max(s.stats.CachePeakBytes, uint64(len(s.cache))*pageCharge)
	return nil
}
func (s *pager) flush(id uint64, p *page) error {
	start := time.Now()
	n, err := s.file.WriteAt(p[:], int64(id-1)*pageSize)
	s.stats.SpillNanoseconds += uint64(time.Since(start))
	if err != nil {
		return err
	}
	if n != pageSize {
		return io.ErrShortWrite
	}
	s.stats.SpillWrites++
	return nil
}
func (s *pager) read(id uint64) (*page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readLocked(id, false)
}

func (s *pager) readLocked(id uint64, wantOverflow bool) (*page, error) {
	if s.file == nil {
		return nil, os.ErrClosed
	}
	if id == 0 || id > s.top {
		return nil, errPage
	}
	if item := s.cache[id]; item != nil {
		p := item.Value.(cachedPage).p
		if wantOverflow && p[4] != overflow {
			return nil, errPage
		}
		s.queue(p).MoveToFront(item)
		return p, nil
	}
	p := new(page)
	if _, err := s.file.ReadAt(p[:], int64(id-1)*pageSize); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.Join(errPage, err)
		}
		return nil, err
	}
	if err := p.check(id); err != nil {
		return nil, err
	}
	if wantOverflow && p[4] != overflow {
		return nil, errPage
	}
	if p[4] != overflow {
		// A derived index cannot contain a key longer than any key this
		// process has inserted. Guard both address and length before following
		// an overflow pointer from a page read off disk.
		for i := 0; i < p.count(); i++ {
			c, _ := p.raw(i)
			if c.length > s.maxKey.Load() || c.tail > s.top || c.child > s.top {
				return nil, errPage
			}
		}
	}
	if p.link() > s.top {
		return nil, errPage
	}
	if err := p.validateOrder(func(child uint64) (*page, error) { return s.readLocked(child, true) }); err != nil {
		return nil, err
	}
	s.stats.PageReads++
	switch p[4] {
	case branch:
		s.stats.BranchReads++
	case leaf:
		s.stats.LeafReads++
	case overflow:
		s.stats.OverflowReads++
	}
	if err := s.cachePage(id, p, 0); err != nil {
		return nil, err
	}
	return p, nil
}

// A page created in this transaction can be updated in place until a
// reader-visible root is published. A B+tree page-cache miss falls back to copy-on-write.
func (s *pager) private(id, owner uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.cache[id]
	return e != nil && e.Value.(cachedPage).owner == owner
}
func (s *pager) write(id uint64, p *page, owner uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return os.ErrClosed
	}
	return s.cachePage(id, p, owner)
}
func (s *pager) allocate() (uint64, error) {
	if s.freeN == 0 && s.freeBytes > 0 {
		var b [128 * 8]byte
		n := min(int64(len(b)), s.freeBytes)
		if _, err := s.freeFile.ReadAt(b[:n], s.freeBytes-n); err != nil {
			return 0, err
		}
		s.freeBytes -= n
		s.freeN = int(n) / 8
		for i := range s.freeN {
			s.free[i] = order.Uint64(b[i*8:])
		}
	}
	if s.freeN > 0 {
		s.freeN--
		return s.free[s.freeN], nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.top >= math.MaxInt64/pageSize {
		return 0, errors.New("btree: page ID exhausted")
	}
	s.top++
	return s.top, nil
}
func (s *pager) release(id uint64) error {
	s.mu.Lock()
	if e := s.cache[id]; e != nil {
		s.queue(e.Value.(cachedPage).p).Remove(e)
		delete(s.cache, id)
	}
	s.mu.Unlock()

	if s.freeN == len(s.free) {
		if s.freeFile == nil {
			f, err := s.temp()
			if err != nil {
				return err
			}
			s.freeFile = f
		}
		var b [128 * 8]byte
		for i, v := range s.free {
			order.PutUint64(b[i*8:], v)
		}
		if _, err := s.freeFile.WriteAt(b[:], s.freeBytes); err != nil {
			return err
		}
		s.freeBytes += int64(len(b))
		s.freeN = 0
	}
	s.free[s.freeN] = id
	s.freeN++
	return nil
}
func (s *pager) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.file != nil {
		err = s.file.Close()
		s.file = nil
	}
	if s.freeFile != nil {
		err = errors.Join(err, s.freeFile.Close())
		s.freeFile = nil
	}
	s.cache = nil
	s.branches.Init()
	s.ordinary.Init()
	return err
}
func (s *pager) metrics() index.Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.stats
	m.CacheBytes = uint64(len(s.cache)) * pageCharge
	m.ScratchBytes = uint64(s.scratchBytes.Load())
	m.ScratchPeakBytes = uint64(s.scratchPeak.Load())
	return m
}

// idLog bounds rollback/retirement bookkeeping even for large batches.
type idLog struct {
	buf   [128]uint64
	n     int
	file  *countedFile
	bytes int64
}

func (l *idLog) add(s *pager, id uint64) error {
	if l.n == len(l.buf) {
		if l.file == nil {
			f, err := s.temp()
			if err != nil {
				return err
			}
			l.file = f
		}
		var b [128 * 8]byte
		for i, v := range l.buf {
			order.PutUint64(b[i*8:], v)
		}
		if _, err := l.file.WriteAt(b[:], l.bytes); err != nil {
			return err
		}
		l.bytes += int64(len(b))
		l.n = 0
	}
	l.buf[l.n] = id
	l.n++
	return nil
}
func (l *idLog) each(visit func(uint64) error) error {
	var b [128 * 8]byte
	for pos := int64(0); pos < l.bytes; pos += int64(len(b)) {
		if _, err := l.file.ReadAt(b[:], pos); err != nil {
			return err
		}
		for i := range len(b) / 8 {
			if err := visit(order.Uint64(b[i*8:])); err != nil {
				return err
			}
		}
	}
	for _, v := range l.buf[:l.n] {
		if err := visit(v); err != nil {
			return err
		}
	}
	return nil
}
func (l *idLog) close() error {
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}

// retireQueue stores obsolete IDs on disk while snapshots can still see them.
// Epochs are monotonic; a reader registers before publication releases a root.
type retireQueue struct {
	file       *countedFile
	start, end int64
}

func (q *retireQueue) append(s *pager, rootGeneration uint64, l *idLog) error {
	if l.n == 0 && l.bytes == 0 {
		return nil
	}
	if q.file == nil {
		f, err := s.temp()
		if err != nil {
			return err
		}
		q.file = f
	}
	var buf [2048]byte
	used := 0
	flush := func() error {
		if used == 0 {
			return nil
		}
		if _, err := q.file.WriteAt(buf[:used], q.end); err != nil {
			return err
		}
		q.end += int64(used)
		used = 0
		return nil
	}
	err := l.each(func(id uint64) error {
		order.PutUint64(buf[used:], rootGeneration)
		order.PutUint64(buf[used+8:], id)
		used += 16
		if used == len(buf) {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}
func (q *retireQueue) collect(s *pager, oldestRootGeneration uint64) error {
	if q.start >= q.end {
		return nil
	}
	var buf [2048]byte
	for q.start < q.end {
		n := min(int64(len(buf)), q.end-q.start)
		if _, err := q.file.ReadAt(buf[:n], q.start); err != nil {
			return err
		}
		for i := 0; i < int(n); i += 16 {
			if order.Uint64(buf[i:]) >= oldestRootGeneration {
				return q.compact()
			}
			if err := s.release(order.Uint64(buf[i+8:])); err != nil {
				return err
			}
			q.start += 16
		}
	}
	q.start, q.end = 0, 0
	return q.file.Truncate(0)
}

// Overlapping readers may keep the queue nonempty forever. Move the live
// suffix forward occasionally so consumed records never accumulate on disk.
func (q *retireQueue) compact() error {
	if q.start < 64<<10 || q.start < q.end-q.start {
		return nil
	}
	var buf [8192]byte
	var dst int64
	for src := q.start; src < q.end; {
		n := min(int64(len(buf)), q.end-src)
		if _, err := q.file.ReadAt(buf[:n], src); err != nil {
			return err
		}
		if _, err := q.file.WriteAt(buf[:n], dst); err != nil {
			return err
		}
		dst += n
		src += n
	}
	if err := q.file.Truncate(dst); err != nil {
		return err
	}
	q.start, q.end = 0, dst
	return nil
}
