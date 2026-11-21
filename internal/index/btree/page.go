package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"strings"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

const (
	pageSize    = 8192
	header      = 32
	leaf        = 1
	branch      = 2
	overflow    = 3
	maxInline   = 240
	longPrefix  = 24
	maxDepth    = 64
	addressSize = 40
)

var errPage = fmt.Errorf("btree: invalid index page: %w", format.ErrCorrupt)
var order = binary.LittleEndian
var crcTable = crc32.MakeTable(crc32.Castagnoli)

type page [pageSize]byte

// Layout: checksum, type, shared-prefix length, slot count, slots end,
// lowest cell offset, reserved bytes, page ID, leftmost child/overflow next.
// Prefix bytes and the sorted uint16 slot directory grow up from the header.
// Variable-length cells grow down from the end.
func (p *page) initialize(kind byte, id uint64) {
	p[4] = kind
	order.PutUint64(p[16:24], id)
}
func (p *page) seal()        { order.PutUint32(p[:4], crc32.Checksum(p[4:], crcTable)) }
func (p *page) link() uint64 { return order.Uint64(p[24:32]) }
func (p *page) count() int   { return int(order.Uint16(p[6:8])) }

// packedCellBytes excludes the page header and the shared key prefix.
func (p *page) packedCellBytes() int {
	return 2*p.count() + pageSize - int(order.Uint16(p[10:12]))
}
func (p *page) offset(i int) int {
	o := header + int(p[5]) + 2*i
	return int(order.Uint16(p[o : o+2]))
}
func (p *page) cellEnd(i int) int {
	if i == 0 {
		return pageSize
	}
	return p.offset(i - 1)
}
func (p *page) check(id uint64) error {
	if order.Uint64(p[16:24]) != id || order.Uint32(p[:4]) != crc32.Checksum(p[4:], crcTable) {
		return errPage
	}
	if p[4] == overflow {
		n := int(order.Uint16(p[6:8]))
		if n == 0 || n > pageSize-header || p.link() == id {
			return errPage
		}
		return nil
	}
	if p[4] != leaf && p[4] != branch {
		return errPage
	}
	n := p.count()
	lower := header + int(p[5]) + 2*n
	if lower > pageSize || int(order.Uint16(p[8:10])) != lower || p[4] == branch && p.link() == 0 {
		return errPage
	}
	previous := pageSize
	for i := 0; i < n; i++ {
		start := p.offset(i)
		if start < lower || start >= previous {
			return errPage
		}
		if _, err := p.raw(i); err != nil {
			return err
		}
		previous = start
	}
	if previous != int(order.Uint16(p[10:12])) {
		return errPage
	}
	return nil
}

// For a large key, inline contains its first longPrefix bytes. The remaining
// bytes are stored in owned overflow pages and compared incrementally.
type keyRef struct {
	inline string
	length uint64
	tail   uint64
}
type cell struct {
	key     keyRef
	address index.Address
	child   uint64
}
type node struct {
	kind  byte
	left  uint64
	cells []cell
}

// rawCell borrows immutable bytes from its page. Checking and searching short
// keys therefore need no owned strings, including when a page is first read.
type rawCell struct {
	prefix, suffix      []byte
	length, tail, child uint64
	address             index.Address
}

func (c rawCell) key() keyRef {
	return keyRef{inline: string(c.prefix) + string(c.suffix), length: c.length, tail: c.tail}
}
func (c rawCell) compare(s *pager, key string) (int, error) {
	if c.tail != 0 {
		return c.key().compare(s, key)
	}
	n := min(len(c.prefix), len(key))
	if cmp := compareBytesString(c.prefix[:n], key[:n]); cmp != 0 {
		return cmp, nil
	}
	if n < len(c.prefix) {
		return 1, nil
	}
	return compareBytesString(c.suffix, key[n:]), nil
}
func (p *page) cell(i int) (cell, error) {
	c, err := p.raw(i)
	if err != nil {
		return cell{}, err
	}
	return cell{key: c.key(), address: c.address, child: c.child}, nil
}

func (p *page) raw(i int) (rawCell, error) {
	var c rawCell
	end := p.cellEnd(i)
	start := p.offset(i)
	if start < header || end > pageSize || end <= start {
		return c, errPage
	}
	b := p[start:end]
	addressBytes := addressSize
	if p[4] == branch {
		addressBytes = 8
	}
	if len(b) < 1+addressBytes {
		return c, errPage
	}
	suffix := int(b[0])
	b = b[1:]
	if suffix == 255 {
		if len(b) != 16+longPrefix+addressBytes {
			return c, errPage
		}
		c.length = order.Uint64(b[:8])
		c.tail = order.Uint64(b[8:16])
		if c.length <= maxInline || c.length > math.MaxInt || c.tail == 0 {
			return c, errPage
		}
		c.suffix = b[16 : 16+longPrefix]
		b = b[16+longPrefix:]
	} else {
		if len(b) != suffix+addressBytes || int(p[5])+suffix > maxInline {
			return c, errPage
		}
		c.prefix = p[header : header+int(p[5])]
		c.suffix = b[:suffix]
		c.length = uint64(len(c.prefix) + suffix)
		b = b[suffix:]
	}
	if p[4] == branch {
		c.child = order.Uint64(b)
		if c.child == 0 {
			return c, errPage
		}
	} else {
		c.address = index.Address{FileID: order.Uint64(b), Offset: int64(order.Uint64(b[8:])), Bytes: int64(order.Uint64(b[16:])), ValueBytes: int64(order.Uint64(b[24:])), Sequence: order.Uint64(b[32:])}
		if c.address.Offset < 0 || c.address.Bytes < 0 || c.address.ValueBytes < 0 {
			return c, errPage
		}
	}
	return c, nil
}
func (p *page) decode() node {
	n := node{kind: p[4], left: p.link(), cells: make([]cell, p.count(), p.count()+1)}
	for i := range n.cells {
		n.cells[i], _ = p.cell(i)
	}
	return n
}
func common(a, b string) int {
	i := 0
	for i < min(len(a), len(b)) && a[i] == b[i] {
		i++
	}
	return i
}
func (n node) prefix() string {
	if len(n.cells) == 0 {
		return ""
	}
	// For sorted cells, every key shares the prefix of the first and last.
	first, last := n.cells[0].key.inline, n.cells[len(n.cells)-1].key.inline
	return first[:common(first, last)]
}
func (n node) size() int {
	prefix := n.prefix()
	used := header + len(prefix)
	for _, c := range n.cells {
		size := 2 + 1 + len(c.key.inline) - len(prefix)
		if c.key.tail != 0 {
			size = 2 + 1 + 16 + longPrefix
		}
		if n.kind == leaf {
			size += addressSize
		} else {
			size += 8
		}
		used += size
	}
	return used
}
func (n node) encode(id uint64) *page {
	p := new(page)
	p.initialize(n.kind, id)
	prefix := n.prefix()
	p[5] = byte(len(prefix))
	copy(p[header:], prefix)
	directory := header + len(prefix)
	order.PutUint16(p[6:8], uint16(len(n.cells)))
	order.PutUint16(p[8:10], uint16(directory+2*len(n.cells)))
	order.PutUint64(p[24:32], n.left)
	end := pageSize
	for i, c := range n.cells {
		payload := addressSize
		if n.kind == branch {
			payload = 8
		}
		size := 1 + len(c.key.inline) - len(prefix) + payload
		if c.key.tail != 0 {
			size = 1 + 16 + longPrefix + payload
		}
		end -= size
		b := p[end : end+size]
		if c.key.tail != 0 {
			b[0] = 255
			order.PutUint64(b[1:9], c.key.length)
			order.PutUint64(b[9:17], c.key.tail)
			copy(b[17:], c.key.inline)
			b = b[17+longPrefix:]
		} else {
			suffix := c.key.inline[len(prefix):]
			b[0] = byte(len(suffix))
			copy(b[1:], suffix)
			b = b[1+len(suffix):]
		}
		if n.kind == leaf {
			putAddress(b, c.address)
		} else {
			order.PutUint64(b, c.child)
		}
		order.PutUint16(p[directory+2*i:], uint16(end))
	}
	order.PutUint16(p[10:12], uint16(end))
	p.seal()
	return p
}
func putAddress(b []byte, a index.Address) {
	order.PutUint64(b, a.FileID)
	order.PutUint64(b[8:], uint64(a.Offset))
	order.PutUint64(b[16:], uint64(a.Bytes))
	order.PutUint64(b[24:], uint64(a.ValueBytes))
	order.PutUint64(b[32:], a.Sequence)
}
func (k keyRef) compare(s *pager, key string) (int, error) {
	if k.tail == 0 {
		return strings.Compare(k.inline, key), nil
	}
	r := stream(k, s.read)
	for {
		b, err := r.bytes()
		if err != nil {
			return 0, err
		}
		n := min(len(b), len(key))
		if c := compareBytesString(b[:n], key[:n]); c != 0 {
			return c, nil
		}
		if len(b) == 0 || n < len(b) {
			return len(b) - len(key), nil
		}
		key = key[n:]
		r.data = r.data[n:]
	}
}

// Converting inside the comparison lets Go compare the page bytes directly
// with the search key without copying either operand.
func compareBytesString(data []byte, key string) int {
	if string(data) < key {
		return -1
	}
	if string(data) > key {
		return 1
	}
	return 0
}
func (p *page) chunk(remaining uint64) ([]byte, uint64, error) {
	n := p.count()
	if p[4] != overflow || n == 0 || uint64(n) > remaining || (uint64(n) == remaining) != (p.link() == 0) {
		return nil, 0, errPage
	}
	return p[header : header+n], p.link(), nil
}
func (k keyRef) string(s *pager) (string, error) {
	if k.tail == 0 {
		return k.inline, nil
	}
	var out strings.Builder
	r := stream(k, s.read)
	for {
		b, err := r.bytes()
		if err != nil {
			return "", err
		}
		if len(b) == 0 {
			return out.String(), nil
		}
		out.Write(b)
		r.data = nil
	}
}

// Leaves use lower_bound; equality with an internal separator descends right.
func (p *page) search(s *pager, key string) (int, bool, error) {
	if p[4] != branch && p[4] != leaf {
		return 0, false, errPage
	}
	var found bool
	var searchErr error
	i := sort.Search(p.count(), func(i int) bool {
		if searchErr != nil {
			return true
		}
		c, e := p.raw(i)
		if e != nil {
			searchErr = e
			return true
		}
		v, e := c.compare(s, key)
		if e != nil {
			searchErr = e
			return true
		}
		if v == 0 {
			found = true
		}
		return v > 0 || v == 0 && p[4] == leaf
	})
	return i, found, searchErr
}
func (p *page) child(i int) uint64 {
	if i == 0 {
		return p.link()
	}
	c, _ := p.raw(i - 1)
	return c.child
}
func (p *page) setChild(i int, id uint64) {
	if i == 0 {
		order.PutUint64(p[24:32], id)
		return
	}
	end := p.cellEnd(i - 1)
	order.PutUint64(p[end-8:end], id)
}

// keyStream compares and copies overflow keys one page at a time. It can
// stop after a separator prefix without materializing either full key.
type keyStream struct {
	data                       []byte
	tail, remaining            uint64
	cycleAt, cyclePower, steps uint64
	read                       func(uint64) (*page, error)
}

func stream(k keyRef, read func(uint64) (*page, error)) *keyStream {
	return &keyStream{data: []byte(k.inline), tail: k.tail, remaining: k.length - uint64(len(k.inline)), read: read}
}
func (c *keyStream) bytes() ([]byte, error) {
	if len(c.data) > 0 || c.remaining == 0 {
		return c.data, nil
	}
	id := c.tail
	// Brent's cycle check requires constant storage and no additional reads.
	if id == 0 || id == c.cycleAt {
		return nil, errPage
	}
	if c.steps == c.cyclePower {
		c.cycleAt = id
		c.cyclePower = max(1, c.cyclePower*2)
		c.steps = 0
	}
	c.steps++
	p, err := c.read(id)
	if err != nil {
		return nil, err
	}
	c.data, c.tail, err = p.chunk(c.remaining)
	if err != nil || c.tail == id {
		return nil, errPage
	}
	c.remaining -= uint64(len(c.data))
	return c.data, nil
}
func compareKeys(a, b keyRef, read func(uint64) (*page, error)) (int, uint64, error) {
	l, r := stream(a, read), stream(b, read)
	var matched uint64
	for {
		left, err := l.bytes()
		if err != nil {
			return 0, 0, err
		}
		right, err := r.bytes()
		if err != nil {
			return 0, 0, err
		}
		if len(left) == 0 || len(right) == 0 {
			return bytes.Compare(left, right), matched, nil
		}
		length := min(len(left), len(right))
		for i := range length {
			if left[i] != right[i] {
				return int(left[i]) - int(right[i]), matched + uint64(i), nil
			}
		}
		matched += uint64(length)
		l.data = left[length:]
		r.data = right[length:]
	}
}

// validateOrder resolves overflow bytes only when the inline prefixes do not
// distinguish adjacent keys. The read function only accepts overflow pages.
func (p *page) validateOrder(read func(uint64) (*page, error)) error {
	if p[4] == overflow || p.count() < 2 {
		return nil
	}
	prev, _ := p.raw(0)
	for i := 1; i < p.count(); i++ {
		current, _ := p.raw(i)
		var cmp int
		if prev.tail == 0 && current.tail == 0 {
			cmp = bytes.Compare(prev.suffix, current.suffix)
		} else {
			var err error
			cmp, _, err = compareKeys(prev.key(), current.key(), read)
			if err != nil {
				return err
			}
		}
		if cmp >= 0 {
			return errPage
		}
		prev = current
	}
	return nil
}
