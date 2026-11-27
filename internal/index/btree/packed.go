package btree

import (
	"context"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"strings"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

// ErrInvalidPacked identifies an unusable checkpoint image. Scratch and reader
// I/O errors, cancellation, and errors returned by the visitor remain distinct.
var ErrInvalidPacked = errors.New("btree: invalid packed index")

const packedHeaderBytes = 48

// Packed images use fixed page offsets after the file header.
const PackedHeaderBytes, PackedPageBytes = packedHeaderBytes, pageSize

type packedHeader struct {
	root, pages, keys, maxKey uint64
}

func (h packedHeader) encode() [packedHeaderBytes]byte {
	var b [packedHeaderBytes]byte
	copy(b[:], "TARPBTP1")
	order.PutUint32(b[8:], pageSize)
	order.PutUint64(b[16:], h.root)
	order.PutUint64(b[24:], h.pages)
	order.PutUint64(b[32:], h.keys)
	order.PutUint64(b[40:], h.maxKey)
	order.PutUint32(b[12:], crc32.Checksum(b[16:], crcTable))
	return b
}

func decodePackedHeader(b []byte, size int64) (packedHeader, error) {
	if size < packedHeaderBytes || (size-packedHeaderBytes)%pageSize != 0 ||
		string(b[:8]) != "TARPBTP1" || order.Uint32(b[8:]) != pageSize ||
		order.Uint32(b[12:]) != crc32.Checksum(b[16:], crcTable) {
		return packedHeader{}, ErrInvalidPacked
	}
	h := packedHeader{order.Uint64(b[16:]), order.Uint64(b[24:]), order.Uint64(b[32:]), order.Uint64(b[40:])}
	payload := uint64(size - packedHeaderBytes)
	if h.pages != payload/pageSize || h.root > h.pages || h.maxKey > min(payload, math.MaxInt) ||
		h.keys > min(payload/addressSize, math.MaxInt) {
		return packedHeader{}, ErrInvalidPacked
	}
	if h == (packedHeader{}) || h.pages != 0 && h.root != 0 && h.keys != 0 && h.maxKey != 0 {
		return h, nil
	}
	return packedHeader{}, ErrInvalidPacked
}

// ExportPacked writes a fresh bulk-built tree in page-ID order, including the
// committed dirty pages still resident in its private pager. Ordinary mutable
// trees can contain free or retired pages and must first be packed by BuildSorted.
func ExportPacked(ctx context.Context, backend index.Backend, w io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t, ok := backend.(*tree)
	if !ok {
		return errors.New("btree: packed export requires a B+tree")
	}
	t.writer.Lock()
	defer t.writer.Unlock()
	if t.closed {
		return os.ErrClosed
	}
	h := t.packed
	if t.count == 0 {
		h = packedHeader{}
	} else if h.root == 0 {
		return errors.New("btree: packed export requires a fresh bulk build")
	}
	b := h.encode()
	if err := writePacked(w, b[:]); err != nil {
		return err
	}
	for id := uint64(1); id <= h.pages; id++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		p, err := t.p.read(id)
		if err != nil {
			return err
		}
		if err := writePacked(w, p[:]); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func writePacked(w io.Writer, b []byte) error {
	n, err := w.Write(b)
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

func readPacked(r io.Reader, b []byte) error {
	_, err := io.ReadFull(r, b)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrInvalidPacked
	}
	return err
}

// OpenPacked uses a verified immutable page source directly and writes mutable
// pages into a private overlay. The source must authenticate every page read
// against the same checkpoint as its header. Ownership transfers on success.
func OpenPacked(ctx context.Context, source io.ReaderAt, closer io.Closer, size int64, dir string, budget int64) (_ index.Backend, result error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if size < packedHeaderBytes {
		return nil, ErrInvalidPacked
	}
	var b [packedHeaderBytes]byte
	if err := readPacked(io.NewSectionReader(source, 0, packedHeaderBytes), b[:]); err != nil {
		return nil, err
	}
	h, err := decodePackedHeader(b[:], size)
	if err != nil {
		return nil, err
	}
	backend, err := NewIn(dir, budget)
	if err != nil {
		return nil, err
	}
	t := backend.(*tree)
	defer func() {
		if result != nil {
			result = errors.Join(result, t.Close())
		}
	}()
	t.p.base = io.NewSectionReader(source, packedHeaderBytes, size-packedHeaderBytes)
	t.p.top, t.p.basePages = h.pages, h.pages
	t.p.maxKey.Store(h.maxKey)
	t.root, t.count, t.packed = h.root, int(h.keys), h
	t.p.root.Store(h.root)
	if h.root != 0 {
		p, err := t.p.read(h.root)
		if err != nil {
			if errors.Is(err, errPage) {
				return nil, ErrInvalidPacked
			}
			return nil, err
		}
		if p[4] != branch && p[4] != leaf || p.count() == 0 {
			return nil, ErrInvalidPacked
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.p.baseCloser = closer
	return t, nil
}

// ImportPacked consumes size bytes and returns a writable private tree only
// after checking all pages and the complete routing topology. The optional
// visitor receives live entries in key order and can check DATA-specific
// bounds. Its partial results must be discarded on any error.
func ImportPacked(ctx context.Context, r io.Reader, size int64, dir string, budget int64, visit func(string, index.Address) error) (_ index.Backend, result error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if size < packedHeaderBytes {
		return nil, ErrInvalidPacked
	}
	var b [packedHeaderBytes]byte
	if err := readPacked(r, b[:]); err != nil {
		return nil, err
	}
	h, err := decodePackedHeader(b[:], size)
	if err != nil {
		return nil, err
	}
	backend, err := NewIn(dir, budget)
	if err != nil {
		return nil, err
	}
	t := backend.(*tree)
	defer func() {
		if result != nil {
			result = errors.Join(result, t.Close())
		}
	}()
	t.p.top = h.pages
	t.p.maxKey.Store(h.maxKey)
	var p page
	for id := uint64(1); id <= h.pages; id++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := readPacked(r, p[:]); err != nil {
			return nil, err
		}
		if p.check(id) != nil {
			return nil, ErrInvalidPacked
		}
		n, err := t.p.file.WriteAt(p[:], int64(id-1)*pageSize)
		if err != nil {
			return nil, err
		}
		if n != pageSize {
			return nil, io.ErrShortWrite
		}
	}
	if h.root != 0 {
		c := packedCheck{ctx: ctx, pager: t.p, visit: visit}
		if err := c.check(h); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.root, t.count, t.packed = h.root, int(h.keys), h
	t.p.root.Store(h.root)
	return t, nil
}

// One bit per page proves exclusive ownership of child and overflow links.
// Keep a single bitmap window in memory and spill larger bitmaps to scratch.
type packedSeen struct {
	bits        [4096]byte
	windowIndex int64
	file        *countedFile
	count       uint64
}

func (s *packedSeen) open(p *pager) error {
	return s.openCount(p, p.top)
}

func (s *packedSeen) openCount(p *pager, pages uint64) error {
	if pages == 0 {
		return nil
	}
	windowCount := int64((pages-1)/(uint64(len(s.bits))*8)) + 1
	if windowCount == 1 {
		return nil
	}
	f, err := p.temp()
	if err != nil {
		return err
	}
	s.file = f
	return f.Truncate(windowCount * int64(len(s.bits)))
}

func (s *packedSeen) claim(id uint64) error {
	i, bit, err := s.position(id)
	if err != nil {
		return err
	}
	if s.bits[i]&bit != 0 {
		return ErrInvalidPacked
	}
	s.bits[i] |= bit
	s.count++
	return nil
}

func (s *packedSeen) position(id uint64) (int64, byte, error) {
	offset := int64((id - 1) / 8)
	windowIndex := offset / int64(len(s.bits))
	if s.windowIndex != windowIndex {
		if _, err := s.file.WriteAt(s.bits[:], s.windowIndex*int64(len(s.bits))); err != nil {
			return 0, 0, err
		}
		if _, err := s.file.ReadAt(s.bits[:], windowIndex*int64(len(s.bits))); err != nil {
			return 0, 0, err
		}
		s.windowIndex = windowIndex
	}
	return offset % int64(len(s.bits)), byte(1 << ((id - 1) % 8)), nil
}

type packedCheck struct {
	ctx            context.Context
	pager          *pager
	seen           packedSeen
	visit          func(string, index.Address) error
	previous       keyRef
	separator      bool
	count, longest uint64
}

// read maps structural errors from the private page codec to ErrInvalidPacked.
// Other I/O errors and visitor errors keep their original meaning.
func (c *packedCheck) read(id uint64) (*page, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, err
	}
	p, err := c.pager.read(id)
	if errors.Is(err, errPage) {
		return nil, ErrInvalidPacked
	}
	return p, err
}

func (c *packedCheck) check(h packedHeader) (result error) {
	defer func() {
		if c.seen.file != nil {
			result = errors.Join(result, c.seen.file.Close())
		}
	}()
	if err := c.seen.open(c.pager); err != nil {
		return err
	}
	stack := []frame{{id: h.root, pos: -1}}
	leafDepth := 0
	for len(stack) != 0 {
		f := &stack[len(stack)-1]
		p, err := c.read(f.id)
		if err != nil {
			return err
		}
		if f.pos < 0 {
			if err := c.seen.claim(f.id); err != nil {
				return err
			}
			f.pos = 0
			switch p[4] {
			case branch:
				if len(stack) >= maxDepth || len(stack) == 1 && p.count() == 0 {
					return ErrInvalidPacked
				}
				stack = append(stack, frame{id: p.link(), pos: -1})
				continue
			case leaf:
				if p.link() != 0 || p.count() == 0 || leafDepth != 0 && len(stack) != leafDepth {
					return ErrInvalidPacked
				}
				leafDepth = len(stack)
			default:
				return ErrInvalidPacked
			}
		}
		if f.pos == p.count() {
			stack = stack[:len(stack)-1]
			continue
		}
		cel, err := p.cell(f.pos)
		if err != nil {
			return ErrInvalidPacked
		}
		f.pos++
		isBranch := p[4] == branch
		if err := c.entry(cel, isBranch); err != nil {
			return err
		}
		if isBranch {
			stack = append(stack, frame{id: cel.child, pos: -1})
		}
	}
	if c.seen.count != h.pages || c.count != h.keys || c.longest != h.maxKey {
		return ErrInvalidPacked
	}
	return nil
}

func (c *packedCheck) entry(cell cell, branch bool) error {
	ref := cell.key
	if ref.length == 0 {
		return ErrInvalidPacked
	}
	if c.count != 0 {
		// In-order traversal interleaves leaves and separators. Each separator
		// is strictly above its left subtree and at or below its right subtree.
		cmp, _, err := compareKeys(c.previous, ref, c.read)
		if err != nil {
			if errors.Is(err, errPage) {
				return ErrInvalidPacked
			}
			return err
		}
		if cmp > 0 || cmp == 0 && !c.separator {
			return ErrInvalidPacked
		}
	}
	c.previous, c.separator = ref, branch
	key := ref.inline
	if ref.tail != 0 {
		var materialized strings.Builder
		needKey := !branch && c.visit != nil
		if needKey {
			materialized.WriteString(ref.inline)
		}
		id, left := ref.tail, ref.length-uint64(len(ref.inline))
		for left != 0 {
			p, err := c.read(id)
			if err != nil {
				return err
			}
			if err := c.seen.claim(id); err != nil {
				return err
			}
			b, next, err := p.chunk(left)
			if err != nil {
				return ErrInvalidPacked
			}
			if needKey {
				materialized.Write(b)
			}
			left -= uint64(len(b))
			id = next
		}
		key = materialized.String()
	}
	if branch {
		return nil
	}
	c.count++
	c.longest = max(c.longest, ref.length)
	a := cell.address
	if a.FileID == 0 || a.Sequence == 0 || a.RecordOffset < format.SegmentHeaderBytes ||
		a.RecordBytes < format.RecordHeaderBytes || a.ValueBytes > a.RecordBytes-format.RecordHeaderBytes ||
		uint64(a.RecordBytes-format.RecordHeaderBytes-a.ValueBytes) != ref.length || a.RecordOffset > math.MaxInt64-a.RecordBytes {
		return ErrInvalidPacked
	}
	if c.visit != nil {
		return c.visit(key, a)
	}
	return nil
}
