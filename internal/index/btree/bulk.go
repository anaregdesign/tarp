package btree

import (
	"bufio"
	"context"
	"errors"
	"hash/crc32"
	"io"

	"github.com/anaregdesign/tarp/internal/index"
)

func (t *tree) BulkWorkspace() (directory string, bytes int64) {
	return t.p.dir, t.p.budget / 4
}

// A packed page is assembled in memory while sorted records arrive. Keep
// the size with compression disabled so checking the new page-wide shared
// prefix after an append is constant work, even for hundreds of short keys.
type packed struct {
	node
	base, short int
}

func newPacked(kind byte, left uint64) packed {
	return packed{node: node{kind: kind, left: left}, base: header}
}
func (p *packed) add(c cell) bool {
	sz := 2 + 1 + len(c.key.inline)
	short := p.short
	if c.key.tail != 0 {
		sz += 16
	} else {
		short++
	}
	if p.kind == branch {
		sz += 8
	} else {
		sz += addressSize
	}
	first := c.key.inline
	if len(p.cells) > 0 {
		first = p.cells[0].key.inline
	}
	total := p.base + sz + common(first, c.key.inline)*(1-short)
	if total > pageSize {
		return false
	}
	p.base += sz
	p.short = short
	p.cells = append(p.cells, c)
	return true
}

// A level stores the first child directly. Once a second child exists,
// stream remaining boundary keys and page IDs through disposable scratch;
// the number of live page buffers is independent of the tree height.
type level struct {
	s     *pager
	first cell
	count uint64
	file  *countedFile
	out   *bufio.Writer
}

func (l *level) add(boundary keyRef, id uint64) error {
	if l.count == 0 {
		l.first = cell{key: boundary, child: id}
		l.count = 1
		return nil
	}
	if l.file == nil {
		f, err := l.s.temp()
		if err != nil {
			return err
		}
		l.file = f
		l.out = bufio.NewWriterSize(f, 4096)
	}
	var b [29 + maxInline]byte
	n := len(boundary.inline)
	b[4] = byte(n)
	order.PutUint64(b[5:], id)
	order.PutUint64(b[13:], boundary.length)
	order.PutUint64(b[21:], boundary.tail)
	copy(b[29:], boundary.inline)
	record := b[:29+n]
	order.PutUint32(b[:4], crc32.Checksum(record[4:], crcTable))
	_, err := l.out.Write(record)
	if err == nil {
		l.count++
	}
	return err
}
func (l *level) all(visit func(cell) error) error {
	if l.count == 0 {
		return nil
	}
	if err := visit(l.first); err != nil {
		return err
	}
	if l.count == 1 {
		return nil
	}
	if err := l.out.Flush(); err != nil {
		return err
	}
	rd := bufio.NewReaderSize(io.NewSectionReader(l.file, 0, l.file.size), 4096)
	var b [29 + maxInline]byte
	for i := uint64(1); i < l.count; i++ {
		if _, err := io.ReadFull(rd, b[:29]); err != nil {
			return err
		}
		n := int(b[4])
		if n > maxInline {
			return errPage
		}
		if _, err := io.ReadFull(rd, b[29:29+n]); err != nil {
			return err
		}
		if order.Uint32(b[:4]) != crc32.Checksum(b[4:29+n], crcTable) {
			return errPage
		}
		ref := keyRef{inline: string(b[29 : 29+n]), length: order.Uint64(b[13:21]), tail: order.Uint64(b[21:29])}
		if err := visit(cell{key: ref, child: order.Uint64(b[5:13])}); err != nil {
			return err
		}
	}
	return nil
}
func (l *level) close() error {
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (t *tree) BuildSorted(ctx context.Context, produce func(put func(string, index.Address) error) error) (result error) {
	x := t.Begin().(*transaction)
	defer x.Abort()
	if err := ctx.Err(); err != nil {
		return err
	}
	if x.err != nil {
		return x.err
	}
	if x.count != 0 {
		return errors.New("btree: bulk construction requires an empty index")
	}
	current := &level{s: t.p}
	defer func() { result = errors.Join(result, current.close()) }()
	leafPage := newPacked(leaf, 0)
	var boundary, prev keyRef
	emit := func(page node, lower keyRef, level *level) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		id, err := x.write(page)
		if err != nil {
			return err
		}
		return level.add(lower, id)
	}
	put := func(key string, a index.Address) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		k, err := x.key(key)
		if err != nil {
			return err
		}
		var matched uint64
		if x.count > 0 {
			var cmp int
			cmp, matched, err = compareKeys(prev, k, t.p.read)
			if err != nil {
				return err
			}
			if cmp >= 0 {
				return errors.New("btree: sorted input is not strictly increasing")
			}
		}
		c := cell{key: k, address: a}
		if !leafPage.add(c) {
			if err := emit(leafPage.node, boundary, current); err != nil {
				return err
			}
			boundary, err = x.shortKey(k, matched+1)
			if err != nil {
				return err
			}
			leafPage = newPacked(leaf, 0)
			if !leafPage.add(c) {
				return errPage
			}
		}
		x.count++
		prev = k
		return nil
	}
	var inputErr error
	err := produce(func(key string, a index.Address) error {
		if inputErr == nil {
			inputErr = put(key, a)
		}
		return inputErr
	})
	if inputErr != nil {
		return inputErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	if x.count == 0 {
		x.Commit()
		return nil
	}
	if err := emit(leafPage.node, boundary, current); err != nil {
		return err
	}
	leafPage = packed{} // Release leaf working memory before assembling branches.
	for depth := 1; current.count > 1; depth++ {
		if depth >= maxDepth {
			return errPage
		}
		next := &level{s: t.p}
		parent := newPacked(branch, 0)
		err = current.all(func(child cell) error {
			if parent.left == 0 {
				parent.left = child.child
				boundary = child.key
				return nil
			}
			if parent.add(child) {
				return nil
			}
			if err := emit(parent.node, boundary, next); err != nil {
				return err
			}
			parent = newPacked(branch, child.child)
			boundary = child.key
			return nil
		})
		if err == nil {
			err = emit(parent.node, boundary, next)
		}
		if err = errors.Join(err, current.close()); err != nil {
			return errors.Join(err, next.close())
		}
		current = next
	}
	x.root = current.first.child
	x.Commit()
	return nil
}
