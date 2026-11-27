package btree

import (
	"errors"
	"slices"
	"strings"

	"github.com/anaregdesign/tarp/internal/index"
)

type transaction struct {
	tree                 *tree
	root, rootGeneration uint64
	count                int
	allocated, old       idLog
	err                  error
	done                 bool
	packed               bool
}

func (x *transaction) Lookup(key string) (index.Address, bool, error) {
	if x.err != nil {
		return index.Address{}, false, x.err
	}
	return lookup(x.tree.p, x.root, key)
}
func (x *transaction) alloc() (uint64, error) {
	p := x.tree.p
	id, err := p.allocate()
	if err != nil {
		return 0, err
	}
	if err = x.allocated.add(p, id); err != nil {
		return 0, errors.Join(err, p.release(id))
	}
	return id, nil
}
func (x *transaction) write(n node) (uint64, error) {
	if n.size() > pageSize {
		return 0, errPage
	}
	id, err := x.alloc()
	if err != nil {
		return 0, err
	}
	return id, x.tree.p.write(id, n.encode(id), x.rootGeneration)
}

// An overwrite changes the leaf address and, on its COW path, child IDs.
// The caller patches a copy so every page buffer returned by the pager
// remains immutable, including buffers for evicted private pages.
func (x *transaction) writePatched(id uint64, p *page) (replacement, error) {
	if !x.tree.p.private(id, x.rootGeneration) {
		if err := x.retire(id); err != nil {
			return replacement{}, err
		}
		next, err := x.alloc()
		if err != nil {
			return replacement{}, err
		}
		id = next
		p.initialize(p[4], id)
	}
	p.seal()
	return replacement{left: id}, x.tree.p.write(id, p, x.rootGeneration)
}
func (x *transaction) key(key string) (keyRef, error) {
	ref := keyRef{inline: key, length: uint64(len(key))}
	if ref.length > x.tree.p.maxKey.Load() {
		x.tree.p.maxKey.Store(ref.length)
	}
	if len(key) <= maxInline {
		return ref, nil
	}
	ref.inline = strings.Clone(key[:inlinePrefixBytes])
	for end := len(key); end > inlinePrefixBytes; {
		start := max(inlinePrefixBytes, end-(pageSize-header))
		id, err := x.alloc()
		if err != nil {
			return keyRef{}, err
		}
		p := new(page)
		p.initialize(overflow, id)
		order.PutUint16(p[6:8], uint16(end-start))
		order.PutUint64(p[24:32], ref.tail)
		copy(p[header:], key[start:end])
		p.seal()
		if err = x.tree.p.write(id, p, x.rootGeneration); err != nil {
			return keyRef{}, err
		}
		ref.tail = id
		end = start
	}
	return ref, nil
}
func (x *transaction) retire(id uint64) error {
	if id <= x.tree.p.basePages {
		return nil
	}
	return x.old.add(x.tree.p, id)
}

func (x *transaction) retireKey(k keyRef) error {
	if k.tail != 0 && k.tail <= x.tree.p.basePages {
		return nil
	}
	r := stream(k, x.tree.p.read)
	r.data = nil
	for r.remaining > 0 {
		id := r.tail
		if _, err := r.bytes(); err != nil {
			return err
		}
		if err := x.retire(id); err != nil {
			return err
		}
		r.data = nil
	}
	return nil
}
func (x *transaction) Put(key string, a index.Address) error {
	_, _, err := x.Apply(index.Mutation{Key: key, Address: a})
	return err
}
func (x *transaction) Delete(key string) error {
	_, _, err := x.Apply(index.Mutation{Key: key, Delete: true})
	return err
}

type mutation struct {
	index.Mutation
	previous index.Address
	existed  bool
}

func (x *transaction) Apply(m index.Mutation) (index.Address, bool, error) {
	if x.err != nil {
		return index.Address{}, false, &index.LookupError{Err: x.err}
	}
	if x.done {
		return index.Address{}, false, errors.New("btree: finished transaction")
	}
	edit := mutation{Mutation: m}
	res, err := x.update(x.root, &edit, 0)
	if err == nil && res.right != 0 {
		res.left, err = x.write(node{kind: branch, left: res.left, cells: []cell{{key: res.separator, child: res.right}}})
	}
	if err == nil && m.Delete && edit.existed {
		for res.left != 0 {
			p, e := x.tree.p.read(res.left)
			if e != nil {
				err = e
				break
			}
			if p[4] != branch || p.count() != 0 {
				break
			}
			if err = x.retire(res.left); err != nil {
				break
			}
			res.left = p.link()
		}
	}
	if err != nil {
		x.err = err
		return edit.previous, edit.existed, err
	}
	x.root = res.left
	return edit.previous, edit.existed, nil
}

type replacement struct {
	left, right uint64
	separator   keyRef
}

func (x *transaction) update(id uint64, edit *mutation, depth int) (replacement, error) {
	if depth >= maxDepth {
		return replacement{}, &index.LookupError{Err: errPage}
	}
	var p *page
	var err error
	slot := 0
	found := false
	if id != 0 {
		p, err = x.tree.p.read(id)
		if err != nil {
			return replacement{}, &index.LookupError{Err: err}
		}
		slot, found, err = p.search(x.tree.p, edit.Key)
		if err != nil {
			return replacement{}, &index.LookupError{Err: err}
		}
	}
	var n node
	if id == 0 || p[4] == leaf {
		if edit.Delete && !found {
			return replacement{left: id}, nil
		}
		if found && !edit.Delete {
			old, err := p.raw(slot)
			if err != nil {
				return replacement{}, &index.LookupError{Err: err}
			}
			edit.previous, edit.existed = old.address, true
			updated := *p
			end := p.cellEnd(slot)
			putAddress(updated[end-addressSize:end], edit.Address)
			return x.writePatched(id, &updated)
		}
		if id != 0 {
			n = p.decode()
		} else {
			n.kind = leaf
		}
		if edit.Delete {
			edit.previous, edit.existed = n.cells[slot].address, true
			if err = x.retireKey(n.cells[slot].key); err != nil {
				return replacement{}, err
			}
			n.cells = slices.Delete(n.cells, slot, slot+1)
			x.count--
		} else {
			ref, e := x.key(edit.Key)
			if e != nil {
				return replacement{}, e
			}
			n.cells = slices.Insert(n.cells, slot, cell{key: ref, address: edit.Address})
			x.count++
		}
	} else {
		child := p.child(slot)
		change, e := x.update(child, edit, depth+1)
		if e != nil {
			return replacement{}, e
		}
		if change.left == child && change.right == 0 {
			return replacement{left: id}, nil
		}
		if edit.existed && !edit.Delete {
			updated := *p
			updated.setChild(slot, change.left)
			return x.writePatched(id, &updated)
		}
		n = p.decode()
		if change.left == 0 {
			if len(n.cells) == 0 {
				n.left = 0
			} else {
				pos := max(slot-1, 0)
				if slot == 0 {
					n.left = n.cells[0].child
				}
				if e = x.retireKey(n.cells[pos].key); e != nil {
					return replacement{}, e
				}
				n.cells = slices.Delete(n.cells, pos, pos+1)
			}
		} else {
			if slot == 0 {
				n.left = change.left
			} else {
				n.cells[slot-1].child = change.left
			}
			if change.right != 0 {
				n.cells = slices.Insert(n.cells, slot, cell{key: change.separator, child: change.right})
			} else if edit.Delete && len(n.cells) > 0 {
				if e = x.mergeChildren(&n, max(slot-1, 0)); e != nil {
					return replacement{}, e
				}
			}
		}
	}
	if len(n.cells) > 0 && n.size() <= pageSize && id != 0 && x.tree.p.private(id, x.rootGeneration) {
		return replacement{left: id}, x.tree.p.write(id, n.encode(id), x.rootGeneration)
	}
	if id != 0 {
		if err = x.retire(id); err != nil {
			return replacement{}, err
		}
	}
	if len(n.cells) == 0 && n.left == 0 {
		return replacement{}, nil
	}
	if n.size() <= pageSize {
		updated, err := x.write(n)
		return replacement{left: updated}, err
	}
	return x.split(n)
}
func (x *transaction) mergeChildren(parent *node, sep int) error {
	left := parent.left
	if sep > 0 {
		left = parent.cells[sep-1].child
	}
	right := parent.cells[sep].child
	a, err := x.tree.p.read(left)
	if err != nil {
		return err
	}
	b, err := x.tree.p.read(right)
	if err != nil {
		return err
	}
	if a[4] != b[4] {
		return errPage
	}
	// The merged prefix cannot be longer than either sibling's prefix, so
	// their cells need at least as much space as they already occupy.
	if header+a.packedCellBytes()+b.packedCellBytes() > pageSize {
		return nil
	}
	merged := a.decode()
	r := b.decode()
	if merged.kind == branch {
		merged.cells = append(merged.cells, cell{key: parent.cells[sep].key, child: r.left})
	}
	merged.cells = append(merged.cells, r.cells...)
	if merged.size() > pageSize {
		return nil
	}
	if merged.kind == leaf {
		if err = x.retireKey(parent.cells[sep].key); err != nil {
			return err
		}
	}
	id, err := x.write(merged)
	if err != nil {
		return err
	}
	if err = x.retire(left); err != nil {
		return err
	}
	if err = x.retire(right); err != nil {
		return err
	}
	if sep == 0 {
		parent.left = id
	} else {
		parent.cells[sep-1].child = id
	}
	parent.cells = slices.Delete(parent.cells, sep, sep+1)
	return nil
}
func (x *transaction) split(n node) (replacement, error) {
	// Try the middle first. A new outlying key can remove a long shared
	// prefix from a previously compact page, requiring an asymmetric split.
	mid := len(n.cells) / 2
	for d := 0; d <= len(n.cells); d++ {
		for _, i := range [2]int{mid - d, mid + d} {
			if i <= 0 || i >= len(n.cells) {
				continue
			}
			left := node{kind: n.kind, left: n.left, cells: n.cells[:i]}
			right := node{kind: n.kind, cells: n.cells[i:]}
			if n.kind == branch {
				right.left = n.cells[i].child
				right.cells = n.cells[i+1:]
			}
			if left.size() > pageSize || right.size() > pageSize {
				continue
			}
			sep := n.cells[i].key
			if n.kind == leaf {
				cmp, matched, err := compareKeys(n.cells[i-1].key, n.cells[i].key, x.tree.p.read)
				if err != nil {
					return replacement{}, err
				}
				if cmp >= 0 {
					return replacement{}, errPage
				}
				sep, err = x.shortKey(n.cells[i].key, matched+1)
				if err != nil {
					return replacement{}, err
				}
			}
			l, err := x.write(left)
			if err != nil {
				return replacement{}, err
			}
			r, err := x.write(right)
			if err != nil {
				return replacement{}, err
			}
			return replacement{left: l, right: r, separator: sep}, nil
		}
	}
	return replacement{}, errPage
}
func (x *transaction) Commit() {
	if x.done {
		return
	}
	if x.err != nil {
		x.Abort()
		return
	}
	t := x.tree
	t.mu.Lock()
	t.root, t.count, t.rootGeneration = x.root, x.count, t.rootGeneration+1
	t.packed = packedHeader{}
	if x.packed {
		t.packed = packedHeader{root: x.root, pages: t.p.top, keys: uint64(x.count), maxKey: t.p.maxKey.Load()}
	}
	t.p.root.Store(x.root)
	t.retiredPages.Add(uint64(x.old.n) + uint64(x.old.bytes/8))
	t.mu.Unlock()
	t.pending = x
	x.done = true
	t.writer.Unlock()
}
func (x *transaction) Abort() {
	if x.done {
		return
	}
	x.done = true
	p := x.tree.p
	err := x.allocated.each(p.release)
	err = errors.Join(err, x.allocated.close(), x.old.close())
	if err != nil {
		x.tree.allocErr = err
	}
	x.tree.writer.Unlock()
}

// shortKey owns a copy of the separator, even when it spans overflow pages.
// Leaf deletion can therefore reclaim the leaf's key independently.
func (x *transaction) shortKey(source keyRef, length uint64) (keyRef, error) {
	reader := stream(source, x.tree.p.read)
	copyPrefix := func(out []byte) error {
		for len(out) > 0 {
			b, err := reader.bytes()
			if err != nil {
				return err
			}
			if len(b) == 0 {
				return errPage
			}
			n := copy(out, b)
			out = out[n:]
			reader.data = b[n:]
		}
		return nil
	}
	if length <= maxInline {
		buf := make([]byte, int(length))
		err := copyPrefix(buf)
		return keyRef{inline: string(buf), length: length}, err
	}
	inline := make([]byte, inlinePrefixBytes)
	if err := copyPrefix(inline); err != nil {
		return keyRef{}, err
	}
	id, err := x.alloc()
	if err != nil {
		return keyRef{}, err
	}
	key := keyRef{inline: string(inline), length: length, tail: id}
	for remaining := length - inlinePrefixBytes; remaining > 0; {
		p := new(page)
		p.initialize(overflow, id)
		n := min(remaining, uint64(pageSize-header))
		if err := copyPrefix(p[header : header+n]); err != nil {
			return keyRef{}, err
		}
		order.PutUint16(p[6:8], uint16(n))
		remaining -= n
		var next uint64
		if remaining != 0 {
			next, err = x.alloc()
			if err != nil {
				return keyRef{}, err
			}
			order.PutUint64(p[24:32], next)
		}
		p.seal()
		if err = x.tree.p.write(id, p, x.rootGeneration); err != nil {
			return keyRef{}, err
		}
		id = next
	}
	return key, nil
}
