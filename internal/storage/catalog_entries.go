package storage

import (
	"iter"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
)

// CatalogEntries is a stable view of an ordered catalog. Publishing an edit
// copies only the affected paths. Existing captures can walk the previous tree
// without synchronizing with a rotation or a cleaner publication.
type CatalogEntries struct {
	root *catalogNode
}

type catalogNode struct {
	entry       format.CatalogEntry
	left, right *catalogNode
	size        int
	height      int
}

func (n *catalogNode) len() int {
	if n == nil {
		return 0
	}
	return n.size
}

func (n *catalogNode) depth() int {
	if n == nil {
		return 0
	}
	return n.height
}

func catalogBranch(left *catalogNode, entry format.CatalogEntry, right *catalogNode) *catalogNode {
	return &catalogNode{entry: entry, left: left, right: right,
		size: left.len() + 1 + right.len(), height: 1 + max(left.depth(), right.depth())}
}

func catalogFromSlice(entries []format.CatalogEntry) *catalogNode {
	if len(entries) == 0 {
		return nil
	}
	middle := len(entries) / 2
	return catalogBranch(catalogFromSlice(entries[:middle]), entries[middle], catalogFromSlice(entries[middle+1:]))
}

func catalogBalance(left *catalogNode, entry format.CatalogEntry, right *catalogNode) *catalogNode {
	if left.depth() > right.depth()+1 {
		if left.left.depth() >= left.right.depth() {
			return catalogBranch(left.left, left.entry, catalogBranch(left.right, entry, right))
		}
		middle := left.right
		return catalogBranch(catalogBranch(left.left, left.entry, middle.left),
			middle.entry, catalogBranch(middle.right, entry, right))
	}
	if right.depth() > left.depth()+1 {
		if right.right.depth() >= right.left.depth() {
			return catalogBranch(catalogBranch(left, entry, right.left), right.entry, right.right)
		}
		middle := right.left
		return catalogBranch(catalogBranch(left, entry, middle.left),
			middle.entry, catalogBranch(middle.right, right.entry, right.right))
	}
	return catalogBranch(left, entry, right)
}

// Join two ordered trees using the entry between them.
func catalogJoin(left *catalogNode, entry format.CatalogEntry, right *catalogNode) *catalogNode {
	if left.depth() > right.depth()+1 {
		return catalogBalance(left.left, left.entry, catalogJoin(left.right, entry, right))
	}
	if right.depth() > left.depth()+1 {
		return catalogBalance(catalogJoin(left, entry, right.left), right.entry, right.right)
	}
	return catalogBranch(left, entry, right)
}

func catalogSplit(root *catalogNode, position int) (*catalogNode, *catalogNode) {
	if position == 0 {
		return nil, root
	}
	if position == root.len() {
		return root, nil
	}
	if position <= root.left.len() {
		before, after := catalogSplit(root.left, position)
		return before, catalogJoin(after, root.entry, root.right)
	}
	before, after := catalogSplit(root.right, position-root.left.len()-1)
	return catalogJoin(root.left, root.entry, before), after
}

func catalogConcat(left, right *catalogNode) *catalogNode {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	first, rest := catalogSplit(right, 1)
	return catalogJoin(left, first.entry, rest)
}

func (v CatalogEntries) splice(start, remove int, insert []format.CatalogEntry) CatalogEntries {
	if remove == 0 && len(insert) == 0 {
		return v
	}
	if start == v.Len() && remove == 0 && len(insert) == 1 {
		return CatalogEntries{catalogJoin(v.root, insert[0], nil)}
	}
	before, rest := catalogSplit(v.root, start)
	_, after := catalogSplit(rest, remove)
	return CatalogEntries{catalogConcat(catalogConcat(before, catalogFromSlice(insert)), after)}
}

func (v CatalogEntries) Len() int { return v.root.len() }

func (v CatalogEntries) At(position int) format.CatalogEntry {
	if position < 0 || position >= v.Len() {
		panic("storage: catalog entry index out of bounds")
	}
	for n := v.root; ; {
		left := n.left.len()
		if position == left {
			return n.entry
		}
		if position < left {
			n = n.left
			continue
		}
		position -= left + 1
		n = n.right
	}
}

func (n *catalogNode) walk(start, end int, yield func(format.CatalogEntry) bool) bool {
	if n == nil || end <= start {
		return true
	}
	left := n.left.len()
	if start < left && !n.left.walk(start, min(end, left), yield) {
		return false
	}
	if start <= left && left < end && !yield(n.entry) {
		return false
	}
	return end <= left+1 || n.right.walk(max(0, start-left-1), end-left-1, yield)
}

func (v CatalogEntries) All() iter.Seq[format.CatalogEntry] {
	return func(yield func(format.CatalogEntry) bool) { v.root.walk(0, v.Len(), yield) }
}

func (v CatalogEntries) Slice(start, end int) []format.CatalogEntry {
	if start < 0 || end < start || end > v.Len() {
		panic("storage: catalog entry slice out of bounds")
	}
	if start == end {
		return nil
	}
	entries := make([]format.CatalogEntry, 0, end-start)
	return slices.AppendSeq(entries, func(yield func(format.CatalogEntry) bool) {
		v.root.walk(start, end, yield)
	})
}

func (v CatalogEntries) Index(entry format.CatalogEntry) int {
	if entry.Kind != format.SnapshotKind {
		offset := 0
		for n := v.root; n != nil; {
			if n.entry.FirstSequence < entry.FirstSequence {
				offset += n.left.len() + 1
				n = n.right
			} else if n.entry.FirstSequence > entry.FirstSequence {
				n = n.left
			} else if n.entry == entry {
				return offset + n.left.len()
			} else {
				return -1
			}
		}
		return -1
	}
	position := 0
	for item := range v.All() {
		if item == entry {
			return position
		}
		if item.Kind != format.SnapshotKind {
			break
		}
		position++
	}
	return -1
}

func (s *Store) CatalogEntries() CatalogEntries { return s.catalogEntries }

func (s *Store) CatalogMetadataBytes() uint64 { return s.catalogBytes + format.FrontierBytes }

func (s *Store) sealedThroughSequence() uint64 {
	if n := s.catalogEntries.Len(); n > 0 {
		if last := s.catalogEntries.At(n - 1); last.Kind != format.SnapshotKind {
			return last.LastSequence
		}
	}
	return s.Catalog.CheckpointCutoffSequence
}

func (s *Store) CatalogSnapshot() format.Catalog {
	catalog := s.Catalog
	catalog.Entries = s.catalogEntries.Slice(0, s.catalogEntries.Len())
	return catalog
}
