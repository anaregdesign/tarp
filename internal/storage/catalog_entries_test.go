package storage

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestCatalogEntriesSplicePreservesCapturedViews(t *testing.T) {
	random := rand.New(rand.NewPCG(17, 23))
	var catalog CatalogEntries
	var expected []format.CatalogEntry
	for id := uint64(1); id <= 5000; id++ {
		start := random.IntN(len(expected) + 1)
		end := start + random.IntN(min(3, len(expected)-start+1))
		insert := []format.CatalogEntry{{ID: id}, {ID: uint64(id + 5000)}}
		previous := catalog
		before := slices.Clone(expected)
		catalog = catalog.splice(start, end-start, insert)
		expected = slices.Replace(expected, start, end, insert...)
		if !slices.Equal(slices.Collect(previous.All()), before) ||
			!slices.Equal(catalog.Slice(0, catalog.Len()), expected) {
			t.Fatalf("catalog splice %d did not preserve order and the old view", id)
		}
		if catalog.At(start) != insert[0] {
			t.Fatalf("catalog splice %d misplaced new entry", id)
		}
	}
	verifyCatalogBalance(t, catalog.root)
}

func verifyCatalogBalance(t *testing.T, node *catalogNode) {
	t.Helper()
	if node == nil {
		return
	}
	left, right := node.left.depth(), node.right.depth()
	if left > right+1 || right > left+1 || node.height != 1+max(left, right) ||
		node.size != 1+node.left.len()+node.right.len() {
		t.Fatal("catalog tree lost balance or subtree accounting")
	}
	verifyCatalogBalance(t, node.left)
	verifyCatalogBalance(t, node.right)
}
