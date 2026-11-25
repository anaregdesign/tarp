package tarp_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp"
)

type snapshotCompositeKey struct {
	Namespace string
	Seq       int64
	Tag       string
}

func TestTypedSnapshotOperations(t *testing.T) {
	ctx := context.Background()
	keyCodec, err := tarp.NewOrderedCodec[snapshotCompositeKey]()
	if err != nil {
		t.Fatal(err)
	}
	db, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, keyCodec, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()

	var wb tarp.TypedWriteBatch[snapshotCompositeKey, string]
	wb.Put(snapshotCompositeKey{Namespace: "ns1", Seq: 1, Tag: "a"}, "one")
	wb.Put(snapshotCompositeKey{Namespace: "ns1", Seq: 2, Tag: "b"}, "two")
	wb.Put(snapshotCompositeKey{Namespace: "ns2", Seq: 1, Tag: "c"}, "three")
	if err := db.Apply(&wb); err != nil {
		t.Fatal(err)
	}

	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()

	if snap.Sequence() != 3 {
		t.Fatalf("TypedSnapshot.Sequence() = %d, want 3", snap.Sequence())
	}

	// Mutate live TypedDB after taking TypedSnapshot.
	if err := db.Delete(snapshotCompositeKey{Namespace: "ns1", Seq: 1, Tag: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(snapshotCompositeKey{Namespace: "ns1", Seq: 2, Tag: "b"}, "updated"); err != nil {
		t.Fatal(err)
	}

	got, err := snap.Get(snapshotCompositeKey{Namespace: "ns1", Seq: 1, Tag: "a"})
	if err != nil || got != "one" {
		t.Fatalf("snap.Get = %q, %v; want \"one\"", got, err)
	}

	batchRes, err := snap.BatchGet([]snapshotCompositeKey{
		{Namespace: "ns1", Seq: 2, Tag: "b"},
		{Namespace: "ns9", Seq: 9, Tag: "z"},
	})
	if err != nil || !batchRes[0].Found || batchRes[0].Value != "two" || batchRes[1].Found {
		t.Fatalf("snap.BatchGet = %+v, %v", batchRes, err)
	}

	pfx, err := tarp.EncodeOrderedTuplePrefix("ns1")
	if err != nil {
		t.Fatal(err)
	}
	lo, hi := tarp.PrefixRange(pfx)

	var vals []string
	for e, err := range snap.ScanRange(ctx, lo, hi, tarp.WithReverse()) {
		if err != nil {
			t.Fatalf("snap.ScanRange: %v", err)
		}
		vals = append(vals, e.Value)
	}
	if len(vals) != 2 || vals[0] != "two" || vals[1] != "one" {
		t.Fatalf("snap.ScanRange(reverse) = %v, want [two one]", vals)
	}

	var pfxEntries []string
	for e, err := range snap.ScanPrefix(ctx, pfx) {
		if err != nil {
			t.Fatalf("snap.ScanPrefix: %v", err)
		}
		pfxEntries = append(pfxEntries, e.Value)
	}
	slices.Sort(pfxEntries)
	if len(pfxEntries) != 2 || pfxEntries[0] != "one" || pfxEntries[1] != "two" {
		t.Fatalf("snap.ScanPrefix = %v, want [one two]", pfxEntries)
	}

	var seqs []int64
	for k, err := range snap.KeysPrefix(ctx, pfx) {
		if err != nil {
			t.Fatalf("snap.KeysPrefix: %v", err)
		}
		seqs = append(seqs, k.Seq)
	}
	if len(seqs) != 2 {
		t.Fatalf("snap.KeysPrefix count = %d, want 2", len(seqs))
	}

	var allKeys int
	for _, err := range snap.Keys(ctx) {
		if err != nil {
			t.Fatalf("snap.Keys: %v", err)
		}
		allKeys++
	}
	if allKeys != 3 {
		t.Fatalf("snap.Keys count = %d, want 3", allKeys)
	}

	var allEntries int
	for _, err := range snap.Scan(ctx) {
		if err != nil {
			t.Fatalf("snap.Scan: %v", err)
		}
		allEntries++
	}
	if allEntries != 3 {
		t.Fatalf("snap.Scan count = %d, want 3", allEntries)
	}

	var rangeKeys []int64
	for k, err := range snap.KeysRange(ctx, lo, hi) {
		if err != nil {
			t.Fatalf("snap.KeysRange: %v", err)
		}
		rangeKeys = append(rangeKeys, k.Seq)
	}
	if len(rangeKeys) != 2 || rangeKeys[0] != 1 || rangeKeys[1] != 2 {
		t.Fatalf("snap.KeysRange = %v, want [1 2]", rangeKeys)
	}

	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := snap.Get(snapshotCompositeKey{Namespace: "ns1", Seq: 1, Tag: "a"}); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("TypedSnapshot.Get after Close = %v, want ErrClosed", err)
	}
}
