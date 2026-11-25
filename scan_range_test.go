package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

func TestPrefixRangeBoundsAndOwnership(t *testing.T) {
	if l, u := tarp.PrefixRange(nil); l != nil || u != nil {
		t.Fatalf("PrefixRange(nil) = (%v, %v), want (nil, nil)", l, u)
	}
	if l, u := tarp.PrefixRange([]byte{}); l != nil || u != nil {
		t.Fatalf("PrefixRange(empty) = (%v, %v), want (nil, nil)", l, u)
	}

	orig := []byte{'a', 'b', 0xff}
	lower, upper := tarp.PrefixRange(orig)
	if !bytes.Equal(lower, []byte{'a', 'b', 0xff}) || !bytes.Equal(upper, []byte{'a', 'c'}) {
		t.Fatalf("PrefixRange(ab\\xff) = (%x, %x), want (6162ff, 6163)", lower, upper)
	}
	orig[0] = 'z'
	lower[0] = 'x'
	if !bytes.Equal(upper, []byte{'a', 'c'}) {
		t.Fatalf("upper aliased input or lower: %x", upper)
	}

	allFF := []byte{0xff, 0xff}
	lower, upper = tarp.PrefixRange(allFF)
	if !bytes.Equal(lower, []byte{0xff, 0xff}) || upper != nil {
		t.Fatalf("PrefixRange(ffff) = (%x, %v), want (ffff, nil)", lower, upper)
	}
}

func TestScanRangeAndKeysRangeParityAcrossModes(t *testing.T) {
	for modeName, mode := range map[string]tarp.IndexMode{
		"memory": tarp.IndexMemory,
		"btree":  tarp.IndexBTree,
	} {
		t.Run(modeName, func(t *testing.T) {
			db := openPublicDB(t, t.TempDir(), tarp.Options{IndexMode: mode})
			var keys []string
			values := make(map[string]string)

			for i := range 350 {
				var k string
				if i%19 == 0 {
					// >240-byte overflow key in B+tree leaves and separators.
					k = fmt.Sprintf("item/%03d/%s", i, strings.Repeat("L", 320+(i%30)))
				} else {
					k = fmt.Sprintf("item/%03d", i)
				}
				v := fmt.Sprintf("val-%03d", i)
				if err := db.Put([]byte(k), []byte(v)); err != nil {
					t.Fatal(err)
				}
				if i%11 == 0 {
					if err := db.Delete([]byte(k)); err != nil {
						t.Fatal(err)
					}
					continue
				}
				keys = append(keys, k)
				values[k] = v
			}
			// Add boundary keys with 0x00 and 0xff bytes.
			for _, special := range []string{"\x00", "\x00\xff", "\xff", "\xff\xff", "\xff\xff\x01"} {
				v := "special:" + fmt.Sprintf("%x", special)
				if err := db.Put([]byte(special), []byte(v)); err != nil {
					t.Fatal(err)
				}
				keys = append(keys, special)
				values[special] = v
			}
			slices.Sort(keys)

			cases := []struct {
				name         string
				lower, upper []byte
			}{
				{"unbounded", nil, nil},
				{"lower_only", []byte("item/200"), nil},
				{"upper_only", nil, []byte("item/050")},
				{"interior", []byte("item/040"), []byte("item/180")},
				{"prefix_item_1", []byte("item/1"), []byte("item/2")},
				{"all_ff_prefix", []byte{0xff, 0xff}, nil},
				{"equal_bounds", []byte("item/100"), []byte("item/100")},
				{"inverted_bounds", []byte("item/200"), []byte("item/100")},
				{"before_all", nil, []byte{0x00}},
				{"after_all", []byte{0xff, 0xff, 0xff}, nil},
			}

			for _, tc := range cases {
				var wantAsc []string
				if len(tc.upper) == 0 || bytes.Compare(tc.lower, tc.upper) < 0 {
					for _, k := range keys {
						kb := []byte(k)
						if bytes.Compare(kb, tc.lower) >= 0 && (len(tc.upper) == 0 || bytes.Compare(kb, tc.upper) < 0) {
							wantAsc = append(wantAsc, k)
						}
					}
				}
				wantDesc := slices.Clone(wantAsc)
				slices.Reverse(wantDesc)

				assertRangeMatch(t, db, tc.name+"/asc", tc.lower, tc.upper, wantAsc, values)
				assertRangeMatch(t, db, tc.name+"/desc", tc.lower, tc.upper, wantDesc, values, tarp.WithReverse())
			}

			// Mutating caller bound slices after building the iterator must not affect results.
			mutLower := []byte("item/010")
			mutUpper := []byte("item/020")
			seq := db.KeysRange(t.Context(), mutLower, mutUpper)
			mutLower[0] = 0xff
			mutUpper[0] = 0x00
			var gotMut []string
			for k, err := range seq {
				if err != nil {
					t.Fatal(err)
				}
				gotMut = append(gotMut, string(k))
			}
			if len(gotMut) == 0 || gotMut[0] < "item/010" || gotMut[len(gotMut)-1] >= "item/020" {
				t.Fatalf("mutated caller bounds leaked into iterator: %v", gotMut)
			}

			// Context cancellation stops iteration with context.Canceled.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var seen int
			var gotErr error
			for _, err := range db.ScanRange(ctx, nil, nil, tarp.WithReverse()) {
				if err != nil {
					gotErr = err
					break
				}
				seen++
				if seen == 3 {
					cancel()
				}
			}
			if !errors.Is(gotErr, context.Canceled) {
				t.Fatalf("canceled ScanRange error = %v, want context.Canceled", gotErr)
			}
		})
	}
}

func assertRangeMatch(t *testing.T, db *tarp.DB, label string, lower, upper []byte, wantKeys []string, values map[string]string, opts ...tarp.ScanOption) {
	t.Helper()
	var gotScanKeys []string
	for entry, err := range db.ScanRange(t.Context(), lower, upper, opts...) {
		if err != nil {
			t.Fatalf("%s ScanRange: %v", label, err)
		}
		k := string(entry.Key)
		if string(entry.Value) != values[k] {
			t.Fatalf("%s ScanRange[%q] = %q, want %q", label, k, entry.Value, values[k])
		}
		// Mutate returned buffers to verify caller ownership.
		if len(entry.Key) > 0 {
			entry.Key[0] ^= 0xff
		}
		if len(entry.Value) > 0 {
			entry.Value[0] ^= 0xff
		}
		gotScanKeys = append(gotScanKeys, k)
	}
	if !slices.Equal(gotScanKeys, wantKeys) {
		t.Fatalf("%s ScanRange keys = %d, want %d", label, len(gotScanKeys), len(wantKeys))
	}

	var gotOnlyKeys []string
	for key, err := range db.KeysRange(t.Context(), lower, upper, opts...) {
		if err != nil {
			t.Fatalf("%s KeysRange: %v", label, err)
		}
		k := string(key)
		if len(key) > 0 {
			key[0] ^= 0xff
		}
		gotOnlyKeys = append(gotOnlyKeys, k)
	}
	if !slices.Equal(gotOnlyKeys, wantKeys) {
		t.Fatalf("%s KeysRange keys = %d, want %d", label, len(gotOnlyKeys), len(wantKeys))
	}
}

func TestTypedScanRangeWithOrderedCompositeCodec(t *testing.T) {
	type EventKey struct {
		Namespace string
		Timestamp time.Time
		Seq       int64
	}
	keyCodec, err := tarp.NewOrderedCodec[EventKey]()
	if err != nil {
		t.Fatal(err)
	}
	db, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, keyCodec, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []EventKey{
		{Namespace: "alpha", Timestamp: base.Add(-time.Hour), Seq: -2},
		{Namespace: "alpha", Timestamp: base, Seq: -1},
		{Namespace: "alpha", Timestamp: base, Seq: 0},
		{Namespace: "alpha", Timestamp: base.Add(time.Hour), Seq: 1},
		{Namespace: "alpha\x00sub", Timestamp: base, Seq: 1},
		{Namespace: "alphabet", Timestamp: base, Seq: 1},
		{Namespace: "beta", Timestamp: base, Seq: 1},
	}
	for i, ev := range events {
		if err := db.Put(ev, fmt.Sprintf("payload-%d", i)); err != nil {
			t.Fatal(err)
		}
	}

	// Tuple prefix for exact Namespace == "alpha" in reverse order.
	tuplePfx, err := tarp.EncodeOrderedTuplePrefix("alpha")
	if err != nil {
		t.Fatal(err)
	}
	lower, upper := tarp.PrefixRange(tuplePfx)
	var gotAlphaDesc []EventKey
	for ev, err := range db.KeysRange(t.Context(), lower, upper, tarp.WithReverse()) {
		if err != nil {
			t.Fatal(err)
		}
		gotAlphaDesc = append(gotAlphaDesc, ev)
	}
	wantAlphaDesc := []EventKey{events[3], events[2], events[1], events[0]}
	if !slices.Equal(gotAlphaDesc, wantAlphaDesc) {
		t.Fatalf("tuple prefix desc = %+v, want %+v", gotAlphaDesc, wantAlphaDesc)
	}

	// Leading string prefix for strings.HasPrefix(Namespace, "alpha") in ascending order.
	strLower, strUpper := tarp.PrefixRange(tarp.EncodeOrderedStringPrefix("alpha"))
	var gotStrPrefix []EventKey
	for entry, err := range db.ScanRange(t.Context(), strLower, strUpper) {
		if err != nil {
			t.Fatal(err)
		}
		gotStrPrefix = append(gotStrPrefix, entry.Key)
	}
	if !slices.Equal(gotStrPrefix, events[:6]) {
		t.Fatalf("string prefix asc = %+v, want %+v", gotStrPrefix, events[:6])
	}
}
