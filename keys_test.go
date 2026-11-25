package tarp_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestTypedKeysDecodeOnlyKeysAndStopOnError(t *testing.T) {
	raw := openPublicDB(t, t.TempDir(), tarp.Options{})
	badKey := errors.New("bad encoded key")
	view, err := tarp.Bind(raw, rejectingStringCodec{badKey}, tarp.Uint64Codec{})
	if err != nil {
		t.Fatal(err)
	}
	seq := view.Keys(t.Context())
	if err := raw.Put([]byte("good"), []byte{0}); err != nil {
		t.Fatal(err)
	}
	count := 0
	for key, err := range seq {
		count++
		if err != nil || key != "good" {
			t.Fatalf("Keys with undecodable value = %q, %v", key, err)
		}
	}
	if count != 1 {
		t.Fatalf("yielded %d keys, want 1", count)
	}
	if err := raw.Delete([]byte("good")); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"invalid1", "invalid2"} {
		if err := raw.Put([]byte(key), nil); err != nil {
			t.Fatal(err)
		}
	}
	count = 0
	for key, err := range seq {
		count++
		if !errors.Is(err, badKey) || key != "" {
			t.Fatalf("Keys with undecodable key = %q, %v", key, err)
		}
	}
	if count != 1 {
		t.Fatalf("yielded %d errors, want 1", count)
	}
}

func TestTypedKeysKeepsItsDatabaseAndCodecAfterViewReplacement(t *testing.T) {
	view, replacement := rebindableIteratorViews(t)
	seq := view.Keys(t.Context())
	for attempt := range 2 {
		var keys []string
		for key, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, key)
			view = replacement
		}
		slices.Sort(keys)
		if !slices.Equal(keys, []string{"invalid1", "invalid2"}) {
			t.Fatalf("Keys run %d returned %v", attempt, keys)
		}
	}
}

func TestIterationReportsCloseAfterLastKey(t *testing.T) {
	for modeName, mode := range map[string]tarp.IndexMode{"memory": tarp.IndexMemory, "btree": tarp.IndexBTree} {
		for _, method := range []string{"Keys", "Scan"} {
			t.Run(modeName+"/"+method, func(t *testing.T) {
				db := openPublicDB(t, t.TempDir(), tarp.Options{IndexMode: mode})
				if err := db.Put([]byte("only"), []byte("value")); err != nil {
					t.Fatal(err)
				}
				if method == "Keys" {
					checkCloseAfterLastRow(t, db, db.Keys(t.Context()))
				} else {
					checkCloseAfterLastRow(t, db, db.Scan(t.Context()))
				}
			})
		}
	}
}

func checkCloseAfterLastRow[T any](t *testing.T, db *tarp.DB, seq iter.Seq2[T, error]) {
	t.Helper()
	next, stop := iter.Pull2(seq)
	defer stop()
	if _, err, ok := next(); !ok || err != nil {
		t.Fatalf("first row: error %v, ok %t", err, ok)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err, ok := next(); !ok || !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("after Close: error %v, ok %t", err, ok)
	}
	if _, _, ok := next(); ok {
		t.Fatal("iterator yielded after ErrClosed")
	}
}

func ExampleDB_Keys() {
	dir, err := os.MkdirTemp("", "tarp-keys-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	if err := db.BatchPut([]tarp.Entry{
		{Key: []byte("pear"), Value: []byte("green")},
		{Key: []byte("apple"), Value: []byte("red")},
	}); err != nil {
		panic(err)
	}
	var keys []string
	for key, err := range db.Keys(context.Background()) {
		if err != nil {
			panic(err)
		}
		keys = append(keys, string(key))
	}
	slices.Sort(keys)
	fmt.Println(keys)
	// Output: [apple pear]
}

func TestKeysPrefixMatchesBinaryKeysAndOwnsPrefix(t *testing.T) {
	keys := []string{"\x00", "\x00\xff", "\x00\xff\x00", "\x01", "\xfe", "\xff", "\xff\x00", "\xff\xff", "\xff\xffx"}
	for name, mode := range map[string]tarp.IndexMode{"btree": tarp.IndexBTree, "memory": tarp.IndexMemory} {
		t.Run(name, func(t *testing.T) {
			db := openPublicDB(t, t.TempDir(), tarp.Options{IndexMode: mode})
			for _, key := range keys {
				if err := db.Put([]byte(key), nil); err != nil {
					t.Fatal(err)
				}
			}
			for _, tc := range []struct {
				prefix []byte
				want   []string
			}{
				{nil, keys},
				{[]byte{}, keys},
				{[]byte{0x00, 0xff}, []string{"\x00\xff", "\x00\xff\x00"}},
				{[]byte{0x01, 0xff}, nil},
				{[]byte{0xff}, []string{"\xff", "\xff\x00", "\xff\xff", "\xff\xffx"}},
				{[]byte{0xff, 0xff}, []string{"\xff\xff", "\xff\xffx"}},
			} {
				prefix := slices.Clone(tc.prefix)
				seq := db.KeysPrefix(t.Context(), prefix)
				clear(prefix)
				var got []string
				for key, err := range seq {
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, string(key))
					clear(key)
				}
				slices.Sort(got)
				if !slices.Equal(got, tc.want) {
					t.Errorf("prefix %x yielded %x, want %x", tc.prefix, got, tc.want)
				}
			}
		})
	}
}
