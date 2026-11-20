package tarp_test

import (
	"context"
	"errors"
	"fmt"
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
