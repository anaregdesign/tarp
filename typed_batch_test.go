package tarp_test

import (
	"errors"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestTypedBatchesUseEncodedIdentityAndPreserveCodecErrors(t *testing.T) {
	keyErr, valueErr := errors.New("bad key"), errors.New("bad value")
	db, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, rejectingStringCodec{keyErr}, rejectingStringCodec{valueErr})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	type entry = tarp.TypedEntry[string, string]
	if err := db.BatchPut([]entry{{Key: "a", Value: "initial"}, {Key: "b", Value: ""}, {Key: "a", Value: "latest"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchPut([]entry{{Key: "a", Value: "updated"}, {Key: "b", Value: "rejected"}}); !errors.Is(err, valueErr) {
		t.Fatalf("encode batch value = %v", err)
	}
	if err := db.BatchDelete([]string{"a", "rejected"}); !errors.Is(err, keyErr) {
		t.Fatalf("encode delete key = %v", err)
	}
	before := db.Stats()
	if r, err := db.BatchGet([]string{"a", "a", "rejected"}); r != nil || !errors.Is(err, keyErr) {
		t.Fatalf("encode read key = %+v, %v", r, err)
	}
	if s := db.Stats(); s != before {
		t.Fatalf("key encode failure changed Stats from %+v to %+v", before, s)
	}
	r, err := db.BatchGet([]string{"b", "missing", "a", "a"})
	if err != nil || len(r) != 4 || !r[0].Found || r[0].Value != "" || r[1].Found || !r[2].Found || r[2].Value != "latest" || r[2] != r[3] {
		t.Fatalf("typed batch = %+v, %v", r, err)
	}
	if err := db.Put("b", "invalid bytes"); err != nil {
		t.Fatal(err)
	}
	before = db.Stats()
	if r, err := db.BatchGet([]string{"b", "b", "missing", "a"}); r != nil || !errors.Is(err, valueErr) {
		t.Fatalf("decode batch = %+v, %v", r, err)
	}
	want := before
	want.Cache.Hits += 2
	want.MissingKeys++
	want.BatchGetDuplicateInputs++
	if s := db.Stats(); s != want {
		t.Fatalf("value decode failure left Stats %+v, want %+v", s, want)
	}
	if err := db.BatchDelete([]string{"a", "a", "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get("a"); !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("Get after BatchDelete = %v", err)
	}
}

func TestTypedBatchGetClassifiesEncodedFloatKeys(t *testing.T) {
	db, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, tarp.Float64Codec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	nan, negativeZero := math.NaN(), math.Copysign(0, -1)
	if err := db.Put(nan, "NaN"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(0, "positive zero"); err != nil {
		t.Fatal(err)
	}
	// The two zeros encode differently. Both NaNs encode identically.
	results, err := db.BatchGet([]float64{nan, 0, negativeZero, nan, negativeZero})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 5 || !results[0].Found || results[0].Value != "NaN" ||
		!results[1].Found || results[1].Value != "positive zero" || results[2].Found ||
		results[3] != results[0] || results[4].Found {
		t.Fatalf("unexpected results: %+v", results)
	}
	s := db.Stats()
	if s.Cache.Hits != 2 || s.Cache.Misses != 0 || s.MissingKeys != 1 || s.BatchGetDuplicateInputs != 2 {
		t.Fatalf("unexpected counts for three distinct encoded keys: %+v", s)
	}
}

func TestTypedBatchBounds(t *testing.T) {
	opts := tarp.Options{MaxRecordBytes: 60, TargetSegmentBytes: 165}
	db, err := tarp.OpenTyped(t.TempDir(), opts, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	type entry = tarp.TypedEntry[string, string]
	if err := db.BatchPut([]entry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchPut([]entry{{Key: "a", Value: "X"}, {Key: "b", Value: "Y"}, {Key: "c", Value: "Z"}}); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("oversized batch = %v", err)
	}
	if got, err := db.Get("a"); got != "1" || err != nil {
		t.Fatalf("oversized batch changed a = %q, %v", got, err)
	}
	keys := make([]string, tarp.MaxBatchEntries+1)
	if err := db.BatchDelete(keys); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("oversized delete = %v", err)
	}
	if _, err := db.BatchGet(keys); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("oversized read = %v", err)
	}
}

func ExampleTypedDB_BatchGet() {
	dir, err := os.MkdirTemp("", "tarp-batch-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	db, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	keys := []string{"state", "result"}
	err = db.BatchPut([]tarp.TypedEntry[string, string]{
		{Key: "state", Value: "done"},
		{Key: "result", Value: "report.pdf"},
	})
	if err != nil {
		panic(err)
	}
	results, err := db.BatchGet(keys)
	if err != nil {
		panic(err)
	}
	for i, result := range results {
		fmt.Println(keys[i], result.Found, result.Value)
	}
	if err := db.BatchDelete(keys); err != nil {
		panic(err)
	}
	// Output:
	// state true done
	// result true report.pdf
}
