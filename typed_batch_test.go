package tarp_test

import (
	"errors"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/anaregdesign/tarp"
)

func requireBatchCodecError(t *testing.T, err, cause error, op string, index int, field string) {
	t.Helper()
	var e *tarp.BatchCodecError
	if !errors.Is(err, cause) || !errors.As(err, &e) || e.Operation != op || e.Index != index || e.Field != field {
		t.Fatalf("got %v; want %s input[%d] %s with cause %v", err, op, index, field, cause)
	}
}

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
	err = db.BatchPut([]entry{{Key: "a", Value: "updated"}, {Key: "b", Value: "rejected"}})
	requireBatchCodecError(t, err, valueErr, "BatchPut", 1, "value")
	err = db.BatchPut([]entry{{Key: "a", Value: "updated"}, {Key: "rejected", Value: "ok"}})
	requireBatchCodecError(t, err, keyErr, "BatchPut", 1, "key")
	err = db.BatchDelete([]string{"a", "a", "rejected"})
	requireBatchCodecError(t, err, keyErr, "BatchDelete", 2, "key")
	before := db.Stats()
	r, err := db.BatchGet([]string{"a", "a", "rejected"})
	requireBatchCodecError(t, err, keyErr, "BatchGet", 2, "key")
	if r != nil {
		t.Fatalf("partial results after key encode failure: %+v", r)
	}
	if s := db.Stats(); s != before {
		t.Fatalf("key encode failure changed Stats from %+v to %+v", before, s)
	}
	r, err = db.BatchGet([]string{"b", "missing", "a", "a"})
	if err != nil || len(r) != 4 || !r[0].Found || r[0].Value != "" || r[1].Found || !r[2].Found || r[2].Value != "latest" || r[2] != r[3] {
		t.Fatalf("typed batch = %+v, %v", r, err)
	}
	if err := db.Put("b", "invalid bytes"); err != nil {
		t.Fatal(err)
	}
	before = db.Stats()
	r, err = db.BatchGet([]string{"a", "missing", "b", "b"})
	requireBatchCodecError(t, err, valueErr, "BatchGet", 2, "value")
	if r != nil {
		t.Fatalf("partial results after value decode failure: %+v", r)
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
	codec := rejectingStringCodec{errors.New("unexpected codec call after size limit")}
	db, err := tarp.OpenTyped(t.TempDir(), opts, codec, codec)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	type entry = tarp.TypedEntry[string, string]
	if err := db.BatchPut([]entry{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchPut([]entry{{Key: "a", Value: "X"}, {Key: "b", Value: "Y"}, {Key: "c", Value: "Z"}, {Key: "rejected", Value: "rejected"}}); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("oversized batch = %v", err)
	}
	if got, err := db.Get("a"); got != "1" || err != nil {
		t.Fatalf("oversized batch changed a = %q, %v", got, err)
	}
	if err := db.BatchDelete([]string{"a", "b", "c", "rejected"}); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("expected size rejection before encoding the last delete: %v", err)
	}
	entries := make([]entry, tarp.MaxBatchEntries+1)
	entries[0].Key = "rejected"
	if err := db.BatchPut(entries); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("oversized input count = %v", err)
	}
	keys := make([]string, tarp.MaxBatchEntries+1)
	keys[0] = "rejected"
	if err := db.BatchDelete(keys); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("oversized delete = %v", err)
	}
	if _, err := db.BatchGet(keys); !errors.Is(err, tarp.ErrTooLarge) {
		t.Fatalf("oversized read = %v", err)
	}
}

type closeOwnerCodec struct {
	tarp.StringCodec
	owner *tarp.DB
}

func (c closeOwnerCodec) Encode(key string) ([]byte, error) {
	if err := c.owner.Close(); err != nil {
		return nil, err
	}
	return c.StringCodec.Encode(key)
}

func TestTypedBatchCodecCanCloseOwnerBeforeCommit(t *testing.T) {
	for _, op := range []string{"put", "delete"} {
		t.Run(op, func(t *testing.T) {
			dir := t.TempDir()
			owner := openPublicDB(t, dir, tarp.Options{})
			if err := owner.Put([]byte("k"), []byte("old")); err != nil {
				t.Fatal(err)
			}
			view, err := tarp.Bind(owner, closeOwnerCodec{owner: owner}, tarp.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			switch op {
			case "put":
				err = view.BatchPut([]tarp.TypedEntry[string, string]{{Key: "k", Value: "new"}})
			case "delete":
				err = view.BatchDelete([]string{"k"})
			}
			if !errors.Is(err, tarp.ErrClosed) {
				t.Fatalf("batch after owner closed by codec: %v", err)
			}
			reopened := openPublicDB(t, dir, tarp.Options{})
			if got, err := reopened.Get([]byte("k")); string(got) != "old" || err != nil {
				t.Fatalf("stored value after failed commit: %q, %v", got, err)
			}
		})
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
