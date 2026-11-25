package tarp_test

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

type trimmedKeyCodec struct {
	tarp.StringCodec
	calls int
}

func (c *trimmedKeyCodec) Encode(key string) ([]byte, error) {
	c.calls++
	return c.StringCodec.Encode(strings.TrimSpace(key))
}

func TestTypedBatchKeyErrorsUseOriginalInputsAndEncodeOnce(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	if err := db.Put([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	codec := &trimmedKeyCodec{}
	view, err := tarp.Bind(db, codec, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"a", " a", " ", "later"}
	entries := []tarp.TypedEntry[string, string]{{Key: keys[0]}, {Key: keys[1]}, {Key: keys[2]}, {Key: keys[3]}}
	before := db.Stats()
	for _, tc := range []struct {
		op    string
		call  func() error
		cause error
		count int
	}{
		{"BatchGet", func() error { _, err := view.BatchGet(keys); return err }, tarp.ErrEmptyKey, 3},
		{"BatchPut", func() error { return view.BatchPut(entries) }, tarp.ErrEmptyKey, 3},
		{"BatchDelete", func() error { return view.BatchDelete(keys) }, tarp.ErrEmptyKey, 3},
		{"BatchDelete", func() error { return view.BatchDelete([]string{"a", " a", "missing", " missing", "other"}) }, tarp.ErrNotFound, 5},
	} {
		codec.calls = 0
		requireBatchKeyError(t, tc.call(), tc.op, 2, tc.cause)
		if codec.calls != tc.count {
			t.Fatalf("%s encoded %d inputs, want %d", tc.op, codec.calls, tc.count)
		}
	}
	if after := db.Stats(); after != before {
		t.Fatalf("rejected typed batch changed Stats: before %+v, after %+v", before, after)
	}
}

func TestTypedBatchesStopAtFirstEmptyEncodedKey(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	badCodec := rejectingStringCodec{errors.New("unexpected codec call")}
	view, err := tarp.Bind(db, badCodec, badCodec)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"kept", "", "rejected"}
	entries := []tarp.TypedEntry[string, string]{
		{Key: keys[0], Value: "new"}, {Key: keys[1], Value: "rejected"}, {Key: keys[2]},
	}
	if err := db.Put([]byte("kept"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()
	results, err := view.BatchGet(keys)
	requireBatchKeyError(t, err, "BatchGet", 1, tarp.ErrEmptyKey)
	if results != nil {
		t.Fatalf("partial read results: %+v", results)
	}
	requireBatchKeyError(t, view.BatchPut(entries), "BatchPut", 1, tarp.ErrEmptyKey)
	requireBatchKeyError(t, view.BatchDelete(keys), "BatchDelete", 1, tarp.ErrEmptyKey)
	if after := db.Stats(); after != before {
		t.Fatalf("rejected batches changed Stats from %+v to %+v", before, after)
	}
	if got, err := db.Get([]byte("kept")); string(got) != "old" || err != nil {
		t.Fatalf("value after rejected batches: %q, %v", got, err)
	}
}

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
	want.ValueCache.Hits += 2
	want.MissingKeyLookups++
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
	if s.ValueCache.Hits != 2 || s.ValueCache.Misses != 0 || s.MissingKeyLookups != 1 || s.BatchGetDuplicateInputs != 2 {
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

func TestTypedBatchGetChargesEncodedBytesBeforeDecoding(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{MaxBatchGetValueBytes: 7})
	if err := db.Put([]byte("key"), []byte("invalid")); err != nil {
		t.Fatal(err)
	}
	decodeErr := errors.New("malformed value")
	view, err := tarp.Bind(db, tarp.StringCodec{}, rejectingStringCodec{decodeErr})
	if err != nil {
		t.Fatal(err)
	}
	got, err := view.BatchGet([]string{"key", "absent", "key"})
	var limit *tarp.BatchLimitError
	if got != nil || !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &limit) ||
		limit.Kind != tarp.BatchGetValueBytesLimit || limit.Index != 2 || limit.Actual != 14 || limit.Limit != 7 {
		t.Fatalf("oversized typed result: %+v, %v", got, err)
	}
	_, err = view.BatchGet([]string{"key"})
	requireBatchCodecError(t, err, decodeErr, "BatchGet", 0, "value")
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

func TestTypedBatchCodecCanCloseOwnerBeforeAdmission(t *testing.T) {
	for _, op := range []string{"get", "put", "delete"} {
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
			case "get":
				_, err = view.BatchGet([]string{"k"})
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

type failingIntCodec struct {
	failOn int
	err    error
}

func (c failingIntCodec) Encode(v int) ([]byte, error) {
	if v == c.failOn {
		return nil, c.err
	}
	return tarp.OrderedIntCodec{}.Encode(v)
}

func (c failingIntCodec) Decode(b []byte) (int, error) {
	return tarp.OrderedIntCodec{}.Decode(b)
}

func TestTypedWriteBatchApplyAndCodecErrors(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	keyBoom := errors.New("key encode boom")
	valBoom := errors.New("val encode boom")

	view, err := tarp.Bind(db, failingIntCodec{failOn: 99, err: keyBoom}, failingIntCodec{failOn: -99, err: valBoom})
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Put(1, 10); err != nil {
		t.Fatal(err)
	}
	if err := view.Put(2, 20); err != nil {
		t.Fatal(err)
	}
	before := db.Stats()

	// Key codec failure reports Operation="Apply", Field="key", exact Index.
	var badKey tarp.TypedWriteBatch[int, int]
	badKey.Put(1, 100)
	badKey.Delete(99)
	requireBatchCodecError(t, view.Apply(&badKey), keyBoom, "Apply", 1, "key")

	// Value codec failure reports Operation="Apply", Field="value", exact Index.
	var badVal tarp.TypedWriteBatch[int, int]
	badVal.Delete(1)
	badVal.Put(3, -99)
	requireBatchCodecError(t, view.Apply(&badVal), valBoom, "Apply", 1, "value")

	if after := db.Stats(); after != before {
		t.Fatalf("codec errors mutated DB stats: before=%+v after=%+v", before, after)
	}

	// Valid mixed TypedWriteBatch commits atomically and treats absent Delete as idempotent.
	var good tarp.TypedWriteBatch[int, int]
	if good.Len() != 0 {
		t.Fatalf("zero TypedWriteBatch Len = %d", good.Len())
	}
	good.Put(1, 111)
	good.Delete(2)
	good.Delete(404) // absent
	good.Put(3, 333)
	good.Put(4, 444)
	good.Delete(4) // Put -> Delete on absent key
	if good.Len() != 6 {
		t.Fatalf("TypedWriteBatch Len = %d, want 6", good.Len())
	}
	if err := view.Apply(&good); err != nil {
		t.Fatalf("view.Apply: %v", err)
	}
	good.Reset()
	if good.Len() != 0 {
		t.Fatalf("TypedWriteBatch Len after Reset = %d", good.Len())
	}
	if err := view.Apply(nil); err != nil {
		t.Fatalf("view.Apply(nil): %v", err)
	}

	if got, err := view.Get(1); err != nil || got != 111 {
		t.Fatalf("Get(1) = %d, %v; want 111", got, err)
	}
	if got, err := view.Get(3); err != nil || got != 333 {
		t.Fatalf("Get(3) = %d, %v; want 333", got, err)
	}
	for _, missing := range []int{2, 4, 404} {
		if _, err := view.Get(missing); !errors.Is(err, tarp.ErrNotFound) {
			t.Fatalf("Get(%d) err = %v, want ErrNotFound", missing, err)
		}
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
