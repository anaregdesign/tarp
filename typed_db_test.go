package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestBoundViewSharesRecordsAndOwnerControlsLifetime(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	raw, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db, err := tarp.Bind(raw, tarp.BytesCodec{}, tarp.Uint16Codec{})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte{0x00, 0xff}
	if err := db.Put(key, 0x1234); err != nil {
		t.Fatal(err)
	}
	if got, err := raw.Get(key); err != nil || !bytes.Equal(got, []byte{0x12, 0x34}) {
		t.Fatalf("raw Get = %x, %v", got, err)
	}
	if err := raw.Put(key, []byte{0xab, 0xcd}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get(key); err != nil || got != 0xabcd {
		t.Fatalf("typed Get = %x, %v", got, err)
	}
	if err := db.Put(nil, 1); !errors.Is(err, tarp.ErrEmptyKey) {
		t.Fatalf("Put empty key = %v", err)
	}
	for _, method := range []string{"Close", "Backup", "Compact", "Stats", "AutoCompactionStatus"} {
		if _, present := reflect.TypeOf(db).MethodByName(method); present {
			t.Fatalf("bound view exposes owner method %s", method)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(key); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("view Get after owner Close = %v", err)
	}

	reopened, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.BytesCodec{}, tarp.Uint16Codec{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get(key); err != nil || got != 0xabcd {
		t.Fatalf("Get after reopen = %x, %v", got, err)
	}
	if err := reopened.Delete(key); err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Get(key); got != 0 || !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("Get after Delete = %x, %v", got, err)
	}
	if err := reopened.Delete(key); !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("Delete missing key = %v", err)
	}
}

func TestReplacingTypedViewKeepsOwnerOnOriginalDatabase(t *testing.T) {
	owner, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	borrowed := owner.View()
	original := owner.View()
	if err := borrowed.Put("k", "original"); err != nil {
		t.Fatal(err)
	}
	other := openPublicDB(t, t.TempDir(), tarp.Options{})
	replacement, err := tarp.Bind(other, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	*borrowed = *replacement
	if err := borrowed.Put("k", "other"); err != nil {
		t.Fatal(err)
	}
	if got, err := owner.Get("k"); got != "original" || err != nil {
		t.Fatalf("owner after replacing its borrowed view = %q, %v", got, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := original.Get("k"); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("original view after owner closed = %v", err)
	}
	if got, err := borrowed.Get("k"); got != "other" || err != nil {
		t.Fatalf("replacement after owner closed = %q, %v", got, err)
	}
}

// Return partial results on failure to check that TypedDB discards them.
type rejectingStringCodec struct{ err error }

func (c rejectingStringCodec) Encode(s string) ([]byte, error) {
	if s == "rejected" {
		return []byte("key"), c.err
	}
	return []byte(s), nil
}

func (c rejectingStringCodec) Decode(data []byte) (string, error) {
	if strings.HasPrefix(string(data), "invalid") {
		return "partial", c.err
	}
	return string(data), nil
}

func TestTypedDBPreservesCodecErrorsAndPriorValue(t *testing.T) {
	raw, err := tarp.Open(t.TempDir(), tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	keyErr, valueErr := errors.New("key codec error"), errors.New("value codec error")
	db, err := tarp.Bind(raw, rejectingStringCodec{keyErr}, rejectingStringCodec{valueErr})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put("key", "initial"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("rejected", "replacement"); !errors.Is(err, keyErr) {
		t.Fatalf("Put with key encode error = %v", err)
	}
	if err := db.Put("key", "rejected"); !errors.Is(err, valueErr) {
		t.Fatalf("Put with value encode error = %v", err)
	}
	if err := db.Delete("rejected"); !errors.Is(err, keyErr) {
		t.Fatalf("Delete with key encode error = %v", err)
	}
	if got, err := db.Get("rejected"); got != "" || !errors.Is(err, keyErr) {
		t.Fatalf("Get with key encode error = %q, %v", got, err)
	}
	if got, err := db.Get("key"); got != "initial" || err != nil {
		t.Fatalf("Get after failed mutations = %q, %v", got, err)
	}
	if err := raw.Put([]byte("key"), []byte("invalid")); err != nil {
		t.Fatal(err)
	}
	if got, err := db.Get("key"); got != "" || !errors.Is(err, valueErr) {
		t.Fatalf("Get with value decode error = %q, %v", got, err)
	}
}

func TestTypedDBScanStopsOnCodecErrorOrBreak(t *testing.T) {
	keyErr, valueErr := errors.New("bad key"), errors.New("bad value")
	raw, err := tarp.Open(t.TempDir(), tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db, err := tarp.Bind(raw, rejectingStringCodec{keyErr}, rejectingStringCodec{valueErr})
	if err != nil {
		t.Fatal(err)
	}
	seq := db.Scan(t.Context())
	for _, tc := range []struct {
		key, value string
		err        error
	}{
		{"invalid", "value", keyErr},
		{"key", "invalid", valueErr},
	} {
		for _, suffix := range []string{"1", "2"} {
			if err := raw.Put([]byte(tc.key+suffix), []byte(tc.value)); err != nil {
				t.Fatal(err)
			}
		}
		count := 0
		for entry, err := range seq {
			count++
			if !errors.Is(err, tc.err) || entry != (tarp.TypedEntry[string, string]{}) {
				t.Fatalf("Scan invalid entry = %+v, %v", entry, err)
			}
		}
		if count != 1 {
			t.Fatalf("Scan yielded %d errors, want 1", count)
		}
		for _, suffix := range []string{"1", "2"} {
			if err := raw.Delete([]byte(tc.key + suffix)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, key := range []string{"one", "two"} {
		if err := db.Put(key, key+"!"); err != nil {
			t.Fatal(err)
		}
	}
	read := 0
	for entry, err := range seq {
		if err != nil || entry.Value != entry.Key+"!" {
			t.Fatalf("Scan = %+v, %v", entry, err)
		}
		read++
		break
	}
	if read != 1 {
		t.Fatal("Scan yielded no entry")
	}
	if result, err := raw.Compact(t.Context()); err != nil || result.PinnedRetiredDataBytes != 0 {
		t.Fatalf("Compact after break = %+v, %v", result, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	read = 0
	for entry, err := range db.Scan(ctx) {
		read++
		if !errors.Is(err, context.Canceled) || entry != (tarp.TypedEntry[string, string]{}) {
			t.Fatalf("canceled Scan = %+v, %v", entry, err)
		}
	}
	if read != 1 {
		t.Fatalf("canceled Scan yielded %d entries, want 1", read)
	}
}

func TestTypedDBRequiresCoreAndCodecs(t *testing.T) {
	if db, err := tarp.Bind(nil, tarp.StringCodec{}, tarp.StringCodec{}); db != nil || err == nil {
		t.Fatalf("Bind with nil core = %v, %v", db, err)
	}
	dir := filepath.Join(t.TempDir(), "db")
	for _, tc := range []struct{ key, value tarp.Codec[string] }{
		{nil, tarp.StringCodec{}},
		{tarp.StringCodec{}, nil},
		{(*tarp.StringCodec)(nil), tarp.StringCodec{}},
		{tarp.StringCodec{}, (*tarp.StringCodec)(nil)},
	} {
		if db, err := tarp.OpenTyped(dir, tarp.Options{}, tc.key, tc.value); db != nil || err == nil {
			t.Fatalf("OpenTyped with nil codec = %v, %v", db, err)
		}
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid OpenTyped created a database: stat %q = %v", dir, err)
	}
	raw, err := tarp.Open(t.TempDir(), tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if db, err := tarp.Bind(raw, tarp.StringCodec{}, (*tarp.StringCodec)(nil)); db != nil || err == nil {
		t.Fatalf("Bind with nil value codec = %v, %v", db, err)
	}
}

func TestOpenTypedPreservesOpenErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts tarp.Options
		want error
	}{
		{"directory owned", tarp.Options{}, tarp.ErrLocked},
		{"invalid options", tarp.Options{ReadFileHandles: -1}, tarp.ErrInvalidOptions},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			owner := openPublicDB(t, dir, tarp.Options{})
			db, err := tarp.OpenTyped(dir, tc.opts, tarp.StringCodec{}, tarp.BytesCodec{})
			if db != nil || !errors.Is(err, tc.want) {
				t.Fatalf("OpenTyped = %v, %v; want nil, %v", db, err, tc.want)
			}
			if err := owner.Put([]byte("k"), nil); err != nil {
				t.Fatalf("owner Put after failed OpenTyped: %v", err)
			}
		})
	}
}

func Example_typedDB() {
	if err := storeScore(); err != nil {
		panic(err)
	}
	// Output: 42
}

func storeScore() (err error) {
	dir, err := os.MkdirTemp("", "tarp-scores-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()

	db, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.StringCodec{}, tarp.Int64Codec{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	if err := db.Put("alice", 42); err != nil {
		return err
	}
	score, err := db.Get("alice")
	if err != nil {
		return err
	}
	fmt.Println(score)
	return nil
}

func TestTypedCheckpointIndexReopensEncodedKeys(t *testing.T) {
	dir := t.TempDir()
	db, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.BytesCodec{}, tarp.Uint16Codec{})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte{0, 255, 1}
	if err := db.Put(key, 4096); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointIndex(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = tarp.OpenTyped(dir, tarp.Options{OpenValidation: tarp.Indexed}, tarp.BytesCodec{}, tarp.Uint16Codec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got, err := db.Get(key); err != nil || got != 4096 {
		t.Fatalf("Get = %v, %v", got, err)
	}
}

func TestTypedPrefixDecodesOnlyMatchingKeysAndValues(t *testing.T) {
	type edgeKey struct{ Tail, Head string }
	type tailKey struct{ Tail string }
	keys, err := tarp.NewCodec[edgeKey]()
	if err != nil {
		t.Fatal(err)
	}
	tails, err := tarp.NewCodec[tailKey]()
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := tails.Encode(tailKey{"a"})
	if err != nil {
		t.Fatal(err)
	}
	for name, mode := range map[string]tarp.IndexMode{"btree": tarp.IndexBTree, "memory": tarp.IndexMemory} {
		t.Run(name, func(t *testing.T) {
			raw := openPublicDB(t, t.TempDir(), tarp.Options{IndexMode: mode})
			valueErr := errors.New("invalid edge value")
			edges, err := tarp.Bind(raw, keys, rejectingStringCodec{valueErr})
			if err != nil {
				t.Fatal(err)
			}
			match := edgeKey{"a", "z"}
			if err := edges.Put(match, "valid"); err != nil {
				t.Fatal(err)
			}
			if err := edges.Put(edgeKey{"ab", "z"}, "invalid"); err != nil {
				t.Fatal(err)
			}
			if err := raw.Put([]byte{0xff}, nil); err != nil {
				t.Fatal(err)
			}
			count := 0
			for entry, err := range edges.ScanPrefix(t.Context(), prefix) {
				if err != nil || entry.Key != match || entry.Value != "valid" {
					t.Fatalf("ScanPrefix = %+v, %v", entry, err)
				}
				count++
			}
			if count != 1 {
				t.Fatalf("ScanPrefix yielded %d entries", count)
			}
			if err := edges.Put(match, "invalid"); err != nil {
				t.Fatal(err)
			}
			count = 0
			for key, err := range edges.KeysPrefix(t.Context(), prefix) {
				if err != nil || key != match {
					t.Fatalf("KeysPrefix = %+v, %v", key, err)
				}
				count++
			}
			if count != 1 {
				t.Fatalf("KeysPrefix yielded %d keys", count)
			}
			count = 0
			for entry, err := range edges.ScanPrefix(t.Context(), prefix) {
				if !errors.Is(err, valueErr) || entry != (tarp.TypedEntry[edgeKey, string]{}) {
					t.Fatalf("ScanPrefix with invalid value = %+v, %v", entry, err)
				}
				count++
			}
			if count != 1 {
				t.Fatalf("ScanPrefix yielded %d codec errors", count)
			}
		})
	}
}
