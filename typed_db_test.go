package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestTypedDBSharesRawRecordsAndLifecycle(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	raw, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	db, err := tarp.NewTypedDB(raw, tarp.BytesCodec{}, tarp.Uint16Codec{})
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Get(key); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("raw Get after typed Close = %v", err)
	}
	if err := db.Backup(t.Context(), filepath.Join(t.TempDir(), "backup")); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("Backup after typed Close = %v", err)
	}
	if got, want := db.MaintenanceStatus(), raw.MaintenanceStatus(); got != want {
		t.Fatalf("maintenance status after Close = %+v, want %+v", got, want)
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
	db, err := tarp.NewTypedDB(raw, rejectingStringCodec{keyErr}, rejectingStringCodec{valueErr})
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
	db, err := tarp.NewTypedDB(raw, rejectingStringCodec{keyErr}, rejectingStringCodec{valueErr})
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
	if result, err := db.Compact(t.Context()); err != nil || result.PinnedBytes != 0 {
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
	if db, err := tarp.NewTypedDB(nil, tarp.StringCodec{}, tarp.StringCodec{}); db != nil || err == nil {
		t.Fatalf("NewTypedDB with nil core = %v, %v", db, err)
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
	if db, err := tarp.NewTypedDB(raw, tarp.StringCodec{}, (*tarp.StringCodec)(nil)); db != nil || err == nil {
		t.Fatalf("NewTypedDB with nil value codec = %v, %v", db, err)
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
