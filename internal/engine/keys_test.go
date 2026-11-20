package engine

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestKeysSnapshotUsesNoDataReadsOrFilePins(t *testing.T) {
	var reads atomic.Int64
	db := newTestDBWithHooks(t, testOptions(), &storage.IO{Hook: func(point string) error {
		if point == "read:before" {
			reads.Add(1)
		}
		return nil
	}}, &Hooks{CleanerStart: make(chan struct{})})
	keys := db.Keys(t.Context())
	for key, err := range keys {
		t.Fatalf("empty database yielded %q, %v", key, err)
	}
	for _, key := range []string{"a", "b", "\x00\xff"} {
		mustPut(t, db, key, "value")
	}
	db.cache.Clear()
	before := db.Stats()
	pins := make(map[*storage.File]int64)
	for _, f := range db.files {
		pins[f] = f.References()
	}

	seen := make(map[string]bool)
	for key, err := range keys {
		if err != nil || seen[string(key)] {
			t.Fatalf("snapshot yielded %q twice or returned %v", key, err)
		}
		seen[string(key)] = true
		key[0] ^= 0xff
		if len(seen) == 1 {
			for file, count := range pins {
				if got := file.References(); got != count {
					t.Fatalf("file %d references = %d, want %d", file.ID, got, count)
				}
			}
			if db.active != 0 || db.Stats() != before {
				t.Fatal("paused key iterator retained admission or changed stats")
			}
			if err := db.Delete([]byte("a")); err != nil {
				t.Fatal(err)
			}
			mustPut(t, db, "new", "later")
			if result, err := db.Compact(t.Context()); err != nil || result.PinnedBytes != 0 {
				t.Fatalf("compact during Keys = %+v, %v", result, err)
			}
		}
	}
	if want := map[string]bool{"a": true, "b": true, "\x00\xff": true}; !maps.Equal(seen, want) {
		t.Fatalf("snapshot = %q, want %q", slices.Sorted(maps.Keys(seen)), slices.Sorted(maps.Keys(want)))
	}
	var fresh []string
	for key, err := range keys {
		if err != nil {
			t.Fatal(err)
		}
		fresh = append(fresh, string(key))
	}
	slices.Sort(fresh)
	if !slices.Equal(fresh, []string{"\x00\xff", "b", "new"}) || reads.Load() != 0 {
		t.Fatalf("fresh keys = %q, DATA reads = %d", fresh, reads.Load())
	}
}

func TestKeysStopsOnCancelCloseOrBreak(t *testing.T) {
	for _, action := range []string{"cancel", "closed", "break", "nil context", "already canceled"} {
		t.Run(action, func(t *testing.T) {
			db := newTestDB(t, testOptions(), nil)
			mustPut(t, db, "a", "A")
			mustPut(t, db, "b", "B")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			seq := db.Keys(ctx)
			if action == "nil context" {
				seq = db.Keys(nil)
			}
			if action == "already canceled" {
				cancel()
			}
			next, stop := iter.Pull2(seq)
			defer stop()
			if action != "nil context" && action != "already canceled" {
				if key, err, ok := next(); !ok || err != nil || len(key) == 0 {
					t.Fatalf("first key = %q, %v, %v", key, err, ok)
				}
			}
			var want error = context.Canceled
			switch action {
			case "cancel":
				cancel()
			case "closed":
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				want = ErrClosed
			case "break":
				stop()
			}
			if action != "break" {
				key, err, ok := next()
				if !ok || key != nil || err == nil {
					t.Fatalf("error item = %q, %v, %v", key, err, ok)
				}
				if action == "nil context" {
					var op *OpError
					if !errors.As(err, &op) || op.Operation != "keys" || op.Outcome != NotApplied {
						t.Fatalf("unexpected nil-context error: %v", err)
					}
				} else if !errors.Is(err, want) {
					t.Fatalf("got %v, want %v", err, want)
				}
			}
			if _, _, ok := next(); ok || db.active != 0 {
				t.Fatalf("Keys continued or retained admission after %s", action)
			}
		})
	}
}
