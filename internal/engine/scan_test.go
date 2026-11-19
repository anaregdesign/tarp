package engine

import (
	"context"
	"errors"
	"iter"
	"maps"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func scanContents(t *testing.T, seq iter.Seq2[Entry, error]) map[string]string {
	t.Helper()
	got := make(map[string]string)
	for e, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		key := string(e.Key)
		if _, duplicate := got[key]; duplicate {
			t.Fatalf("duplicate key %q", key)
		}
		got[key] = string(e.Value)
		if len(e.Value) != 0 {
			e.Value[0] ^= 0xff
		}
		e.Key[0] ^= 0xff
	}
	return got
}

func TestScanReiteratesWithOwnedEntries(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	seq := db.Scan(t.Context())
	if got := scanContents(t, seq); len(got) != 0 {
		t.Fatalf("empty database yielded %v", got)
	}
	const key = "\x00\xff"
	mustPut(t, db, key, "previous")
	mustPut(t, db, key, "\xff\x00")
	mustPut(t, db, "empty", "")
	mustPut(t, db, "gone", "previous")
	if err := db.Delete([]byte("gone")); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{key: "\xff\x00", "empty": ""}
	for range 2 {
		if got := scanContents(t, seq); !maps.Equal(got, want) {
			t.Fatalf("scan = %q, want %q", got, want)
		}
		wantValue(t, db, key, want[key])
	}
}

func TestScanKeepsUnreadValuesAcrossCompact(t *testing.T) {
	start := make(chan struct{})
	db := newTestDBWithHooks(t, testOptions(), nil, &Hooks{CleanerStart: start})
	want := make(map[string]string)
	old := make(map[string]*storage.File)
	for _, k := range []string{"a", "b", "c", "d"} {
		want[k] = strings.Repeat(k, 64)
		mustPut(t, db, k, want[k])
		old[k] = db.index[k].File // Each pair of these records exceeds the target.
	}
	next, stop := iter.Pull2(db.Scan(t.Context()))
	defer stop()
	first, err, ok := next()
	if !ok || err != nil {
		t.Fatalf("first entry: %v %v", ok, err)
	}
	got := map[string]string{string(first.Key): string(first.Value)}
	unread := make([]string, 0, len(want)-1)
	for k := range want {
		if k != string(first.Key) {
			unread = append(unread, k)
		}
	}
	mustPut(t, db, unread[0], "updated")
	if err := db.Delete([]byte(unread[1])); err != nil {
		t.Fatal(err)
	}
	mustPut(t, db, "new", "later")
	result, err := db.Compact(t.Context())
	if err != nil || result.PinnedBytes == 0 {
		t.Fatalf("checkpoint while paused: %+v, %v", result, err)
	}
	close(start)
	for _, k := range unread {
		if _, err := os.Stat(old[k].Path); err != nil {
			t.Fatalf("unread source for %q: %v", k, err)
		}
	}
	for {
		e, err, ok := next()
		if !ok {
			break
		}
		if _, duplicate := got[string(e.Key)]; err != nil || duplicate {
			t.Fatalf("entry %q, duplicate %t, err %v", e.Key, duplicate, err)
		}
		got[string(e.Key)] = string(e.Value)
	}
	if !maps.Equal(got, want) {
		t.Fatalf("scan = %q, want %q", got, want)
	}
	for _, k := range unread {
		waitForUnlink(t, db, old[k].Path)
	}
}

func TestScanStopsWithoutExtraReadsOrLeases(t *testing.T) {
	var reads atomic.Int64
	db := newTestDB(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "read:before" {
			reads.Add(1)
		}
		return nil
	}})
	mustPut(t, db, "one", "1")
	mustPut(t, db, "two", "2")
	file := db.index["one"].File
	check := func(wantReads int64) {
		t.Helper()
		if reads.Load() != wantReads || file.References() != 2 || db.active != 0 {
			t.Fatalf("reads %d, refs %d, active %d", reads.Load(), file.References(), db.active)
		}
	}
	seq := db.Scan(t.Context())
	check(0)
	for _, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	check(1)
	marker := errors.New("consumer panic")
	func() {
		defer func() {
			if recover() != marker {
				t.Error("consumer panic was lost")
			}
		}()
		for _, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			panic(marker)
		}
	}()
	check(2)
}

func requireScanError(t *testing.T, seq iter.Seq2[Entry, error], want error) {
	t.Helper()
	calls := 0
	for e, err := range seq {
		calls++
		if err == nil || e.Key != nil || e.Value != nil || (want != nil && !errors.Is(err, want)) {
			t.Fatalf("terminal entry %+v, err %v, want %v", e, err, want)
		}
	}
	if calls != 1 {
		t.Fatalf("error yielded %d times", calls)
	}
}

// Cancel as soon as capture pins the first file, without relying on timing.
type cancelOnScanPin struct {
	context.Context
	file        *storage.File
	initialRefs int64
	cancel      context.CancelFunc
}

func (c cancelOnScanPin) Err() error {
	if c.file.References() > c.initialRefs {
		c.cancel()
	}
	return c.Context.Err()
}

func TestScanReleasesPartialCapture(t *testing.T) {
	db := newTestDB(t, Options{}, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	file := db.index["a"].File
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	requireScanError(t, db.Scan(cancelOnScanPin{ctx, file, 2, cancel}), context.Canceled)
	if file.References() != 2 || db.active != 0 {
		t.Fatal("partial capture retained resources")
	}
}

func TestScanPreservesHotCache(t *testing.T) {
	var reads atomic.Int64
	opts := testOptions()
	opts.CacheBytes = 2 * (128 + 1 + 1)
	db := newTestDB(t, opts, &storage.IO{Hook: func(point string) error {
		if point == "read:before" {
			reads.Add(1)
		}
		return nil
	}})
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	wantValue(t, db, "a", "A") // Keep a newer than b.
	scanContents(t, db.Scan(t.Context()))
	if reads.Load() != 2 {
		t.Fatalf("scan made %d storage reads, want 2", reads.Load())
	}
	mustPut(t, db, "c", "C")
	wantValue(t, db, "a", "A")
	if reads.Load() != 2 {
		t.Fatal("scan changed cache eviction order")
	}
	wantValue(t, db, "b", "B")
	if reads.Load() != 3 {
		t.Fatal("expected a cache miss for b")
	}
}

func TestScanCancellationAndReadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var inject atomic.Bool
	db := newTestDB(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "read:after" {
			cancel()
			if inject.Load() {
				return format.ErrCorrupt
			}
		}
		return nil
	}})
	mustPut(t, db, "a", "A")
	mustPut(t, db, "b", "B")
	file := db.index["a"].File
	requireScanError(t, db.Scan(nil), nil)
	requireScanError(t, db.Scan(ctx), context.Canceled) // Canceled after OS read.
	requireScanError(t, db.Scan(ctx), context.Canceled) // Already canceled.
	if file.References() != 2 || db.active != 0 {
		t.Fatal("canceled scan retained resources")
	}
	inject.Store(true)
	requireScanError(t, db.Scan(t.Context()), ErrCorrupt)
	requireScanError(t, db.Scan(t.Context()), ErrUnhealthy)
	if file.References() != 2 || db.active != 0 {
		t.Fatal("failed scan retained resources")
	}
}

func TestScanCloseFromConsumer(t *testing.T) {
	var reads atomic.Int64
	db := newTestDB(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "read:before" {
			reads.Add(1)
		}
		return nil
	}})
	mustPut(t, db, "one", "1")
	mustPut(t, db, "two", "2")
	file := db.index["one"].File
	next, stop := iter.Pull2(db.Scan(t.Context()))
	defer stop()
	if _, err, ok := next(); !ok || err != nil {
		t.Fatalf("first entry: %v %v", ok, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(db.store.Dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	mustPut(t, reopened, "three", "3")
	if e, err, ok := next(); !ok || !errors.Is(err, ErrClosed) || e.Key != nil || e.Value != nil {
		t.Fatalf("closed scan: %+v, %v, %v", e, err, ok)
	}
	if _, _, ok := next(); ok || reads.Load() != 1 || file.References() != 0 {
		t.Fatalf("after Close: yielded %v, reads %d, refs %d", ok, reads.Load(), file.References())
	}
}
