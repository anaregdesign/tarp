package engine

import (
	"context"
	"errors"
	"iter"
	"maps"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func damageProtectedReadFixture(t *testing.T, db *DB) {
	t.Helper()
	mustPut(t, db, "target", "original")
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	loc := testLoc(t, db, "target")
	f, err := os.OpenFile(loc.Segment.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{0xff}, loc.RecordOffset+loc.RecordBytes-1)
	if err = errors.Join(err, f.Close()); err != nil {
		t.Fatal(err)
	}
	db.state.Lock()
	db.cache.Clear()
	db.state.Unlock()
}

func TestForegroundRepairPreservesReadConsistencyDuringWrites(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode IndexMode
	}{{"memory", IndexMemory}, {"btree", IndexBTree}} {
		t.Run(mode.name, func(t *testing.T) {
			paused := make(chan struct{})
			resume := make(chan struct{})
			reads := make(chan struct{}, 3)
			var replacements atomic.Int32
			fs := &storage.IO{Hook: func(point string) error {
				switch point {
				case "read:before":
					select {
					case reads <- struct{}{}:
					default:
					}
				case "data-repair.rename:before":
					if replacements.Add(1) == 1 {
						close(paused)
						<-resume
					}
				}
				return nil
			}}
			db := newTestDBWithHooks(t, Options{IndexMode: mode.mode}, fs, parkedMaintenance())
			damageProtectedReadFixture(t, db)
			release := sync.OnceFunc(func() { close(resume) })
			defer release()
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for a foreground operation during repair")
				}
			}

			type result struct {
				value string
				err   error
			}
			get, batch, scan := make(chan result, 1), make(chan result, 1), make(chan result, 1)
			go func() {
				v, err := db.Get([]byte("target"))
				get <- result{string(v), err}
			}()
			await(paused)
			await(reads)
			go func() {
				v, err := db.BatchGet([][]byte{[]byte("target")})
				var value string
				if len(v) == 1 && v[0].Found {
					value = string(v[0].Value)
				}
				batch <- result{value, err}
			}()
			go func() {
				var got result
				for e, err := range db.Scan(t.Context()) {
					if err != nil {
						got.err = err
						break
					}
					got.value += string(e.Value)
				}
				scan <- got
			}()
			// All three calls now hold their original, still-pinned address.
			await(reads)
			await(reads)
			writeDone := make(chan struct{})
			var writeErr error
			go func() {
				defer close(writeDone)
				if writeErr = db.Put([]byte("other"), []byte("active")); writeErr != nil {
					return
				}
				db.state.Lock()
				db.cache.Delete("other")
				db.state.Unlock()
				value, err := db.Get([]byte("other"))
				if err != nil || string(value) != "active" {
					writeErr = errors.New("could not read unrelated active DATA during repair")
					return
				}
				writeErr = db.Put([]byte("target"), []byte("new revision"))
			}()
			await(writeDone)
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			release()
			for _, op := range []struct {
				name, want string
				done       <-chan result
			}{{"Get", "new revision", get}, {"BatchGet", "original", batch}, {"Scan", "original", scan}} {
				got := <-op.done
				if got.value != op.want || got.err != nil {
					t.Fatalf("%s returned %q, %v; want %q", op.name, got.value, got.err, op.want)
				}
			}
			if n := replacements.Load(); n != 1 {
				t.Fatalf("%d replacements for one damaged file", n)
			}
		})
	}
}

func TestScanCanceledDuringRepairReleasesTheSnapshotAndAllowsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fs := &storage.IO{Hook: func(point string) error {
		if point == "data-repair.sync:after" {
			cancel()
		}
		return nil
	}}
	db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
	damageProtectedReadFixture(t, db)
	f := testLoc(t, db, "target").Segment
	refs := f.References()
	requireScanError(t, db.Scan(ctx), context.Canceled)
	if got := f.References(); got != refs {
		t.Fatalf("references after cancellation = %d, want %d", got, refs)
	}
	wantValue(t, db, "target", "original")
}

func TestScanCancellationPreservesUncertainRepairOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fs := &storage.IO{Hook: func(point string) error {
		if point == "data-repair.dirsync:before" {
			cancel()
			return ctx.Err()
		}
		return nil
	}}
	db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
	damageProtectedReadFixture(t, db)
	requireScanError(t, db.Scan(ctx), ErrCommitUncertain)
	if err := db.Put([]byte("next"), nil); !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("Put after uncertain DATA replacement: %v", err)
	}
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
	old := make(map[string]*storage.DataSegment)
	for _, k := range []string{"a", "b", "c", "d"} {
		want[k] = strings.Repeat(k, 64)
		mustPut(t, db, k, want[k])
		old[k] = testLoc(t, db, k).Segment // Each pair of these records exceeds the target.
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
	if err != nil || result.PinnedRetiredDataBytes == 0 {
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
	segment := testLoc(t, db, "one").Segment
	baseline := segment.References()
	check := func(wantReads int64) {
		t.Helper()
		if reads.Load() != wantReads || segment.References() != baseline || db.active != 0 {
			t.Fatalf("reads %d, refs %d, active %d", reads.Load(), segment.References(), db.active)
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
	segment     *storage.DataSegment
	initialRefs int64
	cancel      context.CancelFunc
}

func (c cancelOnScanPin) Err() error {
	if c.segment.References() > c.initialRefs {
		c.cancel()
	}
	return c.Context.Err()
}

func TestScanReleasesPartialCapture(t *testing.T) {
	db := newTestDB(t, Options{}, nil)
	mustPut(t, db, "a", "1")
	mustPut(t, db, "b", "2")
	segment := testLoc(t, db, "a").Segment
	baseline := segment.References()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	requireScanError(t, db.Scan(cancelOnScanPin{ctx, segment, baseline, cancel}), context.Canceled)
	if segment.References() != baseline || db.active != 0 {
		t.Fatal("partial capture retained resources")
	}
}

func TestScanPreservesHotCache(t *testing.T) {
	var reads atomic.Int64
	opts := testOptions()
	opts.ValueCacheBudgetBytes = 2 * (128 + 1 + 1)
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
		t.Fatal("expected a value-cache miss for b")
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
	segment := testLoc(t, db, "a").Segment
	baseline := segment.References()
	requireScanError(t, db.Scan(nil), nil)
	requireScanError(t, db.Scan(ctx), context.Canceled) // Canceled after OS read.
	requireScanError(t, db.Scan(ctx), context.Canceled) // Already canceled.
	if segment.References() != baseline || db.active != 0 {
		t.Fatal("canceled scan retained resources")
	}
	inject.Store(true)
	requireScanError(t, db.Scan(t.Context()), ErrCorrupt)
	requireScanError(t, db.Scan(t.Context()), ErrUnhealthy)
	if segment.References() != baseline || db.active != 0 {
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
	segment := testLoc(t, db, "one").Segment
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
	if _, _, ok := next(); ok || reads.Load() != 1 || segment.References() != 0 {
		t.Fatalf("after Close: yielded %v, reads %d, refs %d", ok, reads.Load(), segment.References())
	}
}

func TestPrefixReadsOnlyMatchingData(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode IndexMode
	}{{"btree", IndexBTree}, {"memory", IndexMemory}} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int64
			db := newTestDBWithHooks(t, Options{IndexMode: tc.mode}, &storage.IO{Hook: func(point string) error {
				if point == "read:before" {
					reads.Add(1)
				}
				return nil
			}}, parkedMaintenance())
			for _, k := range []string{"a/1", "a/2", "b/1", "c/1"} {
				mustPut(t, db, k, k)
			}
			db.cache.Clear()
			reads.Store(0)
			var n int
			for _, err := range db.KeysPrefix(t.Context(), []byte("a/")) {
				if err != nil {
					t.Fatal(err)
				}
				n++
			}
			if n != 2 || reads.Load() != 0 {
				t.Fatalf("KeysPrefix returned %d keys with %d DATA reads", n, reads.Load())
			}
			for _, prefix := range []string{"missing", "a/"} {
				for e, err := range db.ScanPrefix(t.Context(), []byte(prefix)) {
					if err != nil || !strings.HasPrefix(string(e.Key), prefix) || string(e.Value) != string(e.Key) {
						t.Fatalf("ScanPrefix(%q) = %q, %q, %v", prefix, e.Key, e.Value, err)
					}
				}
			}
			if n := reads.Load(); n != 2 {
				t.Fatalf("ScanPrefix made %d DATA reads, want 2", n)
			}
		})
	}
}

func TestScanPrefixObservesCancellationAtUpperBound(t *testing.T) {
	for _, mode := range []IndexMode{IndexMemory, IndexBTree} {
		db := newTestDB(t, Options{IndexMode: mode}, nil)
		mustPut(t, db, "a/1", "ok")
		mustPut(t, db, "b/1", "skip")
		ctx, cancel := context.WithCancel(t.Context())
		next, stop := iter.Pull2(db.ScanPrefix(ctx, []byte("a/")))
		entry, err, ok := next()
		if err != nil || !ok || string(entry.Value) != "ok" {
			cancel()
			stop()
			t.Fatalf("first entry = %+v, %v, %t", entry, err, ok)
		}
		cancel()
		_, err, ok = next()
		stop()
		if !ok || !errors.Is(err, context.Canceled) {
			t.Fatalf("end of canceled prefix = %v, %t", err, ok)
		}
	}
}

// Cancel after scan admission when the memory index starts capturing a range.
type cancelOnCapture struct {
	context.Context
	cancel context.CancelFunc
}

func (c cancelOnCapture) Done() <-chan struct{} {
	c.cancel()
	return c.Context.Done()
}

func TestCancelDuringBoundedMemoryCaptureReleasesAdmission(t *testing.T) {
	db := newTestDB(t, Options{IndexMode: IndexMemory}, nil)
	mustPut(t, db, "a/1", "before")
	segment := testLoc(t, db, "a/1").Segment
	references := segment.References()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	requireScanError(t, db.ScanPrefix(cancelOnCapture{ctx, cancel}, []byte("a/")), context.Canceled)
	if db.active != 0 || segment.References() != references {
		t.Fatal("canceled capture retained a file or admission")
	}
	mustPut(t, db, "a/1", "after")
	got := scanContents(t, db.ScanPrefix(t.Context(), []byte("a/")))
	if !maps.Equal(got, map[string]string{"a/1": "after"}) {
		t.Fatalf("retry contents = %v", got)
	}
}

func TestFullIterationEndsAfterCancelInLastCallback(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode IndexMode
	}{{"memory", IndexMemory}, {"btree", IndexBTree}} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t, Options{IndexMode: tc.mode}, nil)
			mustPut(t, db, "only", "value")
			snapshot, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snapshot.Close()
			for _, tc := range []struct {
				name string
				keys func(context.Context) iter.Seq2[[]byte, error]
				scan func(context.Context) iter.Seq2[Entry, error]
			}{
				{"DB", db.Keys, db.Scan},
				{"Snapshot", snapshot.Keys, snapshot.Scan},
			} {
				for _, name := range []string{"Keys", "Scan"} {
					t.Run(tc.name+"/"+name, func(t *testing.T) {
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						count := 0
						visit := func(key []byte, err error) bool {
							if err != nil || string(key) != "only" {
								t.Fatalf("unexpected item %d: key %q, error %v", count, key, err)
							}
							count++
							cancel()
							return true
						}
						if name == "Keys" {
							tc.keys(ctx)(visit)
						} else {
							tc.scan(ctx)(func(e Entry, err error) bool { return visit(e.Key, err) })
						}
						if count != 1 {
							t.Fatalf("got %d keys, want 1", count)
						}
					})
				}
			}
		})
	}
}
