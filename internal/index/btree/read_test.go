package btree

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/index"
)

func TestReadWorkspaceBoundsWaitersAndCloseWakesThem(t *testing.T) {
	tr := openTest(t)
	v := tr.View().(*view)
	defer v.Close()
	for range cap(tr.reads.slots) {
		if err := v.beginRead(); err != nil {
			t.Fatal(err)
		}
		defer tr.reads.release()
	}
	const readers = 16
	finished := make(chan error, readers)
	for range readers {
		go func() { _, _, err := v.Lookup("waiting"); finished <- err }()
	}
	deadline := time.Now().Add(3 * time.Second)
	for tr.reads.waits.Load() != readers && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	m := tr.Metrics()
	if m.ReadWaits != readers || m.ReadWorkspaceBytes != m.ReadWorkspaceLimitBytes || m.ReadWorkspacePeakBytes != m.ReadWorkspaceBytes || m.ReadWorkspaceLimitBytes > uint64(MinBudget/4) {
		t.Fatalf("read reservations: %+v", m)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	for range readers {
		select {
		case err := <-finished:
			if !errors.Is(err, os.ErrClosed) {
				t.Fatalf("waiter: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Close left a reader waiting")
		}
	}
}

func TestSuspendedCursorAndBatchCallbacksReleaseReadWorkspace(t *testing.T) {
	tr := openTest(t)
	tx := tr.Begin()
	if err := tx.Put("one", addr(1)); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	v := tr.View().(*view)
	defer v.Close()
	check := func() {
		t.Helper()
		if n := tr.Metrics().ReadWorkspaceBytes; n != 0 {
			t.Fatalf("callback held %d bytes of read workspace", n)
		}
		a, found, err := v.Lookup("one")
		if err != nil || !found || a != addr(1) {
			t.Fatalf("nested read: %v %t %v", a, found, err)
		}
	}
	if err := v.All(func(string, index.Address) bool { check(); return true }); err != nil {
		t.Fatal(err)
	}
	if err := v.LookupMany([]string{"one", "two"}, func(int, index.Address, bool) { check() }); err != nil {
		t.Fatal(err)
	}
}

func TestRangeSeeksAndTraversesAnImmutableRoot(t *testing.T) {
	tr := openTest(t)
	const count = 300
	prefix := strings.Repeat("p", maxInline+1) + "\x00"
	var keys []string
	tx := tr.Begin()
	for i := range count {
		key := fmt.Sprintf("%s%03d", prefix, i)
		keys = append(keys, key)
		if err := tx.Put(key, addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	v := tr.View().(*view)
	defer v.Close()
	root, err := tr.p.read(v.root)
	if err != nil || root[4] != branch {
		t.Fatalf("fixture requires a branch root: %v", err)
	}
	separator, err := root.cell(0)
	if err != nil {
		t.Fatal(err)
	}
	firstRight, err := separator.key.string(tr.p)
	if err != nil {
		t.Fatal(err)
	}
	at := slices.Index(keys, firstRight)
	if at <= 1 || at+2 >= len(keys) {
		t.Fatalf("unexpected separator position %d", at)
	}
	tx = tr.Begin()
	if err := tx.Delete(firstRight); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	for _, tc := range []struct {
		name, lower, upper string
		start, end         int
	}{
		{"branch equality", firstRight, keys[at+2], at, at + 2},
		{"between leaves", keys[at-1] + "\xff", keys[at+2], at, at + 2},
		{"across leaves", keys[at-1], keys[at+2], at - 1, at + 2},
		{"from beginning", "", keys[1], 0, 1},
		{"through end", keys[count-2], "", count - 2, count},
		{"before first", "a", "b", 0, 0},
		{"after last", "q", "", count, count},
		{"equal bounds", firstRight, firstRight, at, at},
		{"reversed bounds", keys[at+1], firstRight, at, at},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			err := v.Range(tc.lower, tc.upper, func(key string, a index.Address) bool {
				if a != addr(uint64(tc.start+len(got))) {
					t.Errorf("address %v at position %d", a, len(got))
				}
				got = append(got, key)
				return true
			})
			if err != nil || !slices.Equal(got, keys[tc.start:tc.end]) {
				t.Fatalf("Range returned %d keys, want positions [%d,%d): %v", len(got), tc.start, tc.end, err)
			}
		})
	}
}

func TestRangeStopsAndReleasesReadWorkspaceAtCallbacks(t *testing.T) {
	tr := openTest(t)
	tx := tr.Begin()
	for i, key := range []string{"a", "b", "c"} {
		if err := tx.Put(key, addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	v := tr.View().(*view)
	defer v.Close()
	calls := 0
	err := v.Range("b", "", func(key string, _ index.Address) bool {
		calls++
		if key != "b" || tr.Metrics().ReadWorkspaceBytes != 0 {
			t.Fatalf("callback at %q with %d reserved bytes", key, tr.Metrics().ReadWorkspaceBytes)
		}
		return false
	})
	if err != nil || calls != 1 {
		t.Fatalf("break after one key: calls=%d, err=%v", calls, err)
	}
	err = v.Range("b", "", func(string, index.Address) bool {
		v.Close()
		return true
	})
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closing view from callback: %v", err)
	}
}

func TestRangeValidatesOnlyOverflowKeysItReaches(t *testing.T) {
	tr := openTest(t)
	long := strings.Repeat("z", 2*pageSize)
	keys := []string{"a", "b", "c", long}
	tx := tr.Begin()
	for i, key := range keys {
		if err := tx.Put(key, addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	v := tr.View().(*view)
	defer v.Close()
	root, err := tr.p.read(v.root)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := root.raw(len(keys) - 1)
	if err != nil {
		t.Fatal(err)
	}
	// Break the last link so reading the inline prefix still succeeds, but
	// reconstructing the complete overflow key detects the invalid chain.
	tail, err := tr.p.read(raw.tail)
	if err != nil {
		t.Fatal(err)
	}
	for tail.link() != 0 {
		tail, err = tr.p.read(tail.link())
		if err != nil {
			t.Fatal(err)
		}
	}
	order.PutUint16(tail[6:8], uint16(tail.count()-1))

	for _, tc := range []struct {
		name, upper string
		stopAfter   int
		want        []string
		wantErr     error
	}{
		{"exclusive bound", "z", 0, keys[:3], nil},
		{"caller breaks", "", 2, keys[:2], nil},
		{"corrupt match", "", 0, keys[:3], errPage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			err := v.Range("a", tc.upper, func(key string, _ index.Address) bool {
				got = append(got, key)
				return len(got) != tc.stopAfter
			})
			if !errors.Is(err, tc.wantErr) || !slices.Equal(got, tc.want) {
				t.Fatalf("got %d keys and %v; want %d and %v", len(got), err, len(tc.want), tc.wantErr)
			}
		})
	}
}

func TestRangeCloseWhileInlineKeysAreBuffered(t *testing.T) {
	tr := openTest(t)
	tx := tr.Begin()
	for i, key := range []string{"a", "b", "c", "d"} {
		if err := tx.Put(key, addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	v := tr.View().(*view)
	defer v.Close()
	var keys []string
	err := v.Range("a", "z", func(key string, _ index.Address) bool {
		keys = append(keys, key)
		if key == "b" {
			if n := tr.Metrics().ReadWorkspaceBytes; n != 0 {
				t.Fatalf("callback retained %d read workspace bytes", n)
			}
			v.Close()
		}
		return true
	})
	if !errors.Is(err, os.ErrClosed) || !slices.Equal(keys, []string{"a", "b"}) {
		t.Fatalf("got %v, %v after closing the view", keys, err)
	}
}

func TestRangeKeysSurviveLaterGroupsAndViewClose(t *testing.T) {
	tr := openTest(t)
	var keys []string
	tx := tr.Begin()
	for i := range rangeGroupSize*4 + 3 {
		key := fmt.Sprintf("v\x00%03d%s", i, strings.Repeat("x", i%17))
		if i == rangeGroupSize+3 {
			key += strings.Repeat("z", maxInline)
		}
		keys = append(keys, key)
		if err := tx.Put(key, addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	v := tr.View().(*view)
	var got []string
	upper := len(keys) - 2
	err := v.Range(keys[0], keys[upper], func(k string, a index.Address) bool {
		if a != addr(uint64(len(got))) {
			t.Fatalf("address %v at position %d", a, len(got))
		}
		got = append(got, k)
		return true
	})
	v.Close()
	if err != nil {
		t.Fatal(err)
	}
	// Exercise different group boundaries on a new view after a deletion.
	tx = tr.Begin()
	if err := tx.Delete(keys[3]); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	next := tr.View().(*view)
	err = next.Range(keys[5], "", func(string, index.Address) bool { return true })
	next.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, keys[:upper]) {
		t.Fatalf("retained keys changed: got %d, want %d matching entries", len(got), upper)
	}
}

func TestRangeDescSeeksAcrossMultiLevelBranchesAndOverflowKeys(t *testing.T) {
	tr := openTest(t)
	const count = 1500
	var keys []string
	tx := tr.Begin()
	for i := range count {
		lead := byte('b' + (i / 300))
		key := fmt.Sprintf("%c%s%04d", lead, strings.Repeat("m", 215), i)
		if i%29 == 0 {
			key += strings.Repeat("o", maxInline+64+(i%50))
		}
		keys = append(keys, key)
		if err := tx.Put(key, addr(uint64(i))); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()

	// Delete scattered keys to exercise shortened separator boundaries where
	// the initial leaf at upper has zero keys < upper.
	tx = tr.Begin()
	var live []string
	var liveAddrs []index.Address
	for i, key := range keys {
		if i%7 == 0 {
			if err := tx.Delete(key); err != nil {
				t.Fatal(err)
			}
		} else {
			live = append(live, key)
			liveAddrs = append(liveAddrs, addr(uint64(i)))
		}
	}
	tx.Commit()

	v := tr.View().(*view)
	defer v.Close()

	// Confirm the tree has multiple branch levels.
	root, err := tr.p.read(v.root)
	if err != nil || root[4] != branch {
		t.Fatalf("expected branch root: %v", err)
	}
	child0, err := tr.p.read(root.child(0))
	if err != nil || child0[4] != branch {
		t.Fatalf("expected multi-level branch tree (child kind=%d, err=%v)", child0[4], err)
	}
	sep0, err := root.cell(0)
	if err != nil {
		t.Fatal(err)
	}
	sepKey, err := sep0.key.string(tr.p)
	if err != nil {
		t.Fatal(err)
	}

	bounds := [][2]string{
		{"", ""},
		{"", live[0]},
		{"", live[1]},
		{"", sepKey},
		{"", sepKey + "\x00"},
		{sepKey, ""},
		{live[len(live)-1], ""},
		{live[len(live)-1] + "\x00", ""},
		{live[10], live[len(live)-10]},
		{live[50] + "\x00", live[120] + "\xff"},
		{"a", "b"},
		{"z", ""},
		{live[40], live[40]},
		{live[50], live[40]},
	}
	for idx, b := range bounds {
		lower, upper := b[0], b[1]
		var wantKeys []string
		var wantAddrs []index.Address
		if upper == "" || lower < upper {
			for i := len(live) - 1; i >= 0; i-- {
				if live[i] >= lower && (upper == "" || live[i] < upper) {
					wantKeys = append(wantKeys, live[i])
					wantAddrs = append(wantAddrs, liveAddrs[i])
				}
			}
		}
		var gotKeys []string
		var gotAddrs []index.Address
		err := v.RangeDesc(lower, upper, func(k string, a index.Address) bool {
			if n := tr.Metrics().ReadWorkspaceBytes; n != 0 {
				t.Fatalf("bound %d retained %d workspace bytes inside callback", idx, n)
			}
			gotKeys = append(gotKeys, k)
			gotAddrs = append(gotAddrs, a)
			return true
		})
		if err != nil {
			t.Fatalf("bound %d [%q, %q): %v", idx, lower, upper, err)
		}
		if !slices.Equal(gotKeys, wantKeys) || !slices.Equal(gotAddrs, wantAddrs) {
			t.Fatalf("bound %d [%q, %q): got %d keys, want %d", idx, lower, upper, len(gotKeys), len(wantKeys))
		}
	}

	// Verify early break and close-inside-callback semantics.
	calls := 0
	err = v.RangeDesc("b", "z", func(string, index.Address) bool {
		calls++
		return calls < 3
	})
	if err != nil || calls != 3 {
		t.Fatalf("early break: calls=%d, err=%v", calls, err)
	}
	v2 := tr.View().(*view)
	err = v2.RangeDesc("", "", func(string, index.Address) bool {
		v2.Close()
		return true
	})
	if !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close inside RangeDesc callback: got %v, want os.ErrClosed", err)
	}
}
