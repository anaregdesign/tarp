package btree

import (
	"errors"
	"os"
	"runtime"
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
