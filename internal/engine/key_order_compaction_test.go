package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

const estimateFixtureRows = 24
const estimateFixtureRecordBytes = 96

func estimateKey(i int) string { return fmt.Sprintf("%02d", 2*i) }

func estimateFixture(t *testing.T, mode IndexMode, rowsPerSegment int) (*DB, string) {
	t.Helper()
	var estimating atomic.Bool
	fs := &storage.IO{Hook: func(point string) error {
		if estimating.Load() {
			t.Errorf("estimate performed I/O at %s", point)
		}
		return nil
	}}
	opts := testOptions()
	opts.IndexMode = mode
	opts.TargetSegmentBytes = int64(format.SegmentHeaderBytes + rowsPerSegment*estimateFixtureRecordBytes)
	db := newTestDBWithHooks(t, opts, fs, parkedMaintenance())
	value := strings.Repeat("a", estimateFixtureRecordBytes-format.RecordHeaderBytes-2)
	for i := range estimateFixtureRows {
		mustPut(t, db, estimateKey(i), value)
	}
	before := db.store.Current
	estimating.Store(true)
	estimate, err := db.EstimateKeyOrderCompaction(t.Context())
	estimating.Store(false)
	if err != nil || estimate.OrderedBase || estimate.CheckpointCutoffSequence != 0 ||
		estimate.Sequence != estimateFixtureRows || estimate.RewriteRecordBytesUpperBound != estimateFixtureRows*estimateFixtureRecordBytes ||
		estimate.PendingLogRecordBytes != estimate.RewriteRecordBytesUpperBound || db.store.Current != before {
		t.Fatalf("estimate before initial checkpoint = %+v, %v", estimate, err)
	}
	if _, err := db.CompactByKey(t.Context()); err != nil {
		t.Fatal(err)
	}
	estimate, err = db.EstimateKeyOrderCompaction(t.Context())
	if err != nil || !estimate.OrderedBase || estimate.Sequence != estimate.CheckpointCutoffSequence ||
		estimate.RewriteRecordBytesUpperBound != 0 || estimate.PendingLogRecordBytes != 0 {
		t.Fatalf("estimate at unchanged cutoff = %+v, %v", estimate, err)
	}
	return db, value
}

func checkEstimatedRewrite(t *testing.T, db *DB, wantBytes, wantUpper uint64) {
	t.Helper()
	before := make(map[uint64]bool)
	for _, entry := range db.store.CatalogSnapshot().Entries {
		if entry.Kind == format.SnapshotKind {
			before[entry.ID] = true
		}
	}
	estimate, err := db.EstimateKeyOrderCompaction(t.Context())
	if err != nil || !estimate.OrderedBase || estimate.RewriteRecordBytesUpperBound != wantUpper || estimate.Sequence <= estimate.CheckpointCutoffSequence {
		t.Fatalf("estimate = %+v, %v, want bound %d", estimate, err, wantUpper)
	}
	if _, err := db.CompactByKey(t.Context()); err != nil {
		t.Fatal(err)
	}
	var rewrites uint64
	for _, entry := range db.store.CatalogSnapshot().Entries {
		if entry.Kind == format.SnapshotKind && !before[entry.ID] {
			rewrites += uint64(entry.Bytes - format.SegmentHeaderBytes)
		}
	}
	if rewrites != wantBytes || rewrites > estimate.RewriteRecordBytesUpperBound {
		t.Fatalf("rewrites %d, want %d and bound %d", rewrites, wantBytes, estimate.RewriteRecordBytesUpperBound)
	}
}

func TestEstimateKeyOrderCompactionBoundsRewrites(t *testing.T) {
	for _, mode := range []IndexMode{IndexBTree, IndexMemory} {
		for _, rowsPerSegment := range []int{2, 4} {
			for _, tt := range []struct {
				name          string
				second        int
				affectedFiles uint64
			}{
				{name: "adjacent", second: 1, affectedFiles: 1},
				{name: "scattered", second: 5, affectedFiles: 2},
			} {
				t.Run(fmt.Sprintf("%v/%d/%s", mode, rowsPerSegment, tt.name), func(t *testing.T) {
					db, value := estimateFixture(t, mode, rowsPerSegment)
					value = strings.ToUpper(value)
					mustPut(t, db, estimateKey(0), value)
					stale, err := db.EstimateKeyOrderCompaction(t.Context())
					if err != nil || stale.PendingLogRecordBytes != estimateFixtureRecordBytes {
						t.Fatalf("estimate before second write = %+v, %v", stale, err)
					}
					mustPut(t, db, estimateKey(tt.second), value)
					next, err := db.EstimateKeyOrderCompaction(t.Context())
					if err != nil || next.Sequence != stale.Sequence+1 || next.PendingLogRecordBytes != 2*estimateFixtureRecordBytes {
						t.Fatalf("estimate after second write = %+v, %v", next, err)
					}
					bytesPerSegment := uint64(rowsPerSegment * estimateFixtureRecordBytes)
					checkEstimatedRewrite(t, db, tt.affectedFiles*bytesPerSegment, (tt.affectedFiles+2)*bytesPerSegment)
					for i := range estimateFixtureRows {
						want := strings.ToLower(value)
						if i == 0 || i == tt.second {
							want = value
						}
						wantValue(t, db, estimateKey(i), want)
					}
				})
			}
		}
	}
}

func TestEstimateKeyOrderCompactionAccountsForDeletesAndInserts(t *testing.T) {
	for _, mode := range []IndexMode{IndexBTree, IndexMemory} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			db, value := estimateFixture(t, mode, 2)
			if err := db.Delete([]byte(estimateKey(0))); err != nil {
				t.Fatal(err)
			}
			checkEstimatedRewrite(t, db, estimateFixtureRecordBytes, estimateFixtureRecordBytes)
			if _, err := db.Get([]byte(estimateKey(0))); !errors.Is(err, ErrNotFound) {
				t.Fatalf("deleted key after Compact = %v", err)
			}
			mustPut(t, db, "05", value) // Between the two keys of an unchanged snapshot segment.
			checkEstimatedRewrite(t, db, 3*estimateFixtureRecordBytes, 3*estimateFixtureRecordBytes)
			wantValue(t, db, "05", value)
			mustPut(t, db, "zz", value) // Beyond all snapshot ranges.
			checkEstimatedRewrite(t, db, estimateFixtureRecordBytes, 3*estimateFixtureRecordBytes)
			wantValue(t, db, "zz", value)
		})
	}
}

func TestEstimateKeyOrderCompactionDuringIndexRelocation(t *testing.T) {
	remapping := make(chan struct{})
	resumed := make(chan struct{})
	var started, released sync.Once
	resume := func() { released.Do(func() { close(resumed) }) }
	defer resume()
	fs := &storage.IO{Hook: func(point string) error {
		if point == "compact.remap:after" {
			started.Do(func() { close(remapping) })
			<-resumed
		}
		return nil
	}}
	db := newTestDBWithHooks(t, testOptions(), fs, parkedMaintenance())
	mustPut(t, db, "a", "old")
	completed := make(chan error, 1)
	go func() {
		_, err := db.CompactByKey(t.Context())
		completed <- err
	}()
	select {
	case <-remapping:
	case err := <-completed:
		t.Fatalf("checkpoint stopped before index relocation = %v", err)
	}
	if _, err := db.EstimateKeyOrderCompaction(t.Context()); !errors.Is(err, ErrBusy) {
		t.Fatalf("estimate during index relocation = %v", err)
	}
	mustPut(t, db, "a", "new")
	resume()
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	estimate, err := db.EstimateKeyOrderCompaction(t.Context())
	if err != nil || estimate.Sequence != 2 || estimate.CheckpointCutoffSequence != 1 ||
		estimate.RewriteRecordBytesUpperBound != estimate.PendingLogRecordBytes {
		t.Fatalf("estimate after relocation = %+v, %v", estimate, err)
	}
}

func TestEstimateKeyOrderCompactionRespectsCancellationAndClose(t *testing.T) {
	db := newTestDBWithHooks(t, testOptions(), nil, parkedMaintenance())
	before := db.store.Current
	cancelMaintenanceWaitingForWriter(t, db, func() bool {
		db.life.Lock()
		defer db.life.Unlock()
		return db.active != 0
	}, func(ctx context.Context) error {
		_, err := db.EstimateKeyOrderCompaction(ctx)
		return err
	})
	if db.store.Current != before {
		t.Fatal("canceled estimate changed CURRENT")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.EstimateKeyOrderCompaction(t.Context()); !errors.Is(err, ErrClosed) {
		t.Fatalf("estimate after Close = %v", err)
	}
}
