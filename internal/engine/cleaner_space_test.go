package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestWriteReserveRejectsBeforeAppendAndRecovers(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(*DB) error
	}{
		{"put", func(db *DB) error { return db.Put([]byte("hot"), []byte("next")) }},
		{"delete", func(db *DB) error { return db.Delete([]byte("hot")) }},
		{"batch put", func(db *DB) error { return db.BatchPut([]Entry{{Key: []byte("hot"), Value: []byte("next")}}) }},
		{"batch delete", func(db *DB) error { return db.BatchDelete([][]byte{[]byte("hot")}) }},
		{"mixed batch", func(db *DB) error {
			return db.ApplyMixed(2, func(i int) MixedOp {
				if i == 0 {
					return MixedOp{Key: []byte("new"), Value: []byte("next")}
				}
				return MixedOp{Key: []byte("hot"), Delete: true}
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var appends int
			fs := &storage.IO{Hook: func(point string) error {
				if point == "data.write:before" {
					appends++
				}
				return nil
			}}
			db := newTestDBWithHooks(t, testOptions(), fs, &Hooks{CleanerStart: make(chan struct{})})
			mustPut(t, db, "hot", "old")
			before := db.store.Current

			// Inject selected capacity and a fresh low-space sample, without a
			// large file, a real filesystem fill, or a foreground I/O failure.
			db.state.Lock()
			realSelected := db.stats.SelectedDataBytes
			db.stats.SelectedDataBytes = cleanerReserveActivationBytes
			db.state.Unlock()
			db.cleaner.space = cleanerSpace{checked: time.Now().Add(time.Hour)}
			priorAppends := appends
			var op *OpError
			if err := tc.write(db); !errors.Is(err, ErrNoSpace) || !errors.As(err, &op) || op.Outcome != NotApplied || op.Phase != "space reserve" {
				t.Fatalf("write outcome = %v", err)
			}
			if db.health() != nil || appends != priorAppends || db.store.Current != before {
				t.Fatal("reserve rejection attempted DATA append or changed the frontier")
			}
			wantValue(t, db, "hot", "old")

			// Advance the sample to model a successful retirement and recheck
			// actual free space. The same operation is safe to retry.
			db.cleaner.space.checked = time.Time{}
			if err := tc.write(db); err != nil {
				t.Fatal(err)
			}
			if db.store.Current.LastUserSequence == before.LastUserSequence || db.AutoCompactionStatus().WriteReserveBytes == 0 {
				t.Fatal("retried write did not commit with the reserve enabled")
			}
			db.state.Lock()
			db.stats.SelectedDataBytes -= cleanerReserveActivationBytes - realSelected
			db.state.Unlock()
		})
	}
}

func TestCleanerMakesProgressWithContinuouslyQueuedWriters(t *testing.T) {
	opts := Options{IndexMode: IndexMemory, MaxRecordBytes: 3072, TargetSegmentBytes: 4096}
	db := newTestDBWithHooks(t, opts, nil, &Hooks{CleanerStart: make(chan struct{})})
	mustPut(t, db, "hot", strings.Repeat("x", 2048))
	source := sealForCleaning(t, db)
	mustPut(t, db, "hot", "live")
	db.cleaner.writers.Store(1)
	defer db.cleaner.writers.Store(0)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if again, retry := db.cleanPass(ctx); !again || retry {
		t.Fatalf("cleaner did not advance during sustained writes: %+v", db.AutoCompactionStatus())
	}
	if db.store.CatalogEntries().Index(source) >= 0 {
		t.Fatal("obsolete source remained selected")
	}
	wantValue(t, db, "hot", "live")
}

func TestCleanerDefersPinnedSourcesUnderSpacePressure(t *testing.T) {
	opts := Options{IndexMode: IndexMemory, MaxRecordBytes: 3072, TargetSegmentBytes: 4096}
	db := newTestDBWithHooks(t, opts, nil, &Hooks{CleanerStart: make(chan struct{})})
	mustPut(t, db, "hot", strings.Repeat("x", 2048))
	source := sealForCleaning(t, db)
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	mustPut(t, db, "hot", "live")
	db.cleaner.pressure.Store(true)
	before := db.store.Current
	db.cleanPass(t.Context())
	if got := db.AutoCompactionStatus(); db.store.Current != before || got.PinnedReclaimDataBytes == 0 || got.PinnedReclaimDataBytes != got.PendingReclaimDataBytes {
		t.Fatalf("pinned source was not deferred: %+v", got)
	}
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if again, retry := db.cleanPass(t.Context()); !again || retry || db.store.CatalogEntries().Index(source) >= 0 {
		t.Fatalf("pin release did not make the source eligible: %+v", db.AutoCompactionStatus())
	}
	wantValue(t, db, "hot", "live")
}
