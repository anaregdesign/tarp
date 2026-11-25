package engine

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestKeysRangePerformsZeroDataReads(t *testing.T) {
	for _, mode := range []IndexMode{IndexMemory, IndexBTree} {
		t.Run(fmt.Sprintf("mode=%d", mode), func(t *testing.T) {
			var dataReads atomic.Int64
			opts := testOptions()
			opts.IndexMode = mode
			db, err := OpenWithIO(t.TempDir(), opts, &storage.IO{
				Hook: func(point string) error {
					if strings.HasPrefix(point, "read:") && strings.Contains(point, ".data") {
						dataReads.Add(1)
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			for i := range 40 {
				if err := db.Put(fmt.Appendf(nil, "k/%02d", i), fmt.Appendf(nil, "v/%02d", i)); err != nil {
					t.Fatal(err)
				}
			}
			dataReads.Store(0)

			for _, rev := range []bool{false, true} {
				var count int
				for _, err := range db.KeysRange(t.Context(), []byte("k/05"), []byte("k/25"), rev) {
					if err != nil {
						t.Fatal(err)
					}
					count++
				}
				if count != 20 {
					t.Fatalf("rev=%v count=%d, want 20", rev, count)
				}
			}
			if got := dataReads.Load(); got != 0 {
				t.Fatalf("KeysRange performed %d DATA reads, want 0", got)
			}

			snap, err := db.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			dataReads.Store(0)

			for _, rev := range []bool{false, true} {
				var count int
				for _, err := range snap.KeysRange(t.Context(), []byte("k/05"), []byte("k/25"), rev) {
					if err != nil {
						t.Fatal(err)
					}
					count++
				}
				if count != 20 {
					t.Fatalf("snap rev=%v count=%d, want 20", rev, count)
				}
			}
			if got := dataReads.Load(); got != 0 {
				t.Fatalf("Snapshot.KeysRange performed %d DATA reads, want 0", got)
			}
		})
	}
}
