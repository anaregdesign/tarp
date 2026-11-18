package engine

import (
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestCheckpointSyncsEveryCompletedSnapshotBeforePublication(t *testing.T) {
	for _, failFinalSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("fail-final-sync-%t", failFinalSync), func(t *testing.T) {
			var files, synced int
			var published bool
			fs := &storage.IO{CleanerStart: make(chan struct{}), Hook: func(point string) error {
				switch point {
				case "compact.segment.write:after":
					files++
				case "compact.sync:before":
					if failFinalSync && synced == 1 {
						return syscall.ENOSPC
					}
				case "compact.sync:after":
					synced++
				case "current.rename:before":
					if files != 0 {
						if synced != files {
							t.Fatalf("publishing %d snapshot files after %d full-file syncs", files, synced)
						}
						published = true
					}
				}
				return nil
			}}
			opts := Options{MaxRecordBytes: 64, TargetSegmentBytes: 180}
			db := newTestDB(t, opts, fs)
			for _, key := range []string{"a", "b", "c", "d"} {
				mustPut(t, db, key, "v")
			}
			_, err := db.Compact(t.Context())
			if files != 2 || published == failFinalSync ||
				(failFinalSync && !errors.Is(err, ErrNoSpace)) || (!failFinalSync && err != nil) {
				t.Fatalf("files=%d synced=%d published=%t err=%v", files, synced, published, err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(db.store.Dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if (reopened.store.Catalog.Cutoff == 4) != published {
				t.Fatalf("cutoff after reopen %d, published %t", reopened.store.Catalog.Cutoff, published)
			}
			for _, key := range []string{"a", "b", "c", "d"} {
				wantValue(t, reopened, key, "v")
			}
		})
	}
}
