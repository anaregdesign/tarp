package engine

import (
	"errors"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestRepackPublishesWholeGroupAndPreservesConcurrentTail(t *testing.T) {
	for _, tc := range []struct {
		name, barrier, fault string
		published            bool
	}{
		{"success", "clean.output.sync:after", "", true},
		{"rotation during catalog preparation", "clean.catalog.dirsync:after", "", true},
		{"before rename", "clean.output.sync:after", "clean.current.rename:before", false},
		{"after rename", "clean.output.sync:after", "clean.current.rename:after", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var staged atomic.Bool
			copied, resume := make(chan struct{}), make(chan struct{})
			db := newTestDB(t, Options{}, &storage.IO{CleanerStart: make(chan struct{}), Hook: func(point string) error {
				if point == tc.barrier && staged.CompareAndSwap(false, true) {
					close(copied)
					<-resume
				}
				if point == tc.fault {
					return syscall.EIO
				}
				return nil
			}})
			mustPut(t, db, "prefix", "earlier")
			prefix := sealForCleaning(t, db)
			var group []format.File
			for _, key := range []string{"first", "updated", "deleted", "last"} {
				mustPut(t, db, key, "original")
				group = append(group, sealForCleaning(t, db))
			}
			done := make(chan error, 1)
			go func() { done <- db.cleanSegment(t.Context(), group...) }()
			select {
			case <-copied:
			case err := <-done:
				t.Fatalf("copy ended before publication barrier: %v", err)
			}
			// Every source has been copied. Commit a new sealed tail and an
			// active append before allowing the worker to install the group.
			var tail format.File
			func() {
				defer close(resume)
				mustPut(t, db, "updated", "new")
				if err := db.Delete([]byte("deleted")); err != nil {
					t.Fatal(err)
				}
				tail = sealForCleaning(t, db)
				mustPut(t, db, "active", "later")
			}()
			err := <-done
			if tc.fault != "" {
				if !errors.Is(err, ErrCommitUncertain) || !errors.Is(db.health(), ErrUnhealthy) {
					t.Fatalf("unexpected publication outcome: %v, health: %v", err, db.health())
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := db.index["deleted"]; ok || db.index["updated"].File.ID != tail.ID {
					t.Fatal("repacking overwrote newer index locations")
				}
			}
			dir := db.store.Dir
			if err := db.Close(); err != nil && !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			image, err := OpenWithIO(dir, Options{}, &storage.IO{CleanerStart: make(chan struct{})})
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			files := image.store.Catalog.Files
			if len(files) < 3 || files[0] != prefix || files[len(files)-1] != tail || image.store.Current.Sequence != 8 {
				t.Fatalf("lost prefix or acknowledged tail: %+v, sequence %d", files, image.store.Current.Sequence)
			}
			middle := files[1 : len(files)-1]
			if tc.published {
				if len(middle) != 1 || middle[0].Kind != format.Cleaned || middle[0].First != group[0].First || middle[0].Last != group[len(group)-1].Last {
					t.Fatalf("incomplete group replacement: %+v", middle)
				}
			} else if !slices.Equal(middle, group) {
				t.Fatalf("original group changed before rename: %+v", middle)
			}
			wantValue(t, image, "prefix", "earlier")
			wantValue(t, image, "first", "original")
			wantValue(t, image, "last", "original")
			wantValue(t, image, "updated", "new")
			wantValue(t, image, "active", "later")
			if _, err := image.Get([]byte("deleted")); !errors.Is(err, ErrNotFound) {
				t.Fatalf("repacked deleted key: %v", err)
			}
		})
	}
}
