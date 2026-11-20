package memory_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func TestMemoryConcurrentReadersAndPublishedMultiKeyTransactions(t *testing.T) {
	db := memory.New()
	defer db.Close()
	const width = 5
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				snapshot := db.View()
				var sequence uint64
				var n int
				err := snapshot.All(func(_ string, addr index.Address) bool {
					if n > 0 && sequence != addr.Sequence {
						t.Errorf("partially published batch %d/%d", sequence, addr.Sequence)
					}
					sequence = addr.Sequence
					n++
					return true
				})
				snapshot.Close()
				if err != nil || n != 0 && n != width {
					t.Errorf("partial snapshot: %d %v", n, err)
				}
			}
		})
	}
	for seq := uint64(1); seq <= 100; seq++ {
		tx := db.Begin()
		for n := range width {
			if err := tx.Put(fmt.Sprint(n), index.Address{FileID: 1, Offset: int64(n) * 64, Bytes: 64, Sequence: seq}); err != nil {
				t.Fatal(err)
			}
		}
		tx.Commit()
	}
	wg.Wait()
}

func TestRelocateChecksFullAddressAndPreservesExistingViews(t *testing.T) {
	db := memory.New()
	defer db.Close()
	old := index.Address{FileID: 1, Offset: 64, Bytes: 80, ValueBytes: 16, Sequence: 5}
	next := old
	next.FileID = 7
	tx := db.Begin()
	if err := tx.Put("k", old); err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	tx.Commit()
	view := db.View()
	defer view.Close()

	moves := db.(index.ResidentRelocator)
	stale := old
	stale.Sequence--
	if moves.Relocate("k", stale, next) {
		t.Fatal("relocated a record at a different sequence")
	}
	if !moves.Relocate("k", old, next) || moves.Relocate("k", old, next) || moves.Relocate("absent", old, next) {
		t.Fatal("expected only the first relocation to match")
	}
	if addr, ok, err := db.Lookup("k"); err != nil || !ok || addr != next {
		t.Fatalf("live index lookup returned %+v, %t, %v", addr, ok, err)
	}
	if addr, ok, err := view.Lookup("k"); err != nil || !ok || addr != old {
		t.Fatalf("captured lookup returned %+v, %t, %v", addr, ok, err)
	}
}
