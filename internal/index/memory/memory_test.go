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
