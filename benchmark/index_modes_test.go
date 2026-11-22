package benchmark

import (
	"encoding/binary"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
)

// Exercise the same byte budget while a live snapshot spans a 10x size
// increase. The metric is the charged resident page cache only; process-level
// allocations and operational latency are measured by the opt-in campaign.
func TestPagedIndexBudgetAtTenfoldCardinality(t *testing.T) {
	db, err := btree.NewIn(t.TempDir(), btree.MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	appendKeys := func(start, end int) {
		t.Helper()
		for start < end {
			tx := db.Begin()
			for n := range min(64, end-start) {
				id := start + n
				var key [16]byte
				copy(key[:], "account/")
				binary.BigEndian.PutUint64(key[8:], uint64(id))
				err := tx.Put(string(key[:]), index.Address{FileID: 1, Sequence: uint64(id) + 1, RecordOffset: int64(id) * 128, RecordBytes: 128, ValueBytes: 64})
				if err != nil {
					tx.Abort()
					t.Fatal(err)
				}
			}
			tx.Commit()
			start += min(64, end-start)
			m := db.(index.Meter).Metrics()
			if m.CacheBytes == 0 || m.CacheBytes > uint64(btree.MinBudget) {
				t.Fatalf("resident page charge %+v", m)
			}
		}
	}
	const initial = 512
	appendKeys(0, initial)
	old := db.View()
	defer old.Close()
	before := db.(index.Meter).Metrics()
	appendKeys(initial, 10*initial)
	after := db.(index.Meter).Metrics()
	if db.Count() != 10*initial || after.ScratchBytes <= before.ScratchBytes {
		t.Fatalf("count=%d, scratch %d to %d", db.Count(), before.ScratchBytes, after.ScratchBytes)
	}
	var oldCount int
	if err := old.All(func(_ string, _ index.Address) bool { oldCount++; return true }); err != nil || oldCount != initial {
		t.Fatalf("old snapshot keys=%d, err=%v", oldCount, err)
	}
}
