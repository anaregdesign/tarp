package engine

import (
	"errors"
	"strings"
	"testing"
)

func TestSnapshotBatchGetRejectsMixedCacheHitsBeforeCopying(t *testing.T) {
	const copies = 128
	db := newTestDBWithHooks(t, Options{IndexMode: IndexMemory, MaxBatchGetValueBytes: 96}, nil, parkedMaintenance())
	mustPut(t, db, "hot", strings.Repeat("v", 64))
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	keys := make([][]byte, copies+1)
	keys[0] = []byte("missing")
	for i := 1; i < len(keys); i++ {
		keys[i] = []byte("hot")
	}
	// The index plan needs a few slices. A rejected batch must never allocate
	// the 128 independent value copies requested by its cached inputs.
	allocs := testing.AllocsPerRun(3, func() {
		results, err := snapshot.BatchGet(keys)
		var limit *BatchLimitError
		if results != nil || !errors.As(err, &limit) || limit.Kind != BatchGetValueBytesLimit ||
			limit.Index != 2 || limit.Actual != 128 || limit.Limit != 96 {
			t.Fatalf("rejected BatchGet returned %v, %v", results, err)
		}
	})
	if allocs >= copies {
		t.Fatalf("rejected BatchGet made %.0f allocations for %d cached inputs", allocs, copies)
	}
}
