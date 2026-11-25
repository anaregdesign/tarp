package engine

import "testing"

func TestPrepareMixedCopiesRepeatedKeysOnce(t *testing.T) {
	db := newTestDBWithHooks(t, Options{}, nil, parkedMaintenance())
	key := []byte("repeated-key")
	at := func(i int) (MixedOp, error) { return MixedOp{Key: key, Delete: i%2 == 0}, nil }
	var batch PreparedMixedBatch
	allocations := func(count int) float64 {
		return testing.AllocsPerRun(3, func() {
			var err error
			batch, err = db.PrepareMixed(count, at)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	one := allocations(1)
	many := allocations(MaxWriteBatchEntries)
	if many > one+2 {
		t.Errorf("preparing one key allocated %.0f times, %d repetitions allocated %.0f times", one, MaxWriteBatchEntries, many)
	}
	if batch.order != nil || len(batch.records) != 1 || cap(batch.records) > 16 || batch.records[0].inputIndex != MaxWriteBatchEntries-1 || batch.records[0].key != string(key) {
		t.Fatalf("coalesced batch = %+v", batch.records)
	}
}

func TestPrepareMixedKeepsUniqueKeysInInputOrder(t *testing.T) {
	db := newTestDBWithHooks(t, Options{}, nil, parkedMaintenance())
	keys := []string{"z", "a", "m"}
	batch, err := db.PrepareMixed(len(keys), func(i int) (MixedOp, error) {
		return MixedOp{Key: []byte(keys[i])}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if batch.order != nil || len(batch.records) != len(keys) {
		t.Fatalf("unique keys need no reordering: %+v", batch)
	}
	for i, record := range batch.records {
		if record.key != keys[i] || record.inputIndex != i {
			t.Fatalf("record %d = %+v, want input key %q", i, record, keys[i])
		}
	}
}
