package tarp_test

import (
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestTypedEstimateKeyOrderCompaction(t *testing.T) {
	db, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put("key", "value"); err != nil {
		t.Fatal(err)
	}
	estimate, err := db.EstimateKeyOrderCompaction(t.Context())
	if err != nil || estimate.Sequence != 1 || estimate.CheckpointCutoffSequence != 0 ||
		estimate.RewriteRecordBytesUpperBound != db.Stats().LiveRecordBytes {
		t.Fatalf("typed estimate = %+v, %v", estimate, err)
	}
}
