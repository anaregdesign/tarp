package tarp

import (
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestMeasurementMaintenanceCopyAndOverlap(t *testing.T) {
	for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "slow-backup-compact"} {
		t.Run(mode, func(t *testing.T) {
			keys, value, opts := measurementInput(64, 8)
			hooks := &measurementHooks{}
			db := newTestDB(t, opts, &storage.IO{Hook: hooks.hook})
			for _, key := range keys {
				if err := db.Put(key, value); err != nil {
					t.Fatal(err)
				}
			}
			for _, key := range keys[:4] {
				if _, err := db.Get(key); err != nil {
					t.Fatal(err)
				}
			}
			result := measureInterferenceFor(t, db, hooks, keys[:4], value, mode, 20*time.Millisecond)
			backup := mode == "backup" || mode == "backup-compact" || mode == "slow-backup-compact"
			compact := mode == "compact" || mode == "backup-compact" || mode == "slow-backup-compact"
			if (result["backup_copy_bytes"].(int64) > 0) != backup ||
				(result["backup_elapsed_ns"].(int64) > 0) != backup ||
				(result["compact_copy_bytes"].(int64) > 0) != compact ||
				(result["compact_elapsed_ns"].(int64) > 0) != compact {
				t.Fatalf("incorrect separate maintenance copy accounting: %#v", result)
			}
			if (result["observed_maintenance_api_overlap_ns"].(int64) > 0) != (backup && compact) {
				t.Fatalf("missing or invented maintenance overlap: %#v", result)
			}
		})
	}
}
