package tarp

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/storage"
)

func TestMeasurementFixtureCopiesRemainPrivate(t *testing.T) {
	keys, value, opts := measurementInput(64, 8)
	seed := newTestDB(t, opts, nil)
	for _, key := range keys {
		if err := seed.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	fixture := filepath.Join(t.TempDir(), "fixture")
	measurementPrepareFixture(t, seed.store.Dir, fixture, opts, keys, value)
	first, _ := measurementOpenClone(t, fixture, opts)
	second, _ := measurementOpenClone(t, fixture, opts)
	digest, count := measurementState(t, second)
	if err := first.Put(keys[0], []byte("changed")); err != nil {
		t.Fatal(err)
	}
	third, _ := measurementOpenClone(t, fixture, opts)
	if got, err := third.Get(keys[0]); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("private clone changed its common fixture: %x %v", got, err)
	}
	if current, n := measurementState(t, third); current != digest || n != count ||
		observedFrontier(third).Sequence != observedFrontier(second).Sequence {
		t.Fatal("fresh copies did not start with the same contents and history")
	}
}

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
			if backup && (result["backup_reopen_verified"] != true ||
				result["backup_capture_sequence"] != result["restored_sequence"] ||
				result["restored_live_keys"] != 9 || result["restored_state_sha256"] != result["fixture_state_sha256"]) {
				t.Fatalf("backup capture/full-state validation was incomplete: %#v", result)
			}
		})
	}
}
