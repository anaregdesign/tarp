package benchmark

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
)

func TestMeasurementRotationUsesSoftTarget(t *testing.T) {
	keys, value, base := measurementInput(64, 2)
	for _, rotate := range []bool{false, true} {
		writer := newTestDB(t, measurementWriteOptions(base, len(keys[0]), len(value), rotate), nil)
		if err := writer.Put(keys[0], value); err != nil {
			t.Fatal(err)
		}
		before := engine.Observe(writer).Frontier()
		if err := writer.Put(keys[0], value); err != nil {
			t.Fatal(err)
		}
		after := engine.Observe(writer).Frontier()
		if (before.ActiveID != after.ActiveID) != rotate {
			t.Fatalf("rotate=%t, active DATA changed from %d to %d", rotate, before.ActiveID, after.ActiveID)
		}
	}
}

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
	measurementPrepareFixture(t, engine.Observe(seed).Dir(), fixture, opts, keys, value)
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
		engine.Observe(third).Frontier() != engine.Observe(second).Frontier() {
		t.Fatal("fresh copies did not start with the same contents and history")
	}
}

func TestMeasurementMaintenanceCopyAndOverlap(t *testing.T) {
	for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", pinnedBackupMode} {
		t.Run(mode, func(t *testing.T) {
			size := 8
			if mode == pinnedBackupMode {
				size = 20 // More than one 16-object cleaner retirement batch.
			}
			keys, value, opts := measurementInput(64, size)
			if mode == pinnedBackupMode {
				opts = measurementWriteOptions(opts, len(keys[0]), len(value), true)
			}
			hooks := &measurementHooks{}
			fs, engineHooks := hooks.interferenceHooks()
			db := newTestDBWithHooks(t, opts, fs, engineHooks)
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
			backup := mode == "backup" || mode == "backup-compact" || mode == pinnedBackupMode
			compact := mode == "compact" || mode == "backup-compact" || mode == pinnedBackupMode
			if (result["backup_copy_bytes"].(int64) > 0) != backup ||
				(result["backup_elapsed_ns"].(int64) > 0) != backup ||
				(result["compact_copy_bytes"].(int64) > 0) != compact ||
				(result["compact_elapsed_ns"].(int64) > 0) != compact {
				t.Fatalf("incorrect separate maintenance copy accounting: %#v", result)
			}
			if (result["observed_maintenance_api_overlap_ns"].(int64) > 0) != (backup && compact) {
				t.Fatalf("missing or invented maintenance overlap: %#v", result)
			}
			if mode == pinnedBackupMode {
				pins := result["pinned_backup"].(pinnedBackupMeasurement)
				t.Logf("backup pin verification: %+v", pins)
				if pins.RetiredFilesHeld <= 16 || pins.RetiredBytesHeld != result["compact"].(engine.CompactResult).PinnedRetiredDataBytes ||
					pins.ReleasedFilesUnlinked != pins.RetiredFilesHeld || pins.ReleasedBytesUnlinked != pins.RetiredBytesHeld ||
					pins.BackupGateWaitNS <= 0 || pins.WorkerReclaimNS <= 0 {
					t.Fatalf("incomplete backup pin witness: %+v", pins)
				}
			}
			if backup && (result["backup_reopen_verified"] != true ||
				result["backup_last_user_sequence"] != result["restored_last_user_sequence"] ||
				result["restored_live_keys"] != len(keys)+1 || result["restored_state_sha256"] != result["fixture_state_sha256"]) {
				t.Fatalf("backup capture/full-state validation was incomplete: %#v", result)
			}
		})
	}
}

// The wire lengths give an independent oracle even when a checkpoint is a
// no-op and its old snapshots are still selected.
func TestMeasurementCountsOnlyNewMaintenanceOutput(t *testing.T) {
	hooks := &measurementHooks{}
	io, engineHooks := hooks.interferenceHooks()
	io.WriteAt = func(fd *os.File, data []byte, off int64) (int, error) {
		return fd.WriteAt(data[:min(len(data), 17)], off)
	}
	db := newTestDBWithHooks(t, engine.Options{}, io, engineHooks)
	mustPut(t, db, "k", "obsolete")
	mustPut(t, db, "k", "value")
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := int64(format.SegmentHeaderBytes + format.RecordHeaderBytes + len("k") + len("value"))
	if hooks.manualBytes.Load() != want || hooks.backupBytes.Load() != 0 {
		t.Fatalf("checkpoint wrote %d DATA bytes; want %d, backup %d", hooks.manualBytes.Load(), want, hooks.backupBytes.Load())
	}
	if _, err := db.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(context.Background(), filepath.Join(t.TempDir(), "backup")); err != nil {
		t.Fatal(err)
	}
	// Backup copies the checkpoint and the header of its empty active log.
	if hooks.manualBytes.Load() != want || hooks.backupBytes.Load() != want+format.SegmentHeaderBytes {
		t.Fatalf("no-op checkpoint or backup counted the wrong bytes: manual=%d backup=%d", hooks.manualBytes.Load(), hooks.backupBytes.Load())
	}
}

func TestMeasurementStateDigestRemainsStableAcrossIndexes(t *testing.T) {
	// Historical framing hashes each key/value length as a little-endian uint64
	// followed by its bytes. Ordering is lexicographic on the full binary key.
	const want = "2a6cccdae98dd72e8eb821527958fe667ceeb75e916fce456bf9267823e117c7"
	for _, mode := range []engine.IndexMode{engine.IndexMemory, engine.IndexBTree} {
		opts := engine.Options{IndexMode: mode}
		db := newTestDB(t, opts, nil)
		for _, kv := range []struct{ key, value string }{
			{"a\x00", "\x00"}, {"\xff", "\x01\xff"}, {"remove", "x"}, {"a", "old"}, {"\x00", ""}, {"a", "last"},
		} {
			if err := db.Put([]byte(kv.key), []byte(kv.value)); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Delete([]byte("remove")); err != nil {
			t.Fatal(err)
		}
		if sum, n := measurementState(t, db); sum != want || n != 4 {
			t.Fatalf("mode %d: digest %s, %d entries", mode, sum, n)
		}
		if _, err := db.Compact(t.Context()); err != nil {
			t.Fatal(err)
		}
		if sum, n := measurementState(t, db); sum != want || n != 4 {
			t.Fatalf("mode %d after compact: digest %s, %d entries", mode, sum, n)
		}
	}
}
