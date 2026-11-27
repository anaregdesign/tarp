package tarp_test

import (
	"bytes"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func refreshTestOptions() tarp.Options {
	return tarp.Options{OpenValidation: tarp.Indexed, TargetSegmentBytes: 32 << 10, MaxRecordBytes: 16 << 10}
}

func refreshTestAnchor() (string, string) {
	return "anchor\x00" + strings.Repeat("long-key", 128), strings.Repeat("base value", 1200)
}

func refreshTestIndexFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "INDEX-*"))
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func refreshTestRead(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRefreshIndexReopensLayeredCheckpointAndReplaysTail(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRefreshIndexLayerChild$")
	cmd.Env = append(os.Environ(), "TARP_REFRESH_INDEX_LAYER_CHILD="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create layered checkpoint and tail: %v\n%s", err, output)
	}

	// Capture the stopped image before opening it. Removing the original base
	// from another copy proves that the latest selection needs its retained
	// ancestor and that recovery can still use authoritative DATA.
	baseMissing := t.TempDir()
	inspectInventory(t, dir, baseMissing)
	files := refreshTestIndexFiles(t, baseMissing)
	if len(files) != 3 {
		t.Fatalf("expected one base and two incremental layers, got %v", files)
	}
	if err := os.Remove(files[0]); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name string
		dir  string
		path tarp.RecoveryPath
	}{
		{"complete chain", dir, tarp.RecoveryFastCheckpointReplay},
		{"missing base", baseMissing, tarp.RecoveryFullReplay},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := tarp.OpenExisting(t.Context(), test.dir, refreshTestOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			info := db.RecoveryInfo()
			if info.Path != test.path || info.LastUserSequence != 10 ||
				info.CoveredSequence != 9 || info.CheckpointID == 0 {
				t.Fatalf("recovery report = %+v, want path %q and 9 of 10 records covered", info, test.path)
			}
			if test.path == tarp.RecoveryFastCheckpointReplay {
				if files := refreshTestIndexFiles(t, test.dir); len(files) != 3 {
					t.Fatalf("Open retired a selected checkpoint ancestor: %v", files)
				}
			}
			anchorKey, anchorValue := refreshTestAnchor()
			want := map[string]string{
				anchorKey: anchorValue, "restored": "reinserted", "replace": "second",
				"tail": "after refresh", "zeta": "fresh",
			}
			keys := slices.Sorted(maps.Keys(want))
			rows := 0
			for entry, err := range db.ScanRange(t.Context(), nil, nil) {
				if err != nil {
					t.Fatal(err)
				}
				if rows >= len(keys) || string(entry.Key) != keys[rows] || string(entry.Value) != want[keys[rows]] {
					t.Fatalf("ordered scan row %d = key %q value length %d", rows, entry.Key, len(entry.Value))
				}
				rows++
			}
			if rows != len(keys) {
				t.Fatalf("ordered scan yielded %d rows, want %d", rows, len(keys))
			}
			if _, err := db.Get([]byte("gone")); !errors.Is(err, tarp.ErrNotFound) {
				t.Fatalf("deleted key Get = %v", err)
			}
		})
	}
}

func TestRefreshIndexLayerChild(t *testing.T) {
	dir := os.Getenv("TARP_REFRESH_INDEX_LAYER_CHILD")
	if dir == "" {
		return
	}
	db, err := tarp.Open(dir, refreshTestOptions())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	put := func(key, value string) {
		t.Helper()
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	del := func(key string) {
		t.Helper()
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	anchorKey, anchorValue := refreshTestAnchor()
	put(anchorKey, anchorValue)
	put("restored", "old")
	put("replace", "old")
	if err := db.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	files := refreshTestIndexFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("initial INDEX files = %v", files)
	}
	base := refreshTestRead(t, files[0])

	del("restored")
	put("replace", "second")
	put("gone", "temporary")
	if err := db.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	del("gone")
	put("restored", "reinserted")
	put("zeta", "fresh")
	if err := db.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(base, refreshTestRead(t, files[0])) {
		t.Fatal("incremental refresh rewrote the base checkpoint")
	}

	currentName := filepath.Join(dir, "CURRENT")
	before := refreshTestRead(t, currentName)
	if err := db.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, refreshTestRead(t, currentName)) {
		t.Fatal("refresh of the current checkpoint republished CURRENT")
	}
	put("tail", "after refresh")
	// Keep the selected layered checkpoint and its tail as they are on disk.
	os.Exit(0)
}

func TestTypedRefreshIndexPublishesEncodedUpdates(t *testing.T) {
	dir := t.TempDir()
	ctx := t.Context()
	db, err := tarp.OpenTyped(dir, refreshTestOptions(), tarp.StringCodec{}, tarp.Uint16Codec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fresh, err := db.IndexCheckpointStatus(ctx)
	if err != nil || fresh.Reason != tarp.RecoveryNoCheckpoint || fresh.ChangedDataBytesExact ||
		fresh.LastUserSequence != 0 || fresh.ReplayBudgetBytes == 0 {
		t.Fatalf("status without a checkpoint = %+v, %v", fresh, err)
	}
	if err := db.Put("key", 100); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	base, err := db.IndexCheckpointStatus(ctx)
	if err != nil || base.CheckpointID == 0 || !base.ChangedDataBytesExact ||
		base.LastUserSequence != 1 || base.CoveredSequence != 1 || base.ChangedDataBytes == 0 {
		t.Fatalf("status after checkpoint = %+v, %v", base, err)
	}
	dataBytes := db.Stats().SelectedDataBytes
	if err := db.Put("key", 200); err != nil {
		t.Fatal(err)
	}
	tail, err := db.IndexCheckpointStatus(ctx)
	if err != nil || tail.CheckpointID != base.CheckpointID || !tail.ChangedDataBytesExact ||
		tail.LastUserSequence != 2 || tail.CoveredSequence != 1 ||
		tail.ChangedDataBytes != base.ChangedDataBytes+db.Stats().SelectedDataBytes-dataBytes {
		t.Fatalf("status after append = %+v, %v; base status %+v", tail, err, base)
	}
	if err := db.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	refreshed, err := db.IndexCheckpointStatus(ctx)
	if err != nil || refreshed.CheckpointID == base.CheckpointID || !refreshed.ChangedDataBytesExact ||
		refreshed.LastUserSequence != 2 || refreshed.CoveredSequence != 2 ||
		refreshed.ChangedDataBytes != base.ChangedDataBytes {
		t.Fatalf("status after refresh = %+v, %v; base status %+v", refreshed, err, base)
	}
	files := refreshTestIndexFiles(t, dir)
	if len(files) != 2 {
		t.Fatalf("typed refresh did not publish a layer: %v", files)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = tarp.OpenTyped(dir, refreshTestOptions(), tarp.StringCodec{}, tarp.Uint16Codec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if value, err := db.Get("key"); err != nil || value != 200 {
		t.Fatalf("typed Get = %d, %v", value, err)
	}
	if info := db.RecoveryInfo(); info.Path != tarp.RecoveryFastCheckpointReplay || info.CoveredSequence != 2 {
		t.Fatalf("typed recovery report = %+v", info)
	}
}
