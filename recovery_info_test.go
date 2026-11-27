package tarp_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestRecoveryInfoReportsPathAndRemainsImmutable(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := tarp.Options{OpenValidation: tarp.Indexed}
	db, err := tarp.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	headerBytes := db.Stats().SelectedDataBytes
	if info := db.RecoveryInfo(); info.Path != tarp.RecoveryCreated || info.Reason != tarp.RecoveryNoReason ||
		info.LastUserSequence != 0 || info.CheckpointID != 0 || info.ReplayBudgetBytes == 0 {
		t.Fatalf("created recovery report = %+v", info)
	}
	if err := db.Put([]byte("key"), []byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = tarp.OpenExisting(ctx, dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	info := db.RecoveryInfo()
	if info.Path != tarp.RecoveryFastCheckpointReplay || info.Reason != tarp.RecoveryNoReason ||
		info.LastUserSequence != 1 || info.CheckpointID == 0 || info.CoveredSequence != 1 ||
		info.ChangedDataBytes != headerBytes || !info.ChangedDataBytesExact || info.ReplayBudgetBytes == 0 {
		t.Fatalf("fast recovery report = %+v; want %d changed bytes", info, headerBytes)
	}
	if err := db.Put([]byte("key"), []byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if after := db.RecoveryInfo(); after != info {
		t.Fatalf("recovery report changed after checkpoint and Close: %+v, want %+v", after, info)
	}

	full, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.BytesCodec{}, tarp.BytesCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	if info := full.RecoveryInfo(); info.Path != tarp.RecoveryCheckpointReplay || info.Reason != tarp.RecoveryFullValidation ||
		info.LastUserSequence != 2 || info.CheckpointID == 0 || info.CoveredSequence != 2 ||
		info.ChangedDataBytes != headerBytes || !info.ChangedDataBytesExact || info.ReplayBudgetBytes != 0 {
		t.Fatalf("Full recovery report = %+v; want %d changed bytes", info, headerBytes)
	}
}

func TestRecoveryInfoReportsFallbackCauses(t *testing.T) {
	ctx := t.Context()
	for _, test := range []struct {
		name   string
		reason tarp.RecoveryReason
		alter  func(string) error
	}{
		{"absent descriptor", tarp.RecoveryNoCheckpoint, nil},
		{"missing file", tarp.RecoveryCheckpointMissing, os.Remove},
		{"unusable file", tarp.RecoveryCheckpointUnusable, func(path string) error { return os.Truncate(path, 0) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			var opts tarp.Options
			if test.alter == nil {
				opts.IndexMode = tarp.IndexMemory
			}
			db, err := tarp.Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Put([]byte("key"), []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if test.alter != nil {
				files, err := filepath.Glob(filepath.Join(dir, "INDEX-*"))
				if err != nil || len(files) == 0 {
					t.Fatalf("selected INDEX files = %v, %v", files, err)
				}
				for _, file := range files {
					if err := test.alter(file); err != nil {
						t.Fatal(err)
					}
				}
			}
			db, err = tarp.OpenExisting(ctx, dir, tarp.Options{OpenValidation: tarp.Indexed})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			info := db.RecoveryInfo()
			if info.Path != tarp.RecoveryFullReplay || info.Reason != test.reason || info.LastUserSequence != 1 ||
				info.ChangedDataBytes != 0 || info.ChangedDataBytesExact || info.ReplayBudgetBytes == 0 {
				t.Fatalf("recovery report = %+v, want fallback %q", info, test.reason)
			}
			if test.alter == nil && (info.CheckpointID != 0 || info.CoveredSequence != 0) ||
				test.alter != nil && (info.CheckpointID == 0 || info.CoveredSequence != 1) {
				t.Fatalf("checkpoint selection was not reported accurately: %+v", info)
			}
			if value, err := db.Get([]byte("key")); err != nil || string(value) != "value" {
				t.Fatalf("recovered Get = %q, %v", value, err)
			}
		})
	}
}

func TestRecoveryInfoReportsExactChangedDataOnOptionFallback(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRecoveryInfoOptionChild$")
	cmd.Env = append(os.Environ(), "TARP_RECOVERY_INFO_CHILD_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create checkpoint and suffix: %v\n%s", err, output)
	}
	db, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{OpenValidation: tarp.Indexed, MaxRecordBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	info := db.RecoveryInfo()
	if info.Path != tarp.RecoveryFullReplay || info.Reason != tarp.RecoveryOptionLimits ||
		info.LastUserSequence != 2 || info.CheckpointID == 0 || info.CoveredSequence != 1 ||
		info.ChangedDataBytes == 0 || !info.ChangedDataBytesExact {
		t.Fatalf("options fallback recovery report = %+v", info)
	}
	if db.Stats().LiveKeys != 0 {
		t.Fatalf("authoritative fallback restored deleted key: %+v", db.Stats())
	}
}

func TestRecoveryInfoOptionChild(t *testing.T) {
	dir := os.Getenv("TARP_RECOVERY_INFO_CHILD_DIR")
	if dir == "" {
		return
	}
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("large"), make([]byte, 4<<10)); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointIndex(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("large")); err != nil {
		t.Fatal(err)
	}
	// Leave the selected checkpoint at the earlier commit without Close refresh.
	os.Exit(0)
}
