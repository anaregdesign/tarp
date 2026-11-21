package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/benchmark/qualification"
)

func TestVMBlockControls(t *testing.T) {
	if os.Getenv("TARP_VM_BLOCK_CONTROL") != "disposable-vm-only" {
		t.Skip("requires explicit disposable guest block-control opt-in")
	}
	guesttest.RequireDisposableGuest(t)
	buffer, err := syscall.Mmap(-1, 0, 4096, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := os.OpenFile("/dev/vdb", os.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range buffer[:512] {
		buffer[i] = 0x71
	}
	if n, err := fd.WriteAt(buffer[:512], 65536); err != nil || n != 512 {
		t.Fatalf("flushed control write: %d %v", n, err)
	}
	if err := fd.Sync(); err != nil {
		t.Fatal(err)
	}
	for i := range buffer[:512] {
		buffer[i] = 0xa3
	}
	if n, err := fd.WriteAt(buffer[:512], 131072); err != nil || n != 512 {
		t.Fatalf("volatile control write: %d %v", n, err)
	}
	fmt.Println("TARP_CONTROL_READY")
	// Keep the raw descriptor open: closing the last block-device handle can
	// itself flush. Only the external controller terminates this whole VM.
	for {
		time.Sleep(time.Hour)
	}
}

func TestVMCampaignGuest(t *testing.T) {
	if os.Getenv("TARP_VM_CAMPAIGN_CONFIRM") != "disposable-vm-only" {
		t.Skip("requires explicit disposable guest campaign opt-in")
	}
	guesttest.RequireDisposableGuest(t)
	var cfg qualification.PowerCase
	data, err := os.ReadFile("/mnt/tarp-input/case.json")
	if err != nil || len(data) > 16384 {
		t.Fatalf("read bounded case configuration: %v", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	cases, err := qualification.CampaignCases("all")
	if err != nil {
		t.Fatal(err)
	}
	known := false
	for _, candidate := range cases {
		known = known || candidate.Name == cfg.Name && candidate.Mode == cfg.Mode &&
			candidate.Phase == cfg.Phase && candidate.Occurrence == cfg.Occurrence
	}
	if !known || cfg.Action != "crash" && cfg.Action != "verify" {
		t.Fatal("case is not in the bounded phase matrix")
	}
	guesttest.MountTarget(t, cfg.Action == "crash", cfg.Name, cfg.Seed)
	dir, output := "/mnt/tarp-data/db", "/mnt/tarp-data/output"
	if cfg.Action == "crash" {
		if cfg.Mode == "exhaustion" {
			vmExhaustion(t, dir, output, cfg.Seed)
			t.Fatal("external exhaustion barrier returned")
		}
		for key, value := range map[string]string{
			"TARP_POWERCUT_CONFIRM": "disposable-vm-only", "TARP_POWERCUT_DIR": dir,
			"TARP_POWERCUT_POINT": cfg.Phase, "TARP_POWERCUT_MODE": cfg.Mode,
			"TARP_POWERCUT_OUTPUT": output, "TARP_POWERCUT_N": strconv.Itoa(cfg.Occurrence),
			"TARP_POWERCUT_SEED": strconv.FormatUint(cfg.Seed, 10),
		} {
			t.Setenv(key, value)
		}
		TestPowerCutGuest(t)
		t.Fatal("external phase barrier returned")
	}
	verifyVMCampaign(t, cfg, dir, output)
}

func guestOracle(t *testing.T, name string, allowEmpty bool) *qualification.Oracle {
	t.Helper()
	fd, err := os.Open(filepath.Join("/mnt/tarp-input", name))
	if err != nil {
		t.Fatal(err)
	}
	events, err := qualification.ReadOracle(fd)
	if err := errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	oracle, err := qualification.ReplayOracle(events, allowEmpty)
	if err != nil {
		t.Fatal(err)
	}
	return oracle
}

func verifyVMDatabase(t *testing.T, db *DB, oracle *qualification.Oracle, mode string) string {
	t.Helper()
	lo, hi := oracle.PowerCutSequenceRange(mode)
	seq := Observe(db).Frontier().Sequence
	if seq < lo || seq > hi {
		t.Fatalf("recovered sequence %d outside acknowledged/pending interval [%d,%d]", seq, lo, hi)
	}
	digest, err := oracle.VerifyAt(seq, Observe(db).LiveKeys(), func(key []byte) ([]byte, bool, error) {
		value, err := db.Get(key)
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		return value, err == nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func verifyVMCampaign(t *testing.T, cfg qualification.PowerCase, dir, output string) {
	t.Helper()
	_, currentErr := os.Stat(filepath.Join(dir, "CURRENT"))
	missingCurrent := errors.Is(currentErr, os.ErrNotExist)
	required := cfg.Mode != "init" || cfg.Phase == "current.dirsync:after" || cfg.Phase == "operation.ack:after"
	if required && currentErr != nil {
		t.Fatalf("committed source frontier is absent: %v", currentErr)
	}
	// No oracle is read until recovery has returned; it cannot reconstruct data.
	db, err := Open(dir, vmCaseOptions(cfg.Mode))
	if err != nil {
		if cfg.Mode == "init" && !required && missingCurrent && errors.Is(err, ErrCorrupt) {
			reportVMRecovery(t, qualification.RecoveryVerdict{Source: "explicit_uninitialized", Output: "not_applicable"})
			return
		}
		t.Fatalf("file-only source recovery: %v", err)
	}
	oracle := guestOracle(t, "oracle.jsonl", cfg.Mode == "init")
	digest := verifyVMDatabase(t, db, oracle, cfg.Mode)
	sequence := Observe(db).Frontier().Sequence
	liveKeys := Observe(db).LiveKeys()
	if cfg.Mode == "init" && liveKeys != 0 {
		t.Fatal("initialization invented a logical history")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	outputState := "not_applicable"
	if cfg.Mode == "backup" || cfg.Mode == "restore" || cfg.Mode == "overlap" || cfg.Mode == "exhaustion" {
		capture := guestOracle(t, "capture-oracle.jsonl", false)
		check := func(path string) {
			db, err := Open(path, vmCaseOptions(cfg.Mode))
			if err != nil {
				t.Fatal(err)
			}
			if seq := Observe(db).Frontier().Sequence; seq != cfg.CaptureSequence {
				t.Fatalf("snapshot sequence %d != capture %d", seq, cfg.CaptureSequence)
			}
			verifyVMDatabase(t, db, capture, cfg.Mode)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if cfg.Mode == "restore" {
			path := filepath.Join(filepath.Dir(dir), "source-validated")
			if err := Restore(context.Background(), RestoreRequest{
				Source:      dir + "-restore-source",
				Destination: path,
				Limits:      testOptions().RestoreLimits(),
			}); err != nil {
				t.Fatalf("acknowledged restore-source backup was lost: %v", err)
			}
			check(path)
		}
		if _, err := os.Stat(output); err != nil {
			if !errors.Is(err, os.ErrNotExist) || cfg.RequireOutput {
				t.Fatalf("required snapshot output missing: %v", err)
			}
			outputState = "permitted_absent"
		} else {
			path := output
			if cfg.Mode != "restore" {
				path = filepath.Join(filepath.Dir(dir), "output-validated")
				if err := Restore(context.Background(), RestoreRequest{
					Source:      output,
					Destination: path,
					Limits:      vmCaseOptions(cfg.Mode).RestoreLimits(),
				}); err != nil {
					t.Fatalf("published incomplete backup: %v", err)
				}
			}
			check(path)
			outputState = "validated_at_capture"
		}
	}
	if cfg.Mode == "exhaustion" {
		opts := vmCaseOptions("exhaustion")
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		const key, value = "post-enospc-probe", "new-durable-value"
		if got := Observe(db).Options(); got.IndexMode != IndexBTree || got.BTreeMemoryBudgetBytes != 16<<20 {
			t.Fatalf("unexpected reopened index configuration: %+v", got)
		}
		if _, err := db.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
			t.Fatalf("post-exhaustion probe unexpectedly exists: %v", err)
		}
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("post-exhaustion durable Put: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		got, err := db.Get([]byte(key))
		after := Observe(db)
		if err != nil || string(got) != value || after.Frontier().Sequence != sequence+1 || after.LiveKeys() != liveKeys+1 {
			t.Fatalf("post-exhaustion Put after reopen: value=%x err=%v sequence=%d keys=%d", got, err, after.Frontier().Sequence, after.LiveKeys())
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("post-exhaustion B+tree cold reboot, durable Put, Close and reopen verified: key=%s sequence=%d live_keys=%d\n", key, sequence+1, liveKeys+1)
	}
	reportVMRecovery(t, qualification.RecoveryVerdict{
		Source: "file_only", SourceSequence: sequence, SourceLiveKeys: liveKeys, StateSHA256: digest,
		Output: outputState, CaptureSequence: cfg.CaptureSequence,
	})
}

func reportVMRecovery(t *testing.T, verdict qualification.RecoveryVerdict) {
	t.Helper()
	data, err := json.Marshal(verdict)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("TARP_RECOVERY_PASS %s\n", data)
}
