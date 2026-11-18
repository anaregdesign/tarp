package tarp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/qualification"
)

func requireDisposableGuest(t *testing.T) {
	t.Helper()
	if os.Getenv("TARP_QUAL_GUEST_CONFIRM") != "disposable-vm-only" || runtime.GOARCH != "amd64" {
		t.Fatal("requires the explicitly guarded Linux amd64 guest")
	}
	initArgs, err := os.ReadFile("/proc/1/cmdline")
	if err != nil || !bytes.Equal(initArgs, []byte("/tarp-init\x00disposable-vm-only\x00")) {
		t.Fatalf("requires the explicitly guarded qualification PID 1: %v", err)
	}
	size, err := os.ReadFile("/sys/block/vdb/size")
	if err != nil || string(bytes.TrimSpace(size)) != "524288" {
		t.Fatalf("requires the disposable 256 MiB vdb: %v", err)
	}
}

func TestVMBlockControls(t *testing.T) {
	if os.Getenv("TARP_VM_BLOCK_CONTROL") != "disposable-vm-only" {
		t.Skip("requires explicit disposable guest block-control opt-in")
	}
	requireDisposableGuest(t)
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
	requireDisposableGuest(t)
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
	release, err := os.ReadFile("/etc/os-release")
	kernel, kernelErr := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil || kernelErr != nil || !bytes.Contains(release, []byte("\nID=ubuntu\n")) ||
		!bytes.Contains(release, []byte("\nVERSION_ID=\"24.04\"\n")) || !bytes.HasPrefix(kernel, []byte("6.8.")) {
		t.Fatalf("requires pinned Ubuntu 24.04 / 6.8 target: %v", errors.Join(err, kernelErr))
	}
	if cfg.Action == "crash" {
		guestCommand(t, "/usr/sbin/mkfs.ext4", "-q", "-F", "-b", "4096", "-m", "0",
			"-E", "lazy_itable_init=0,lazy_journal_init=0", "/dev/vdb")
	}
	if err := os.Mkdir("/mnt/tarp-data", 0700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mount("/dev/vdb", "/mnt/tarp-data", "ext4", 0, "data=ordered"); err != nil {
		t.Fatal(err)
	}
	mount, err := exec.Command("/usr/bin/findmnt", "-rn", "-o", "SOURCE,FSTYPE,OPTIONS", "/mnt/tarp-data").Output()
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(mount))
	if len(fields) != 3 || fields[0] != "/dev/vdb" || fields[1] != "ext4" ||
		!strings.Contains(","+fields[2]+",", ",data=ordered,") {
		t.Fatalf("unexpected selected mount: %s", mount)
	}
	for _, flag := range strings.Split(fields[2], ",") {
		if flag == "nobarrier" || flag == "barrier=0" || flag == "ro" || strings.HasPrefix(flag, "dax") {
			t.Fatalf("unsupported mount option %s", flag)
		}
	}
	fmt.Printf("Go=%s GOMAXPROCS=%d case=%s seed=%d\n%s", runtime.Version(), runtime.GOMAXPROCS(0), cfg.Name, cfg.Seed, mount)
	guestCommand(t, "/usr/bin/uname", "-a")
	guestCommand(t, "/usr/sbin/tune2fs", "-l", "/dev/vdb")
	fmt.Println("TARP_ENVIRONMENT_PASS")
	dir, output := "/mnt/tarp-data/db", "/mnt/tarp-data/output"
	if cfg.Action == "crash" {
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

func guestCommand(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
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

func verifyVMDatabase(t *testing.T, db *DB, oracle *qualification.Oracle) string {
	t.Helper()
	lo, hi := oracle.SequenceRange()
	if seq := db.store.Current.Sequence; seq < lo || seq > hi {
		t.Fatalf("recovered sequence %d outside acknowledged/pending interval [%d,%d]", seq, lo, hi)
	}
	digest, err := oracle.Verify(func(key []byte) ([]byte, bool, error) {
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
	db, err := Open(dir, testOptions())
	if err != nil {
		if cfg.Mode == "init" && !required && missingCurrent && errors.Is(err, ErrCorrupt) {
			reportVMRecovery(t, qualification.RecoveryVerdict{Source: "explicit_uninitialized", Output: "not_applicable"})
			return
		}
		t.Fatalf("file-only source recovery: %v", err)
	}
	oracle := guestOracle(t, "oracle.jsonl", cfg.Mode == "init")
	digest := verifyVMDatabase(t, db, oracle)
	sequence := db.store.Current.Sequence
	if cfg.Mode == "init" && len(db.index) != 0 {
		t.Fatal("initialization invented a logical history")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	outputState := "not_applicable"
	if cfg.Mode == "backup" || cfg.Mode == "restore" || cfg.Mode == "overlap" {
		capture := guestOracle(t, "capture-oracle.jsonl", false)
		check := func(path string) {
			db, err := Open(path, testOptions())
			if err != nil {
				t.Fatal(err)
			}
			if db.store.Current.Sequence != cfg.CaptureSequence {
				t.Fatalf("snapshot sequence %d != capture %d", db.store.Current.Sequence, cfg.CaptureSequence)
			}
			verifyVMDatabase(t, db, capture)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		}
		if cfg.Mode == "restore" {
			path := filepath.Join(filepath.Dir(dir), "source-validated")
			if err := Restore(context.Background(), dir+"-restore-source", path, testOptions()); err != nil {
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
				if err := Restore(context.Background(), output, path, testOptions()); err != nil {
					t.Fatalf("published incomplete backup: %v", err)
				}
			}
			check(path)
			outputState = "validated_at_capture"
		}
	}
	reportVMRecovery(t, qualification.RecoveryVerdict{
		Source: "file_only", SourceSequence: sequence, StateSHA256: digest,
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
