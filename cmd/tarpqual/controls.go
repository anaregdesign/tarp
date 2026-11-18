package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/anaregdesign/tarp/internal/qualification"
)

func hostedGuard(confirm string) error {
	if confirm != "disposable-vm-only" || runtime.GOOS != "linux" || runtime.GOARCH != "amd64" ||
		os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("RUNNER_ENVIRONMENT") != "github-hosted" ||
		os.Getenv("RUNNER_OS") != "Linux" || os.Getenv("RUNNER_ARCH") != "X64" {
		return errors.New("qualification: destructive guest controls require explicit opt-in on a disposable hosted Linux amd64 runner")
	}
	return nil
}

func blockControls(work, confirm string, output io.Writer) (result error) {
	if err := hostedGuard(confirm); err != nil {
		return err
	}
	work, err := filepath.Abs(work)
	if err != nil {
		return err
	}
	journal, err := os.OpenFile(filepath.Join(work, "evidence", "block-controls.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, journal.Close()) }()
	parent, err := os.Open(filepath.Dir(journal.Name()))
	if err != nil {
		return err
	}
	if err := errors.Join(journal.Sync(), parent.Sync(), parent.Close()); err != nil {
		return err
	}
	record := func(event any) error {
		if err := json.NewEncoder(journal).Encode(event); err != nil {
			return err
		}
		return journal.Sync()
	}
	diskPath := filepath.Join(work, "control-disk.raw")
	disk, err := qualification.CreateDisk(diskPath, qualification.MaxDisk)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, disk.Close()) }()
	if err := record(map[string]any{"event": "start", "fixture": "flush-volatile-v1", "size": disk.Size(),
		"flushed_byte": 0x71, "volatile_byte": 0xa3,
		"flushed_offset": 65536, "volatile_offset": 131072, "bytes_each": 512}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	vm, err := qualification.RunVM(ctx, qualification.VMConfig{
		BootDir: work, CaseDir: filepath.Join(work, "controls"), Input: filepath.Join(work, "input"),
		Serial: filepath.Join(work, "evidence", "block-serial.log"),
	}, disk, func(payload string) (bool, error) {
		if payload != "TARP_CONTROL_READY" {
			return false, fmt.Errorf("qualification: unexpected block-control event %q", payload)
		}
		if err := record(map[string]any{"event": "observed_ready", "stats": disk.Stats()}); err != nil {
			return false, err
		}
		return true, nil
	})
	if err := record(map[string]any{"event": "vm_result", "result": vm, "error": fmt.Sprint(err)}); err != nil {
		return err
	}
	if err != nil {
		return err
	}
	if !vm.Cut || vm.Before.Flushes == 0 || vm.Before.DirtyBytes == 0 || vm.After.DirtyBytes != 0 ||
		vm.After.DiscardedBytes == 0 {
		return fmt.Errorf("qualification: ineffective flush/volatile-cut controls: %+v", vm)
	}
	fd, err := os.Open(diskPath)
	if err != nil {
		return err
	}
	flushed, volatile := make([]byte, 512), make([]byte, 512)
	_, firstErr := fd.ReadAt(flushed, 65536)
	_, secondErr := fd.ReadAt(volatile, 131072)
	if err := errors.Join(firstErr, secondErr, fd.Close()); err != nil {
		return err
	}
	if !bytes.Equal(flushed, bytes.Repeat([]byte{0x71}, 512)) || !bytes.Equal(volatile, make([]byte, 512)) {
		return errors.New("qualification: independently read backing file violates flushed-survives/unflushed-is-discarded")
	}
	verdict := map[string]any{
		"event": "control_verdict", "flushed_survives": true, "unflushed_is_discarded": true,
		"guest_power_cut": true, "power_loss_qualification": "not_run", "stats": vm,
	}
	if err := record(verdict); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(verdict)
}
