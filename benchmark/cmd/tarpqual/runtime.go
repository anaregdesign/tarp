package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anaregdesign/tarp/benchmark/qualification"
)

func runtimeSuite(work, confirm string, output io.Writer) (result error) {
	if err := hostedGuard(confirm); err != nil {
		return err
	}
	work, err := filepath.Abs(work)
	if err != nil {
		return err
	}
	fd, err := os.Open(filepath.Join(work, "input", "runtime.tsv"))
	if err != nil {
		return err
	}
	packages, readErr := qualification.RuntimeManifest(fd)
	if err := errors.Join(readErr, fd.Close()); err != nil {
		return err
	}
	log, err := createLog(filepath.Join(work, "evidence", "runtime.jsonl"))
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, log.close()) }()
	disk, err := qualification.CreateDisk(filepath.Join(work, "runtime-disk.raw"), qualification.MaxDisk)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, disk.Close()) }()
	environment, passed := false, false
	ctx, cancel := context.WithTimeout(context.Background(), 900*time.Second)
	defer cancel()
	vm, err := qualification.RunVM(ctx, qualification.VMConfig{
		BootDir: work, CaseDir: filepath.Join(work, "runtime-vm"), Input: filepath.Join(work, "input"),
		Serial: filepath.Join(work, "evidence", "runtime-serial.log"),
	}, disk, func(payload string) (bool, error) {
		if payload == "TARP_ENVIRONMENT_PASS" && !environment {
			environment = true
			return false, log.record(map[string]string{"event": "runtime_environment"})
		}
		const prefix = "TARP_RUNTIME_PASS "
		if !environment || passed || !strings.HasPrefix(payload, prefix) {
			return false, fmt.Errorf("unexpected runtime payload %q", payload)
		}
		var verdict qualification.RuntimeVerdict
		if err := json.Unmarshal([]byte(strings.TrimPrefix(payload, prefix)), &verdict); err != nil {
			return false, err
		}
		if len(verdict.Packages) != len(packages) || verdict.FuzzTarget != "FuzzDecoders" ||
			verdict.FuzzSeconds != 15 || !verdict.FuzzCoverageGuide || verdict.FuzzExecutions == 0 {
			return false, errors.New("incomplete selected-target full-suite/fuzz verdict")
		}
		for i, entry := range verdict.Packages {
			if entry.Package != packages[i].Package || entry.Binary != packages[i].Binary ||
				entry.SHA256 != packages[i].SHA256 || entry.ElapsedNS <= 0 {
				return false, errors.New("selected-target suite differs from the external full-package manifest")
			}
		}
		passed = true
		return false, log.record(map[string]any{"event": "runtime_pass", "verdict": verdict})
	})
	if err := errors.Join(err, log.record(map[string]any{"event": "runtime_vm", "vm": vm, "device": disk.Stats()})); err != nil {
		return err
	}
	if !environment || !passed || vm.Cut {
		return errors.New("selected-target full runtime suite did not complete successfully")
	}
	return json.NewEncoder(output).Encode(map[string]any{"runtime_suite": "pass", "packages": len(packages), "fuzz_seconds": 15})
}
