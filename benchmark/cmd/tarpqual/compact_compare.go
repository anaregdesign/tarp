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

type compactTrial struct {
	Arm   string `json:"arm"`
	Mode  string `json:"mode"`
	Round int    `json:"round"`
}

// Check the schedule independently of the guest process. Each measured pair
// uses the same control, and both control order and release order rotate.
func compactTrials() []compactTrial {
	trials := []compactTrial{{"base", "seed", 0}}
	for round := 1; round <= 6; round++ {
		arms, modes := [2]string{"base", "next"}, [2]string{"overlap", "parked"}
		if round%2 == 0 {
			arms, modes = [2]string{"next", "base"}, [2]string{"parked", "overlap"}
		}
		for _, mode := range modes {
			for _, arm := range arms {
				trials = append(trials, compactTrial{arm, mode, round})
			}
		}
	}
	trials = append(trials, compactTrial{"base", "baseline", 0}, compactTrial{"next", "baseline", 0},
		compactTrial{"next", "pinned", 0}, compactTrial{"base", "pinned", 0})
	for round := 1; round <= 4; round++ {
		arms := [2]string{"base", "next"}
		if round%2 == 0 {
			arms = [2]string{"next", "base"}
		}
		for _, arm := range arms {
			trials = append(trials, compactTrial{arm, "diagnostic", round})
		}
	}
	return trials
}

func compactComparison(work, confirm string, output io.Writer) (result error) {
	if err := hostedGuard(confirm); err != nil {
		return err
	}
	work, err := filepath.Abs(work)
	if err != nil {
		return err
	}
	evidence := filepath.Join(work, "evidence")
	log, err := createLog(filepath.Join(evidence, "compact-compare.jsonl"))
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, log.close()) }()
	diskPath := filepath.Join(work, "compact-compare.raw")
	disk, err := qualification.CreateDisk(diskPath, qualification.MaxDisk)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, disk.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	schedule := compactTrials()
	rows := 0
	environment, passed := false, false
	vm, err := qualification.RunVM(ctx, qualification.VMConfig{
		BootDir: work, CaseDir: filepath.Join(work, "compare-vm"), Input: filepath.Join(work, "input"),
		Serial: filepath.Join(evidence, "compact-compare-serial.log"),
	}, disk, func(payload string) (bool, error) {
		if payload == "TARP_ENVIRONMENT_PASS" && !environment {
			environment = true
			return false, nil
		}
		if !environment || passed {
			return false, errors.New("compact comparison event outside active guest")
		}
		if payload == "TARP_COMPARE_PASS" && rows == len(schedule) {
			passed = true
			return false, nil
		}
		data, ok := strings.CutPrefix(payload, "TARP_COMPARE ")
		if !ok || rows == len(schedule) {
			return false, fmt.Errorf("unexpected compact comparison event at row %d: %.96s", rows, payload)
		}
		// Decode identities with an invalid default so an omitted round 0
		// cannot pass as a seed or an untimed control.
		trial := compactTrial{Round: -1}
		if err := json.Unmarshal([]byte(data), &trial); err != nil {
			return false, err
		}
		if trial != schedule[rows] {
			return false, fmt.Errorf("compact comparison row %d: got %+v, want %+v", rows, trial, schedule[rows])
		}
		rows++
		return false, log.record(json.RawMessage(data))
	})
	if err != nil {
		return err
	}
	if !passed || vm.Cut || disk.Stats().Flushes == 0 {
		return errors.New("compact comparison did not complete on the disposable block device")
	}
	if err := os.Remove(diskPath); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(map[string]any{
		"compact_comparison": "pass", "rows": rows, "paired_rounds": 6, "diagnostic_rounds": 4, "value_bytes": 64,
		"vm": vm, "device": disk.Stats(),
	})
}
