package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/anaregdesign/tarp/benchmark/qualification"
)

func campaign(work, confirm, group, selected string, output io.Writer) (result error) {
	if err := hostedGuard(confirm); err != nil {
		return err
	}
	cases, err := qualification.CampaignCases(group)
	if err != nil {
		return err
	}
	if selected != "" {
		var matches []qualification.PowerCase
		for _, candidate := range cases {
			if candidate.Name == selected {
				matches = append(matches, candidate)
			}
		}
		if len(matches) != 1 {
			return errors.New("qualification: selected case is not in this bounded group")
		}
		cases = matches
	}
	work, err = filepath.Abs(work)
	if err != nil {
		return err
	}
	if err := os.Mkdir(filepath.Join(work, "cases"), 0700); err != nil {
		return err
	}
	log, err := createLog(filepath.Join(work, "evidence", "campaign.jsonl"))
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, log.close()) }()
	if err := log.record(map[string]any{"event": "campaign_start", "group": group, "go": runtime.Version(),
		"cases": cases, "max_device_bytes": qualification.MaxDisk, "max_vm_seconds": 240}); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Minute)
	defer cancel()
	for i, cfg := range cases {
		if err := campaignCase(ctx, work, i, cfg, log); err != nil {
			raw := filepath.Join(work, "campaign-disk.raw")
			preserveErr := os.Rename(raw, filepath.Join(work, "evidence", "failed-disk.raw"))
			if errors.Is(preserveErr, os.ErrNotExist) {
				preserveErr = nil
			}
			return errors.Join(fmt.Errorf("qualification: case %s: %w", cfg.Name, err), preserveErr)
		}
	}
	exhaustion := "not_run"
	for _, cfg := range cases {
		if cfg.Mode == "exhaustion" {
			exhaustion = "pass"
		}
	}
	verdict := map[string]any{"event": "campaign_pass", "group": group, "go": runtime.Version(),
		"passed_cases": len(cases), "power_cuts": len(cases), "device_exhaustion": exhaustion,
		"qualified_measurements": "not_run"}
	if err := log.record(verdict); err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(verdict)
}

func campaignCase(parent context.Context, work string, index int, cfg qualification.PowerCase, log *durableLog) (result error) {
	if err := log.record(map[string]any{"event": "case_start", "index": index, "case": cfg}); err != nil {
		return err
	}
	base := fmt.Sprintf("%03d", index)
	path := filepath.Join(work, "campaign-disk.raw")
	disk, err := qualification.CreateDisk(path, qualification.MaxDisk)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, disk.Close())
		}
	}()
	evidence, input := filepath.Join(work, "evidence"), filepath.Join(work, "input")
	oraclePath := filepath.Join(evidence, base+"-oracle.jsonl")
	oracle, err := createLog(oraclePath)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, oracle.close()) }()
	if err := writeInput(filepath.Join(input, "case.json"), cfg); err != nil {
		return err
	}
	var events []qualification.OracleEvent
	var sequenceMin, sequenceMax uint64
	var witnessed *qualification.Oracle
	exhausted := false
	environment, phase, readerSeen := false, false, false
	observe := func(payload string) (bool, error) {
		if payload == "TARP_ENVIRONMENT_PASS" {
			if environment {
				return false, errors.New("duplicate target environment marker")
			}
			environment = true
			return false, log.record(map[string]any{"event": "guest_environment", "case": cfg.Name})
		}
		if !environment {
			return false, errors.New("oracle event preceded the validated target environment")
		}
		if strings.HasPrefix(payload, "TARP_EXHAUSTION_PASS ") {
			var verdict qualification.ExhaustionVerdict
			if cfg.Mode != "exhaustion" || exhausted {
				return false, errors.New("unexpected or repeated exhaustion verdict")
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(payload, "TARP_EXHAUSTION_PASS ")), &verdict); err != nil {
				return false, err
			}
			if err := verdict.Validate(); err != nil {
				return false, err
			}
			exhausted, cfg.RequireOutput = true, true
			return false, log.record(map[string]any{"event": "exhaustion_verdict", "case": cfg.Name, "verdict": verdict})
		}
		event, recognized, err := qualification.ParseOracle(payload)
		if err != nil || !recognized {
			return false, errors.Join(err, fmt.Errorf("unrecognized campaign payload %q", payload))
		}
		if event.Type == "operation_ack" {
			if event.Mode != cfg.Mode {
				return false, errors.New("operation acknowledgment does not match the selected mode")
			}
			if cfg.Mode == "backup" || cfg.Mode == "restore" || cfg.Mode == "overlap" {
				cfg.RequireOutput = true
			}
		}
		events = append(events, event)
		if err := oracle.record(event); err != nil {
			return false, err
		}
		if event.Type == "reads" {
			readerSeen = event.Count > 0
		}
		if event.Type == "capture" {
			if cfg.Captured {
				return false, errors.New("duplicate snapshot capture")
			}
			cfg.Captured, cfg.CaptureSequence = true, event.Sequence
			prefix := append(append([]qualification.OracleEvent{}, events...), qualification.OracleEvent{Type: "phase", Phase: "capture"})
			if _, err := qualification.ReplayOracle(prefix, false); err != nil {
				return false, err
			}
			capture, err := createLog(filepath.Join(evidence, base+"-capture.jsonl"))
			if err != nil {
				return false, err
			}
			for _, entry := range prefix {
				if err := capture.record(entry); err != nil {
					return false, errors.Join(err, capture.close())
				}
			}
			if err := capture.close(); err != nil {
				return false, err
			}
		}
		if event.Type != "phase" {
			return false, nil
		}
		if event.Phase != cfg.Phase || cfg.Mode == "overlap" && !readerSeen || cfg.Mode == "exhaustion" && !exhausted {
			return false, errors.New("phase or concurrent-reader coverage does not match the selected case")
		}
		model, err := qualification.ReplayOracle(events, cfg.Mode == "init")
		if err != nil {
			return false, err
		}
		sequenceMin, sequenceMax = model.PowerCutSequenceRange(cfg.Mode)
		witnessed = model
		if cfg.Mode == "init" && (sequenceMin != 0 || sequenceMax != 0) {
			return false, errors.New("initialization emitted unexpected logical mutations")
		}
		phase = true
		return true, log.record(map[string]any{"event": "witness_before_cut", "case": cfg, "stats": disk.Stats()})
	}
	ctx, cancel := context.WithTimeout(parent, 240*time.Second)
	cut, err := qualification.RunVM(ctx, qualification.VMConfig{
		BootDir: work, CaseDir: filepath.Join(work, "cases", base+"a"), Input: input,
		Serial: filepath.Join(evidence, base+"-cut.log"),
	}, disk, observe)
	cancel()
	if err := errors.Join(err, log.record(map[string]any{"event": "cut_result", "case": cfg, "vm": cut})); err != nil {
		return err
	}
	if !phase || !cut.Cut {
		return errors.New("selected externally witnessed whole-guest cut did not occur")
	}
	if err := replaceInputLink(oraclePath, filepath.Join(input, "oracle.jsonl")); err != nil {
		return err
	}
	if cfg.Captured {
		if err := replaceInputLink(filepath.Join(evidence, base+"-capture.jsonl"), filepath.Join(input, "capture-oracle.jsonl")); err != nil {
			return err
		}
	}
	if cfg.Phase == "backup.parent.sync:after" || cfg.Phase == "restore.parent.sync:after" {
		cfg.RequireOutput = true
	}
	cfg.Action = "verify"
	if err := writeInput(filepath.Join(input, "case.json"), cfg); err != nil {
		return err
	}
	recovered, environment := false, false
	ctx, cancel = context.WithTimeout(parent, 240*time.Second)
	reboot, err := qualification.RunVM(ctx, qualification.VMConfig{
		BootDir: work, CaseDir: filepath.Join(work, "cases", base+"b"), Input: input,
		Serial: filepath.Join(evidence, base+"-recovery.log"),
	}, disk, func(payload string) (bool, error) {
		if payload == "TARP_ENVIRONMENT_PASS" && !environment {
			environment = true
			return false, nil
		}
		const prefix = "TARP_RECOVERY_PASS "
		if !environment || recovered || !strings.HasPrefix(payload, prefix) {
			return false, fmt.Errorf("unexpected recovery payload %q", payload)
		}
		var verdict qualification.RecoveryVerdict
		if err := json.Unmarshal([]byte(strings.TrimPrefix(payload, prefix)), &verdict); err != nil {
			return false, err
		}
		switch verdict.Source {
		case "file_only":
			digest, err := hex.DecodeString(verdict.StateSHA256)
			if err != nil || len(digest) != 32 || verdict.SourceSequence < sequenceMin || verdict.SourceSequence > sequenceMax {
				return false, errors.New("invalid file-only recovery digest/sequence verdict")
			}
			expected, count, err := witnessed.DigestAt(verdict.SourceSequence)
			if err != nil || expected != verdict.StateSHA256 || count != verdict.SourceLiveKeys {
				return false, errors.New("recovery content/cardinality is not the witnessed state at its actual sequence")
			}
		case "explicit_uninitialized":
			if cfg.Mode != "init" || cfg.Phase == "current.dirsync:after" || cfg.Phase == "operation.ack:after" ||
				sequenceMin != 0 || sequenceMax != 0 || verdict.SourceSequence != 0 {
				return false, errors.New("uninitialized outcome would discard a required committed frontier")
			}
		default:
			return false, errors.New("unknown recovery verdict")
		}
		if cfg.RequireOutput && verdict.Output != "validated_at_capture" ||
			cfg.Captured && (verdict.CaptureSequence != cfg.CaptureSequence ||
				verdict.Output != "validated_at_capture" && verdict.Output != "permitted_absent") ||
			!cfg.Captured && verdict.Output != "not_applicable" {
			return false, errors.New("snapshot output/capture verdict does not match the witnessed contract")
		}
		recovered = true
		return false, log.record(map[string]any{"event": "recovery_verdict", "case": cfg, "verdict": verdict})
	})
	cancel()
	if err := errors.Join(err, log.record(map[string]any{"event": "reboot_result", "case": cfg.Name, "vm": reboot, "stats": disk.Stats()})); err != nil {
		return err
	}
	if !environment || !recovered || reboot.Cut {
		return errors.New("file-only cold reboot/restore verification did not finish")
	}
	if err := disk.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if err := log.record(map[string]any{"event": "case_pass", "case": cfg.Name}); err != nil {
		return err
	}
	return os.Remove(path)
}
