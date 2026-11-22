package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/anaregdesign/tarp/benchmark/qualification"
)

func measurementAck(input, name string) error {
	path := filepath.Join(input, name)
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return errors.Join(err, errors.New("measurement acknowledgment already exists"))
	}
	fd, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, writeErr := fd.WriteString("ready")
	if writeErr == nil && n != len("ready") {
		writeErr = io.ErrShortWrite
	}
	if err := errors.Join(writeErr, fd.Sync(), fd.Close()); err != nil {
		return err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}
	parent, err := os.Open(input)
	if err != nil {
		return err
	}
	return errors.Join(parent.Sync(), parent.Close())
}

func measurements(work, confirm string, size int, output io.Writer) (result error) {
	if err := hostedGuard(confirm); err != nil {
		return err
	}
	if !qualification.MeasurementSize(size) {
		return errors.New("qualification: measurements require 64, 1024 or 4096 byte values")
	}
	work, err := filepath.Abs(work)
	if err != nil {
		return err
	}
	log, err := createLog(filepath.Join(work, "evidence", "measurement.jsonl"))
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, log.close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Minute)
	defer cancel()
	for _, profile := range []bool{false, true} {
		cfg := qualification.MeasurementConfig{ValueBytes: size, Profile: profile}
		if err := measurementVM(ctx, work, cfg, log); err != nil {
			path := filepath.Join(work, fmt.Sprintf("measurement-%t.raw", profile))
			preserve := os.Rename(path, filepath.Join(work, "evidence", "failed-measurement.raw"))
			return errors.Join(err, preserve)
		}
	}
	return json.NewEncoder(output).Encode(map[string]any{
		"measurement_campaign": "pass", "value_bytes": size, "repetitions": 5,
		"primary_working_keys": 4096, "diagnostic_working_keys": 512,
		"cold_gets": 160, "cold_opens": 10, "history_mutations": 10240, "numeric_slo": "not_defined",
	})
}

func measurementVM(parent context.Context, work string, cfg qualification.MeasurementConfig, log *durableLog) (result error) {
	input := filepath.Join(work, "input")
	if err := writeInput(filepath.Join(input, "measurement.json"), cfg); err != nil {
		return err
	}
	if cfg.Profile {
		if err := measurementAck(input, "use-profile"); err != nil {
			return err
		}
	}
	path := filepath.Join(work, fmt.Sprintf("measurement-%t.raw", cfg.Profile))
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
	if err := log.record(map[string]any{"event": "measurement_start", "configuration": cfg}); err != nil {
		return err
	}
	rows, intervals, cold, coldOpens, warm := 0, 0, 0, 0, 0
	kinds := make(map[string]int)
	var activeKind string
	var before qualification.Stats
	environment, passed := false, false
	seconds := 1800
	if cfg.Profile {
		seconds = 900
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(seconds)*time.Second)
	defer cancel()
	vm, err := qualification.RunVM(ctx, qualification.VMConfig{
		BootDir: work, CaseDir: filepath.Join(work, fmt.Sprintf("measurement-%t-vm", cfg.Profile)), Input: input,
		Serial: filepath.Join(work, "evidence", fmt.Sprintf("measurement-%t-serial.log", cfg.Profile)),
	}, disk, func(payload string) (bool, error) {
		if payload == "TARP_ENVIRONMENT_PASS" && !environment {
			environment = true
			return false, log.record(map[string]any{"event": "measurement_environment", "configuration": cfg})
		}
		if !environment || passed {
			return false, errors.New("measurement event before environment or after terminal verdict")
		}
		if strings.HasPrefix(payload, "TARP_MEASUREMENT ") {
			var row qualification.MeasurementRow
			if err := json.Unmarshal([]byte(strings.TrimPrefix(payload, "TARP_MEASUREMENT ")), &row); err != nil {
				return false, err
			}
			if rows >= 4096 || row.ValueBytes != cfg.ValueBytes || row.Repetition < 0 || row.Repetition > 5 ||
				row.Kind == "" || len(row.Kind) > 96 || !json.Valid(row.Data) {
				return false, errors.New("invalid or unbounded measurement row")
			}
			rows++
			kinds[fmt.Sprintf("%s/%d", row.Kind, row.Repetition)]++
			return false, log.record(map[string]any{"event": "measurement_row", "profile": cfg.Profile, "row": row})
		}
		if strings.HasPrefix(payload, "TARP_DEVICE_") {
			fields := strings.Fields(payload)
			if cfg.Profile || len(fields) != 3 || intervals >= 175 ||
				fields[2] != "warm" && fields[2] != "cold" && fields[2] != "cold_open" {
				return false, errors.New("invalid device interval")
			}
			id, err := strconv.Atoi(fields[1])
			if err != nil || id != intervals {
				return false, errors.New("device interval sequence mismatch")
			}
			switch fields[0] {
			case "TARP_DEVICE_BEGIN":
				if activeKind != "" {
					return false, errors.New("overlapping device intervals")
				}
				activeKind, before = fields[2], disk.Stats()
				if err := log.record(map[string]any{"event": "device_interval_begin", "id": id, "kind": activeKind, "stats": before}); err != nil {
					return false, err
				}
				return false, measurementAck(input, fmt.Sprintf("device-%03d-begin", id))
			case "TARP_DEVICE_END":
				if activeKind != fields[2] {
					return false, errors.New("device interval end does not match begin")
				}
				after := disk.Stats()
				reads, bytes := after.Reads-before.Reads, after.ReadBytes-before.ReadBytes
				if activeKind == "cold" || activeKind == "cold_open" {
					if reads == 0 || bytes == 0 {
						return false, errors.New("declared guest-OS-cold operation did not reach the virtual block device")
					}
					if activeKind == "cold" {
						cold++
					} else {
						coldOpens++
					}
				} else {
					warm++
				}
				if err := log.record(map[string]any{"event": "device_interval_end", "id": id, "kind": activeKind,
					"read_requests": reads, "read_bytes": bytes}); err != nil {
					return false, err
				}
				activeKind = ""
				intervals++
				return false, measurementAck(input, fmt.Sprintf("device-%03d-end", id))
			}
		}
		if strings.HasPrefix(payload, "TARP_MEASURE_PASS ") {
			var verdict qualification.MeasurementVerdict
			if err := json.Unmarshal([]byte(strings.TrimPrefix(payload, "TARP_MEASURE_PASS ")), &verdict); err != nil {
				return false, err
			}
			expectedKeys := qualification.MeasurementKeys
			if cfg.Profile {
				expectedKeys = 512
			}
			if verdict.ValueBytes != cfg.ValueBytes || verdict.Profile != cfg.Profile ||
				verdict.WorkingKeys != expectedKeys || verdict.Repetitions != 5 || verdict.Rows != rows || activeKind != "" {
				return false, errors.New("incomplete measurement verdict")
			}
			required := []string{"value_cache_hit_workers_1", "value_cache_hit_workers_4", "value_cache_hit_batch_workers_1", "value_cache_hit_batch_workers_4", "value_cache_miss_os_warm",
				"mixed_multi_writer", "mixed_multi_writer_reads", "mixed_multi_writer_puts", "mixed_multi_writer_schedule",
				"durable_put_rotate_false", "durable_put_rotate_true",
				"durable_delete_rotate_false", "durable_delete_rotate_true", "resources", "guest_os_cold_virtual_device_get",
				"compaction_automatic-incremental", "compaction_manual-full-data-checkpoint", "repack_parked", "repack_concurrent",
				"scan_full_warm", "scan_paused_full_data_checkpoint",
				"startup_default_history", "startup_full_data_checkpoint",
				"startup_cold_default_history", "startup_cold_full_data_checkpoint",
				"complete_index_default_history", "complete_index_full_data_checkpoint"}
			for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "pinned-backup-compact"} {
				required = append(required, "interference_"+mode, "interference_"+mode+"_read", "interference_"+mode+"_durable_put")
			}
			if cfg.Profile {
				required = nil
				for _, mode := range []string{"baseline", "backup", "compact", "backup-compact", "pinned-backup-compact"} {
					for _, lock := range []string{"writer", "state", "lifecycle"} {
						required = append(required, "lock_hold_"+mode+"_"+lock)
					}
				}
				if cold != 0 || coldOpens != 0 || warm != 0 || verdict.ColdCalls != 0 ||
					verdict.ColdOpens != 0 || verdict.HistoryMutations != 0 ||
					kinds["diagnostic_environment/0"] != 1 {
					return false, errors.New("diagnostic role claimed primary measurement work")
				}
			} else if cold != 160 || coldOpens != 10 || warm != 5 || verdict.ColdCalls != 160 ||
				verdict.ColdOpens != 10 || verdict.HistoryMutations != 10240 ||
				kinds["environment/0"] != 1 || kinds["history/0"] != 1 {
				return false, errors.New("cold/storage/history measurement coverage is incomplete")
			}
			for repetition := 1; repetition <= 5; repetition++ {
				for _, kind := range required {
					if kinds[fmt.Sprintf("%s/%d", kind, repetition)] != 1 {
						return false, fmt.Errorf("measurement %s repetition %d missing or duplicated", kind, repetition)
					}
				}
				if cfg.Profile && kinds[fmt.Sprintf("pending_owned_buffers/%d", repetition)] != 3 {
					return false, errors.New("pending-owned-buffer caller profiles are incomplete")
				}
			}
			passed = true
			return false, log.record(map[string]any{"event": "measurement_pass", "verdict": verdict})
		}
		return false, fmt.Errorf("unknown measurement payload %q", payload)
	})
	if err := errors.Join(err, log.record(map[string]any{"event": "measurement_vm", "configuration": cfg,
		"vm": vm, "device": disk.Stats()})); err != nil {
		return err
	}
	if !environment || !passed || vm.Cut {
		return errors.New("qualified-target measurement guest did not complete successfully")
	}
	if err := disk.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	return os.Remove(path)
}
