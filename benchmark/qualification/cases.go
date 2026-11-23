package qualification

import (
	"fmt"
	"strings"
)

type PowerCase struct {
	Name            string `json:"name"`
	Mode            string `json:"mode"`
	Phase           string `json:"phase"`
	Occurrence      int    `json:"occurrence"`
	Seed            uint64 `json:"seed"`
	Action          string `json:"action"`
	Captured        bool   `json:"captured"`
	CaptureSequence uint64 `json:"capture_sequence"`
	RequireOutput   bool   `json:"require_output"`
	IndexMode       string `json:"index_mode,omitempty"`
}

type RecoveryVerdict struct {
	Source                string `json:"source"`
	SourceSequence        uint64 `json:"source_sequence"`
	SourceLiveKeys        int    `json:"source_live_keys"`
	StateSHA256           string `json:"state_sha256"`
	Output                string `json:"output"`
	CaptureSequence       uint64 `json:"capture_sequence"`
	IndexedVerified       bool   `json:"indexed_verified,omitempty"`
	IndexedCheckpointUsed bool   `json:"indexed_checkpoint_used,omitempty"`
	RepairRetried         bool   `json:"repair_retried,omitempty"`
}

func CampaignCases(group string) ([]PowerCase, error) {
	var all []PowerCase
	add := func(category, mode, point string, occurrence int) {
		if group != "all" && group != "smoke" && group != category {
			return
		}
		name := strings.NewReplacer(".", "-", ":", "-").Replace(mode + "-" + point + fmt.Sprintf("-n%d", occurrence))
		all = append(all, PowerCase{Name: name, Mode: mode, Phase: point, Occurrence: occurrence,
			Seed: 0x5eed0000 + uint64(len(all)), Action: "crash"})
	}
	pair := func(category, mode string, occurrence int, points ...string) {
		for _, point := range points {
			add(category, mode, point+":before", occurrence)
			add(category, mode, point+":after", occurrence)
		}
	}
	pair("initialize", "init", 1, "lock.sync", "lock.dirsync", "init.parent.sync", "segment.write", "segment.sync",
		"catalog.write", "catalog.sync", "catalog.dirsync", "current.write", "current.sync", "current.rename", "current.dirsync")
	add("initialize", "init", "operation.ack:after", 1)
	for _, mode := range []string{"put", "delete"} {
		pair("mutate", mode, 1, "data.write", "data.sync", "current.write", "current.sync", "current.rename", "current.dirsync")
		add("mutate", mode, "operation.ack:after", 1)
	}
	for _, mode := range []string{"insert", "reinsert"} {
		pair("mutate", mode, 1, "current.rename", "current.dirsync")
		add("mutate", mode, "operation.ack:after", 1)
	}
	pair("mutate", "rotate", 1, "segment.write", "data.write", "segment.sync", "catalog.write", "catalog.sync", "catalog.dirsync",
		"current.write", "current.sync", "current.rename", "current.dirsync")
	add("mutate", "rotate", "operation.ack:after", 1)
	pair("checkpoint", "compact", 1, "compact.segment.write", "compact.write", "compact.sync", "cleanup.unlink")
	pair("checkpoint", "compact", 2, "compact.write")
	for _, point := range []string{"catalog.write", "catalog.sync", "catalog.dirsync", "current.write", "current.sync", "current.rename", "current.dirsync"} {
		pair("checkpoint", "compact", 2, point)
	}
	add("checkpoint", "compact", "compact.remap:after", 1)
	add("checkpoint", "compact", "operation.ack:after", 1)
	pair("snapshots", "backup", 1, "backup.metadata.write", "backup.metadata.sync", "backup.copy", "backup.file.sync",
		"backup.manifest.write", "backup.manifest.sync", "backup.stage.sync", "backup.rename", "backup.parent.sync")
	pair("snapshots", "backup", 2, "backup.copy")
	pair("snapshots", "backup", 3, "backup.copy")
	add("snapshots", "backup", "operation.ack:after", 1)
	pair("snapshots", "restore", 1, "restore.copy", "restore.file.sync", "restore.stage.sync", "restore.rename", "restore.parent.sync")
	add("snapshots", "restore", "operation.ack:after", 1)
	pair("recovery", "recover", 1, "recover.truncate", "recover.sync")
	pair("recovery", "overlap", 1, "compact.write", "compact.sync", "cleanup.unlink", "backup.parent.sync")
	add("recovery", "overlap", "compact.remap:after", 1)
	add("recovery", "overlap", "operation.ack:after", 1)
	add("exhaustion", "exhaustion", "exhaustion.complete", 1)
	extended := func(category, mode string, points ...string) {
		for _, index := range []string{"btree", "memory"} {
			if mode == "index-close" && index == "memory" {
				continue // Memory Close deliberately does not publish an INDEX.
			}
			for _, point := range points {
				phases := []string{point + ":before", point + ":after"}
				if strings.Contains(point, ":") {
					phases = []string{point}
				}
				for _, phase := range phases {
					add(category, mode, phase, 1)
					if group == "all" || group == category {
						last := &all[len(all)-1]
						last.Name += "-" + index
						last.IndexMode = index
					}
				}
			}
		}
	}
	for _, mode := range []string{"batch-put", "batch-delete", "batch-rotate"} {
		extended("batches", mode, "data.write", "current.write", "current.sync", "current.rename", "current.dirsync")
		if mode != "batch-rotate" {
			extended("batches", mode, "data.sync")
		}
		if mode == "batch-rotate" {
			extended("batches", mode, "segment.write", "segment.sync", "catalog.write", "catalog.sync", "catalog.dirsync")
		}
		extended("batches", mode, "operation.ack")
	}
	for _, mode := range []string{"clean-single", "clean-group", "clean-repack"} {
		extended("cleaner", mode, "clean.output.sync", "clean.parity.sync", "clean.parity.rename",
			"clean.catalog.sync", "clean.catalog.dirsync", "clean.current.sync", "clean.current.rename",
			"clean.current.dirsync", "clean.retire.unlink", "clean.retire.dirsync")
		extended("cleaner", mode, "operation.ack")
		extended("cleaner", mode, "clean.remap:after")
	}
	for _, mode := range []string{"ordered-initial", "ordered-delta", "ordered-reuse"} {
		extended("ordered", mode, "compact.write", "compact.sync", "compact.parity.sync",
			"catalog.sync", "current.rename", "current.dirsync", "clean.retire.unlink", "clean.retire.dirsync")
		extended("ordered", mode, "operation.ack")
		extended("ordered", mode, "compact.remap:after")
	}
	for _, mode := range []string{"index-explicit", "index-close"} {
		extended("indexes", mode, "index.write", "index.sync", "index.rename", "index.dirsync",
			"index.catalog.sync", "index.current.rename", "index.current.dirsync")
	}
	extended("repair", "repair-data", "data-repair.write", "data-repair.sync", "data-repair.rename", "data-repair.dirsync")
	extended("repair", "repair-parity", "parity.write", "parity.sync", "parity.rename", "parity.dirsync")
	add("pager", "pager-exhaustion", "pager.complete", 1)
	if group == "all" || group == "pager" {
		all[len(all)-1].IndexMode = "btree"
	}
	if group == "smoke" {
		wanted := map[string]string{
			"init": "current.rename:after", "put": "data.sync:after", "insert": "current.dirsync:after",
			"delete": "operation.ack:after", "reinsert": "operation.ack:after", "rotate": "segment.sync:after",
			"compact": "cleanup.unlink:after", "backup": "backup.parent.sync:after",
			"restore": "restore.rename:after", "recover": "recover.truncate:after", "overlap": "compact.remap:after",
		}
		var selected []PowerCase
		for _, candidate := range all {
			if wanted[candidate.Mode] == candidate.Phase && candidate.Occurrence == 1 {
				selected = append(selected, candidate)
			}
		}
		all = selected
	}
	if len(all) == 0 {
		return nil, fmt.Errorf("qualification: unknown or oversized campaign group %q", group)
	}
	return all, nil
}
