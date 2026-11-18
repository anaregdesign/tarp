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
}

type RecoveryVerdict struct {
	Source          string `json:"source"`
	SourceSequence  uint64 `json:"source_sequence"`
	SourceLiveKeys  int    `json:"source_live_keys"`
	StateSHA256     string `json:"state_sha256"`
	Output          string `json:"output"`
	CaptureSequence uint64 `json:"capture_sequence"`
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
	pair("checkpoint", "compact", 1, "compact.segment.write", "compact.segment.sync", "compact.write", "compact.sync", "cleanup.unlink")
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
	if len(all) == 0 || len(all) > 192 {
		return nil, fmt.Errorf("qualification: unknown or oversized campaign group %q", group)
	}
	return all, nil
}
