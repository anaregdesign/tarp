package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
)

// Only the tarpprofile binary installs the foreground window's mutex observer.
var compactLockProfile func(*testing.T, engine.Observer, *measurementHooks) func() map[string]compactLockHolds

type compactLockHolds struct {
	Observations        uint64  `json:"observations"`
	TotalHoldNS         uint64  `json:"total_hold_ns"`
	MaximumHoldNS       int64   `json:"maximum_hold_ns"`
	RetainedPrefixCount int     `json:"retained_prefix_count"`
	SampledHoldNS       []int64 `json:"sampled_hold_ns"`
}

func runCompactCompare(t *testing.T, fixture, arm, mode string, round, keyCount int) map[string]any {
	t.Helper()
	keys, value, opts := measurementInput(64, keyCount)
	opts.IndexMode, opts.ValueCacheBudgetBytes = engine.IndexMemory, 64<<10
	opts.TargetSegmentBytes, opts.ReadFileHandles = 1<<20, 4
	if mode == "seed" {
		source := filepath.Join(t.TempDir(), "db")
		db, err := engine.Open(source, opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if err := db.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		measurementPrepareFixture(t, source, fixture, opts, keys, value)
		return map[string]any{"mode": mode, "arm": arm, "round": round, "seed_keys": len(keys)}
	}

	db, hooks := measurementOpenClone(t, fixture, opts)
	probe := engine.Observe(db)
	dir := probe.Dir()
	initial := probe.Frontier()
	initialLastUserSequence := initial.LastUserSequence
	// Match the initial Puts, overwrites and delete/reinsert pairs in the fixture.
	if initialLastUserSequence != uint64(keyCount+min(1024, keyCount)+2*min(256, keyCount/4)) {
		t.Fatalf("fixture sequence %d", initialLastUserSequence)
	}
	row := map[string]any{}
	var puts int64
	switch mode {
	case "parked":
		if err := db.Put([]byte("writer-key-00000"), value); err != nil {
			t.Fatal(err)
		}
		row["fixture_state_sha256"], row["fixture_live_keys"] = measurementState(t, db)
		for _, key := range keys[:8] {
			if _, err := db.Get(key); err != nil {
				t.Fatal(err)
			}
		}
		row["fixture_last_user_sequence_before_workers"] = probe.Frontier().LastUserSequence
		// All foreground workers and the inventory sampler are parked.
		start := time.Now()
		_, err := db.Compact(t.Context())
		row["compact_elapsed_ns"] = time.Since(start).Nanoseconds()
		if err != nil {
			t.Fatal(err)
		}
		row["compact_records_copied"] = hooks.manualRecords.Load()
		row["compact_copy_bytes"] = hooks.manualBytes.Load()
		row["compact_copy_source_opens"] = hooks.manualSourceOpens.Load()
	case "overlap", "fixed", "lock-profile", "baseline", "pinned":
		originalMode := "compact"
		minimum := 500 * time.Millisecond
		switch mode {
		case "fixed":
			minimum = 5 * time.Second
		case "baseline":
			originalMode = "baseline"
		case "pinned":
			originalMode = pinnedBackupMode
		}
		var finishProfile func() map[string]compactLockHolds
		if mode == "lock-profile" {
			if compactLockProfile == nil {
				t.Fatal("lock-profile window requires the tarpprofile binary")
			}
			finishProfile = compactLockProfile(t, probe, hooks)
		}
		// The fixed window continues the same traffic after Compact returns.
		row = measureInterferenceFor(t, db, hooks, keys[:8], value, originalMode, minimum)
		if mode == "fixed" && row["maintenance_ns"].(int64) >= minimum.Nanoseconds() {
			t.Fatal("Compact exceeded the fixed foreground window")
		}
		for _, worker := range []string{"read", "durable_put"} {
			sample := row[worker].(sampledLatency)
			if sample.Operations <= 0 || sample.P99NS <= 0 {
				t.Fatalf("%s missing foreground latency measurements", worker)
			}
			if mode == "fixed" && sample.WallNS < minimum.Nanoseconds() {
				t.Fatalf("%s ran for %d ns of the %v window", worker, sample.WallNS, minimum)
			}
			if worker == "durable_put" {
				puts = sample.Operations
				if mode == "fixed" && int64(len(sample.SamplesNS)) != puts {
					t.Fatalf("fixed window retained %d of %d Put latencies", len(sample.SamplesNS), puts)
				}
				slices.Sort(sample.SamplesNS)
			} else {
				sample.SamplesNS = nil // Keep the much smaller Put sample for pooled tails.
			}
			row[worker] = sample
		}
		if finishProfile != nil {
			row["lock_holds"] = finishProfile()
		}
	default:
		t.Fatalf("unknown comparison mode %q", mode)
	}
	count := int64(len(keys) + 1)
	records, copyBytes := row["compact_records_copied"].(int64), row["compact_copy_bytes"].(int64)
	if mode == "baseline" {
		if records != 0 || copyBytes != 0 || row["compact_elapsed_ns"].(int64) != 0 {
			t.Fatal("foreground-only window observed Compact activity")
		}
	} else {
		// All keys are 16 bytes and their DATA records fit in one snapshot segment.
		wantBytes := format.SegmentHeaderBytes + count*(format.RecordHeaderBytes+16+int64(len(value)))
		if records != count || copyBytes != wantBytes || row["compact_elapsed_ns"].(int64) <= 0 {
			t.Fatalf("Compact copied %d records and %d bytes; want %d and %d", records, copyBytes, count, wantBytes)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := engine.OpenWithHooks(dir, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	digest, live := measurementState(t, reopened)
	frontier := engine.Observe(reopened).Frontier()
	reopenedLastUserSequence := frontier.LastUserSequence
	layout := observedLayout(reopened)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	wantLastUserSequence := initialLastUserSequence + 1 + uint64(puts)
	if int64(live) != count || live != row["fixture_live_keys"].(int) || digest != row["fixture_state_sha256"].(string) ||
		initial.UUID != frontier.UUID || reopenedLastUserSequence != wantLastUserSequence || row["fixture_last_user_sequence_before_workers"].(uint64) != initialLastUserSequence+1 {
		t.Fatalf("reopened state: digest %s, %d keys, sequence %d (want %d)", digest, live, reopenedLastUserSequence, wantLastUserSequence)
	}
	if mode != "baseline" && (layout.CheckpointCutoffSequence < initialLastUserSequence+1 || layout.CheckpointCutoffSequence > reopenedLastUserSequence ||
		layout.SnapshotSegments == 0 || layout.UnselectedPinnedDataBytes != 0) {
		t.Fatalf("unexpected reopened checkpoint layout: %+v", layout)
	}
	row["arm"], row["round"], row["mode"] = arm, round, mode
	row["source_manifest_sha256"] = fmt.Sprintf("%x", initial.ManifestDigest)
	row["successful_puts"], row["reopened_last_user_sequence"], row["reopened_live_keys"] = puts, reopenedLastUserSequence, live
	row["reopened_state_sha256"], row["reopened_checkpoint_cutoff_sequence"] = digest, layout.CheckpointCutoffSequence
	return row
}

// CI seeds once and runs each revision against clones of the same closed fixture.
func TestCompactComparisonFixture(t *testing.T) {
	const keys = 8
	fixture := os.Getenv("TARP_COMPACT_TEST_FIXTURE")
	if fixture == "" {
		fixture = filepath.Join(t.TempDir(), "fixture")
	}
	t.Run("seed", func(t *testing.T) {
		runCompactCompare(t, fixture, "base", "seed", 0, keys)
	})
	for _, arm := range []string{"base", "next"} {
		t.Run(arm, func(t *testing.T) {
			row := runCompactCompare(t, fixture, arm, "parked", 1, keys)
			if row["reopened_last_user_sequence"] != uint64(21) || row["compact_records_copied"] != int64(9) {
				t.Fatalf("unexpected sequence %v or copied DATA records %v", row["reopened_last_user_sequence"], row["compact_records_copied"])
			}
			pinned := runCompactCompare(t, fixture, arm, "pinned", 0, keys)
			pins := pinned["pinned_backup"].(pinnedBackupMeasurement)
			if pins.RetiredFilesHeld == 0 || pins.RetiredBytesHeld == 0 || pins.RetiredFilesHeld != pins.ReleasedFilesUnlinked || pins.RetiredBytesHeld != pins.ReleasedBytesUnlinked {
				t.Fatalf("captured DATA retention and reclamation: %+v", pins)
			}
		})
	}
}
