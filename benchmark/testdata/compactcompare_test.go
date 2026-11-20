//go:build linux

package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/benchmark/qualification"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
)

// The diagnostic-only binary supplies timings for individual Compact stages.
var compactDiagnostic = func() any { return nil }

// TestCompactCompare is compiled into each released benchmark package. A single
// seed invocation prepares the closed disk image used by both binaries.
func TestCompactCompare(t *testing.T) {
	mode := os.Getenv("TARP_COMPARE_MODE")
	if mode == "" {
		t.Skip("requires the paired Compact measurement guest")
	}
	arm := os.Getenv("TARP_COMPARE_ARM")
	round, err := strconv.Atoi(os.Getenv("TARP_COMPARE_ROUND"))
	if (arm != "base" && arm != "next") || err != nil || round < 0 {
		t.Fatalf("invalid measurement arm %q or round %q", arm, os.Getenv("TARP_COMPARE_ROUND"))
	}
	const fixture = "/mnt/tarp-data/compare-fixture"
	keys, value, opts := measurementInput(64, qualification.MeasurementKeys)
	// The two tags use different Go names for the same value LRU budget.
	budget := reflect.ValueOf(&opts).Elem().FieldByName("ValueCacheBudgetBytes")
	if !budget.IsValid() {
		budget = reflect.ValueOf(&opts).Elem().FieldByName("CacheBytes")
	}
	budget.SetInt(64 << 10)
	opts.TargetSegmentBytes, opts.ReadFileHandles = 1<<20, 4
	if mode == "seed" {
		if arm != "base" || round != 0 {
			t.Fatal("only the baseline seeds the shared fixture")
		}
		guesttest.MountTarget(t, true, "compact-compare-64", 42)
	} else {
		guesttest.RequireDisposableGuest(t)
	}
	t.Setenv("TMPDIR", filepath.Dir(fixture))
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
		fmt.Printf("TARP_COMPARE {\"mode\":\"seed\",\"arm\":\"base\",\"round\":0,\"seed_keys\":%d}\n", len(keys))
		return
	}

	db, hooks := measurementOpenClone(t, fixture, opts)
	probe := engine.Observe(db)
	dir := probe.Dir()
	initial := probe.Frontier()
	// 4,096 initial Puts, 1,024 overwrites and 256 delete/reinsert pairs.
	if initial.Sequence != 5632 {
		t.Fatalf("fixture sequence %d", initial.Sequence)
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
		row["fixture_sequence_before_workers"] = probe.Frontier().Sequence
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
	case "overlap", "diagnostic", "baseline", "pinned":
		originalMode := "compact"
		switch mode {
		case "baseline":
			originalMode = "baseline"
		case "pinned":
			originalMode = pinnedBackupMode
		}
		// Use the release's original 5 ms sampler and 500 ms foreground window.
		row = measureInterferenceFor(t, db, hooks, keys[:8], value, originalMode, 500*time.Millisecond)
		for _, worker := range []string{"read", "durable_put"} {
			sample := row[worker].(sampledLatency)
			if sample.Operations <= 0 {
				t.Fatalf("%s completed no foreground operations", worker)
			}
			if worker == "durable_put" {
				puts = sample.Operations
			}
			sample.SamplesNS = nil
			row[worker] = sample
		}
		if mode == "diagnostic" {
			row["stages"] = compactDiagnostic()
			if row["stages"] == nil {
				t.Fatal("missing per-stage instrumentation in diagnostic binary")
			}
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
		// All 4,097 keys are 16 bytes and their records fit in one snapshot.
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
	layout := observedLayout(reopened)
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	wantSequence := initial.Sequence + 1 + uint64(puts)
	if int64(live) != count || live != row["fixture_live_keys"].(int) || digest != row["fixture_state_sha256"].(string) ||
		initial.UUID != frontier.UUID || frontier.Sequence != wantSequence || row["fixture_sequence_before_workers"].(uint64) != initial.Sequence+1 {
		t.Fatalf("reopened state: digest %s, %d keys, sequence %d (want %d)", digest, live, frontier.Sequence, wantSequence)
	}
	if mode != "baseline" && (layout.CheckpointCutoff < initial.Sequence+1 || layout.CheckpointCutoff > frontier.Sequence ||
		layout.SnapshotFiles == 0 || layout.UnselectedPinnedBytes != 0) {
		t.Fatalf("unexpected reopened checkpoint layout: %+v", layout)
	}
	row["arm"], row["round"], row["mode"] = arm, round, mode
	row["source_manifest_sha256"] = fmt.Sprintf("%x", initial.ManifestHash)
	row["successful_puts"], row["reopened_sequence"], row["reopened_live_keys"] = puts, frontier.Sequence, live
	row["reopened_state_sha256"], row["reopened_checkpoint_cutoff"] = digest, layout.CheckpointCutoff
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("TARP_COMPARE %s\n", encoded)
}
