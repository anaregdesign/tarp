//go:build tarpprofile

package benchmark

import (
	"fmt"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp/internal/engine"
)

func init() {
	compactLockProfile = observeCompactLockHolds
}

func observeCompactLockHolds(t *testing.T, probe engine.Observer, hooks *measurementHooks) func() map[string]compactLockHolds {
	t.Helper()
	probe.ResetLockProfile()
	hooks.windowStart = func() { engine.ProfileLocks(true) }
	hooks.windowEnd = func() { engine.ProfileLocks(false) }
	t.Cleanup(hooks.windowEnd)
	return func() map[string]compactLockHolds {
		all := probe.LockProfile()
		holds := make(map[string]compactLockHolds)
		for _, name := range []string{"writer", "state", "lifecycle"} {
			hold, err := summarizeCompactLockHolds(all[name])
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			holds[name] = hold
		}
		return holds
	}
}

// The Observer retains the first 65,536 holds of each mutex. Validate that
// prefix against the all-window aggregates before sampling it for the console.
func summarizeCompactLockHolds(stats engine.LockStats) (compactLockHolds, error) {
	if stats.Count == 0 || uint64(len(stats.Samples)) != min(stats.Count, 65536) {
		return compactLockHolds{}, fmt.Errorf("retained %d samples for %d holds", len(stats.Samples), stats.Count)
	}
	var total uint64
	var maximum int64
	for _, ns := range stats.Samples {
		if ns < 0 {
			return compactLockHolds{}, fmt.Errorf("negative hold of %d ns", ns)
		}
		total += uint64(ns)
		maximum = max(maximum, ns)
	}
	complete := uint64(len(stats.Samples)) == stats.Count
	if stats.MaximumNS <= 0 || uint64(stats.MaximumNS) > stats.TotalNS || total > stats.TotalNS || maximum > stats.MaximumNS ||
		(complete && (total != stats.TotalNS || maximum != stats.MaximumNS)) {
		return compactLockHolds{}, fmt.Errorf("prefix total/max %d/%d ns, all holds %d/%d ns", total, maximum, stats.TotalNS, stats.MaximumNS)
	}
	// Keep both endpoints and at most 64 evenly spaced prefix samples.
	n := min(len(stats.Samples), 64)
	raw := make([]int64, n)
	for i := range raw {
		raw[i] = stats.Samples[i*(len(stats.Samples)-1)/max(1, n-1)]
	}
	return compactLockHolds{stats.Count, stats.TotalNS, stats.MaximumNS, len(stats.Samples), raw}, nil
}

func TestCompactComparisonLockProfile(t *testing.T) {
	t.Run("bounded prefix", func(t *testing.T) {
		const retained = 65536
		stats := engine.LockStats{Count: retained + 1, MaximumNS: retained + 1, Samples: make([]int64, retained)}
		for i := range stats.Samples {
			stats.Samples[i] = int64(i + 1)
			stats.TotalNS += uint64(i + 1)
		}
		stats.TotalNS += uint64(stats.MaximumNS)
		hold, err := summarizeCompactLockHolds(stats)
		if err != nil || hold.RetainedPrefixCount != retained || len(hold.SampledHoldNS) != 64 ||
			hold.SampledHoldNS[0] != 1 || hold.SampledHoldNS[63] != retained {
			t.Fatalf("bounded prefix: %+v, %v", hold, err)
		}
		stats.Count-- // The extra hold can no longer account for the reported sum and max.
		if _, err := summarizeCompactLockHolds(stats); err == nil {
			t.Fatal("accepted a complete prefix with inconsistent aggregates")
		}
		stats.Samples = stats.Samples[:retained-1]
		if _, err := summarizeCompactLockHolds(stats); err == nil {
			t.Fatal("accepted a missing hold sample")
		}
	})
	db := newTestDBWithHooks(t, engine.Options{IndexMode: engine.IndexMemory}, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	hooks := &measurementHooks{}
	report := compactLockProfile(t, engine.Observe(db), hooks)
	hooks.windowStart()
	mustPut(t, db, "key", "value")
	if _, err := db.Get([]byte("key")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	hooks.windowEnd()
	inside := report()
	mustPut(t, db, "other", "value")
	for name, after := range report() {
		before := inside[name]
		if after.Observations != before.Observations || after.TotalHoldNS != before.TotalHoldNS || !slices.Equal(after.SampledHoldNS, before.SampledHoldNS) {
			t.Fatalf("%s recorded holds after its window closed", name)
		}
	}
}
