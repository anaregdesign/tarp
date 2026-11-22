package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/benchmark/qualification"
)

// TestCompactCompare is the guarded entry point in each comparison binary.
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
	if mode == "seed" {
		if arm != "base" || round != 0 {
			t.Fatal("only the baseline seeds the shared fixture")
		}
		guesttest.MountTarget(t, true, "compact-compare-64", 42)
	} else {
		guesttest.RequireDisposableGuest(t)
	}
	const fixture = "/mnt/tarp-data/compare-fixture"
	t.Setenv("TMPDIR", filepath.Dir(fixture))
	row := runCompactCompare(t, fixture, arm, mode, round, qualification.MeasurementKeys)
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("TARP_COMPARE %s\n", encoded)
}
