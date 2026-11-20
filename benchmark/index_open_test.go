package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
)

// TestIndexOpenReplay is an opt-in cross-revision probe. All binaries open the
// same immutable fixtures. Park automatic maintenance throughout the campaign.
func TestIndexOpenReplay(t *testing.T) {
	parent := os.Getenv("TARP_REPLAY_FIXTURE")
	if parent == "" {
		t.Skip("set TARP_REPLAY_FIXTURE to compare Open times")
	}
	opts := engine.Options{CacheBytes: 64 << 10, MaxRecordBytes: 2048, TargetSegmentBytes: 1 << 20}
	open := func(dir string) *engine.DB {
		t.Helper()
		db, err := engine.OpenWithHooks(dir, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
		if err != nil {
			t.Fatal(err)
		}
		return db
	}
	const liveKeys = 16384
	for _, layout := range []string{"history", "checkpoint"} {
		dir := filepath.Join(parent, layout)
		if os.Getenv("TARP_REPLAY_SEED") == "1" {
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			db := open(dir)
			value := make([]byte, 128)
			for _, n := range []int{liveKeys, liveKeys / 2, liveKeys / 4} {
				for start := 0; start < n; start += 128 {
					batch := make([]engine.Entry, 128)
					for j := range batch {
						key := []byte(fmt.Sprintf("%016x", start+j))
						batch[j] = engine.Entry{Key: key, Value: value}
					}
					if err := db.BatchPut(batch); err != nil {
						t.Fatal(err)
					}
				}
			}
			if layout == "checkpoint" {
				if _, err := db.Compact(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, "CURRENT")); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			runtime.GC()
			start := time.Now()
			db := open(dir)
			elapsed := time.Since(start)
			if count := db.Stats().LiveKeys; count != liveKeys {
				t.Fatalf("live keys %d, want %d", count, liveKeys)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if i > 0 { // exclude the warmup Open
				t.Logf("layout=%s sample=%d ns=%d", layout, i, elapsed.Nanoseconds())
			}
		}
	}
}
