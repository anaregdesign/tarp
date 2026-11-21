package benchmark

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/benchmark/scaleprobe"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestStorageScaleOpen(t *testing.T) {
	round := storageScaleRound(t)
	keys, value, opts := measurementInput(128, 4096)
	// Keep a single selected segment, independent of the orphan count.
	opts.TargetSegmentBytes = 4 << 20
	seed := filepath.Join(t.TempDir(), "seed")
	seedOpts := tarp.Options(opts)
	seedOpts.IndexMode = tarp.IndexMemory // Keep the single DATA segment unsealed on Close.
	db, err := tarp.Open(seed, seedOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	for i := 0; i < len(keys); i += 128 {
		batch := make([]tarp.Entry, 128)
		for j := range batch {
			batch[j] = tarp.Entry{Key: keys[i+j], Value: value}
		}
		if err := db.BatchPut(batch); err != nil {
			t.Fatal(err)
		}
	}
	frontier := engine.Observe((*engine.DB)(db)).Frontier()
	wantDigest, count := measurementState(t, (*engine.DB)(db))
	wantStats := db.Stats()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	counts := [...]int{0, 100, 1000}
	for i := range counts {
		n := counts[(round-1+i)%len(counts)]
		dir := filepath.Join(t.TempDir(), "db")
		measurementCloneClosed(t, seed, dir)
		orphans := make([]string, n)
		for j := range orphans {
			id := uint64(10000 + j)
			name := storage.DataName(frontier.UUID, id)
			if j%2 != 0 {
				name = storage.ManifestName(frontier.UUID, id)
			}
			orphans[j] = filepath.Join(dir, name)
			if err := os.WriteFile(orphans[j], nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := (*storage.IO)(nil).SyncDir(dir, "fixture.dirsync"); err != nil {
			t.Fatal(err)
		}

		before := scaleprobe.DirectoryReads.Load()
		start := time.Now()
		opened, err := tarp.OpenExisting(t.Context(), dir, tarp.Options(opts))
		elapsed := time.Since(start)
		reads := scaleprobe.DirectoryReads.Load() - before
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := opened.Close(); err != nil {
				t.Error(err)
			}
		})
		for _, path := range orphans {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("orphan %s remains: %v", path, err)
			}
		}
		gotDigest, gotCount := measurementState(t, (*engine.DB)(opened))
		if gotDigest != wantDigest || gotCount != count || gotCount != len(keys) || reads == 0 || opened.Stats().SelectedDataBytes != wantStats.SelectedDataBytes {
			t.Fatalf("Open lost fixture identity or directory observations: digest %s, keys %d, directory reads %d", gotDigest, gotCount, reads)
		}
		if err := opened.Close(); err != nil {
			t.Fatal(err)
		}
		storageScaleRow(t, round, "open", fmt.Sprintf("orphans-%d", n), map[string]any{
			"elapsed_ns": elapsed.Nanoseconds(), "store_directory_listings": reads, "unselected_files_removed": n,
			"live_keys": count, "selected_data_bytes": wantStats.SelectedDataBytes, "state_sha256": gotDigest,
		})
	}
}
