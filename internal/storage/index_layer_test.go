package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index/btree"
)

func installTestIndexLayer(t *testing.T, s *Store) {
	t.Helper()
	work, err := s.BeginIndexCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := work.Release(); err != nil {
			t.Error(err)
		}
	}()
	if err := work.WriteIncremental(t.Context(), btree.MinBudget); err != nil {
		t.Fatal(err)
	}
	if err := work.PublishFile(); err != nil {
		t.Fatal(err)
	}
	if err := work.Install(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexRefreshReadsOnlyRelocatedDataAtSameSequence(t *testing.T) {
	cfg := Config{IndexedOpen: true, Handles: 1, MaxRecordBytes: 1 << 20, ValueCacheBudgetBytes: 4 << 20, TargetSegmentBytes: 4 << 20}
	dir := t.TempDir()
	reads := make(map[string]int64)
	io := &IO{OpenRead: func(kind string, n int) { reads[kind] += int64(n) }}
	s, err := openTestStore(t, dir, cfg, io)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	appendFastTestBase(t, s)
	healthy := func() error { return nil }
	for _, value := range []string{"old", "latest"} {
		if _, err := s.Append(format.Put, []byte("key"), []byte(value), healthy); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	installFastTestIndex(t, s)
	base := s.Catalog.Index
	old := s.CatalogEntries().At(1)
	work, err := s.BeginClean([]format.CatalogEntry{old})
	if err != nil || work == nil {
		t.Fatalf("BeginClean: %v, %v", work, err)
	}
	defer func() {
		if work != nil {
			work.Release()
		}
	}()
	moves := s.NewRelocations()
	if err := work.Copy(t.Context(), s.IO, moves, nil, healthy, func(_ string, addr Location) (bool, error) {
		return addr.Sequence == base.CoveredSequence, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ProtectCleaned(t.Context(), work, healthy); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallCleaned(work, nil); err != nil {
		t.Fatal(err)
	}
	if err := moves.Close(); err != nil {
		t.Fatal(err)
	}
	cleaned := work.output
	work.Release()
	work = nil
	if _, _, _, err := s.RetireCleaned(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, DataName(s.Current.UUID, old.ID))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired DATA still present: %v", err)
	}

	clear(reads)
	installTestIndexLayer(t, s)
	layer := s.Catalog.Index
	if layer.ID == base.ID || layer.CoveredSequence != base.CoveredSequence ||
		layer.CoveredLayoutFingerprint == base.CoveredLayoutFingerprint {
		t.Fatalf("layer=%+v base=%+v", layer, base)
	}
	if got := reads["index_refresh_encoded_records"]; got != cleaned.Size.Load()-format.SegmentHeaderBytes ||
		reads["replay_encoded_records"] != 0 {
		t.Fatalf("refresh reads=%v, want only %d encoded bytes", reads, cleaned.Size.Load()-format.SegmentHeaderBytes)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Run("Full", func(t *testing.T) {
		full := cfg
		full.IndexedOpen = false
		reopened, _ := openFastTestIndex(t, dir, full, nil)
		if info := reopened.RecoveryInfo(); info.Path != RecoveryFullReplay || info.Reason != RecoveryFullValidation {
			t.Fatalf("Full Open of layered INDEX: %+v", info)
		}
		if _, err := os.Stat(filepath.Join(dir, IndexName(s.Current.UUID, base.ID))); err != nil {
			t.Fatalf("Full Open discarded a selected ancestor: %v", err)
		}
	})
	clear(reads)
	reopened, keys := openFastTestIndex(t, dir, cfg, io)
	if info := reopened.RecoveryInfo(); info.Path != RecoveryFastCheckpointReplay || info.CheckpointID != layer.ID ||
		reads["log_suffix_encoded_records"] != 0 || reads["replay_encoded_records"] != 0 {
		t.Fatalf("reopen: %+v reads=%v", info, reads)
	}
	addr, found, err := keys.Lookup("key")
	if err != nil || !found || addr.FileID != cleaned.ID {
		t.Fatalf("relocated key: %+v %v %v", addr, found, err)
	}
	value, err := reopened.Read(At(reopened.Segments[addr.FileID], addr), "key")
	if err != nil || string(value) != "latest" {
		t.Fatalf("relocated value: %q %v", value, err)
	}
}

func TestIndexRefreshBoundsAndReplacesItsAncestors(t *testing.T) {
	cfg := Config{IndexedOpen: true, Handles: 1, MaxRecordBytes: 1 << 20, ValueCacheBudgetBytes: 4 << 20, TargetSegmentBytes: 4 << 20}
	dir := t.TempDir()
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	healthy := func() error { return nil }
	appendAndRotate := func(kind uint8, value string) {
		t.Helper()
		if _, err := s.Append(kind, []byte("key"), []byte(value), healthy); err != nil {
			t.Fatal(err)
		}
		if err := s.Rotate(); err != nil {
			t.Fatal(err)
		}
	}
	appendAndRotate(format.Put, "original")
	installFastTestIndex(t, s)
	lineage := []uint64{s.Catalog.Index.ID}
	appendAndRotate(format.Delete, "")
	installTestIndexLayer(t, s)
	lineage = append(lineage, s.Catalog.Index.ID)
	empty, err := s.openIndexFile(t.Context(), s.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if err := empty.fd.Close(); err != nil {
		t.Fatal(err)
	}
	if empty.tree.Root != 0 || empty.tree.Keys != 0 || empty.inherited == 0 {
		t.Fatalf("empty overlay layout=%+v, inherits %d", empty.tree, empty.inherited)
	}
	for i := 1; i < maxIndexLayers; i++ {
		appendAndRotate(format.Put, "next")
		installTestIndexLayer(t, s)
		lineage = append(lineage, s.Catalog.Index.ID)
	}
	for _, id := range lineage {
		if _, err := os.Stat(filepath.Join(dir, IndexName(s.Current.UUID, id))); err != nil {
			t.Fatalf("selected ancestor %d: %v", id, err)
		}
	}
	appendAndRotate(format.Put, "final")
	work, err := s.BeginIndexCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	err = work.WriteIncremental(t.Context(), btree.MinBudget)
	if release := work.Release(); release != nil {
		t.Fatal(release)
	}
	if !errors.Is(err, errIndexLayerFull) {
		t.Fatalf("additional overlay: %v", err)
	}
	installFastTestIndex(t, s)
	for _, id := range lineage {
		if _, err := os.Stat(filepath.Join(dir, IndexName(s.Current.UUID, id))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("replaced ancestor %d remains: %v", id, err)
		}
	}
}
