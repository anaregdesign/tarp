package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckpointRetiresAllKnownFilesWithOneDirectorySync(t *testing.T) {
	var syncs int
	fs := &IO{Hook: func(point string) error {
		if point == "clean.retire.dirsync:before" {
			syncs++
		}
		return nil
	}}
	s, err := openTestStore(t, t.TempDir(), Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 64, TargetSegmentBytes: 1024, Handles: 1}, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for id := uint64(100); id < 140; id++ {
		if err := os.WriteFile(filepath.Join(s.Dir, ManifestName(s.Current.UUID, id)), []byte{'x'}, 0600); err != nil {
			t.Fatal(err)
		}
		s.retireManifest(id)
	}
	removed, pinned, err := s.RetireAfterCheckpoint()
	if err != nil || removed != 40 || pinned != 0 || syncs != 1 || s.OrderedCleanupPending() {
		t.Fatalf("retirement removed %d bytes, pinned %d, syncs %d, pending %t: %v", removed, pinned, syncs, s.OrderedCleanupPending(), err)
	}
}

func TestAutomaticRetirementVisitsBoundedDataAndPreservesPins(t *testing.T) {
	s, err := openTestStore(t, t.TempDir(), Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 64, TargetSegmentBytes: 1024, Handles: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const count = 2*autoRetirementVisits + 1
	const first = 100
	probes := 0
	retain := true
	s.ReaderRetainsData = func(id uint64) bool {
		probes++
		return retain && id != first+count-1
	}
	for id := uint64(first); id < first+count; id++ {
		path := filepath.Join(s.Dir, DataName(s.Current.UUID, id))
		if err := os.WriteFile(path, []byte{'x'}, 0600); err != nil {
			t.Fatal(err)
		}
		segment := &DataSegment{ID: id, Path: path}
		segment.Size.Store(1)
		s.Segments[id] = segment
		s.retireData(segment)
	}
	for pass := range 2 {
		probes = 0
		removed, pinned, pending, err := s.RetireCleaned()
		if err != nil || removed != 0 || pinned != 0 || !pending || !s.RetirementSweepPending() || probes != autoRetirementVisits {
			t.Fatalf("pass %d visited %d files, removed %d, pinned %d, pending %t: %v", pass, probes, removed, pinned, pending, err)
		}
	}
	probes = 0
	removed, pinned, pending, err := s.RetireCleaned()
	if err != nil || removed != 1 || pinned != count-1 || !pending || s.RetirementSweepPending() || probes != 1 {
		t.Fatalf("last pass visited %d files, removed %d, pinned %d, pending %t: %v", probes, removed, pinned, pending, err)
	}
	if _, err := os.Stat(s.Segments[first].Path); err != nil {
		t.Fatalf("pinned DATA was removed: %v", err)
	}

	// A complete cleanup starts a new observation even if the bounded pass
	// left a partly visited sweep with earlier pin observations.
	s.RetireCleaned()
	retain = false
	removed, pinned, err = s.RetireAfterCheckpoint()
	if err != nil || removed != count-1 || pinned != 0 || s.OrderedCleanupPending() || s.RetirementSweepPending() {
		t.Fatalf("complete cleanup removed %d, pinned %d: %v", removed, pinned, err)
	}
}
