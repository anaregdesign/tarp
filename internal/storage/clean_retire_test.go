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
