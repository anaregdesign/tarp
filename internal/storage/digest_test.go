package storage

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

// The substitute is a valid record of the same length, sequence and key.
// Its key and payload CRCs pass every decoder, including on a dead record.
func TestSealedDigestRejectsSubstitutionBeforeMaintenancePublication(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 4096, RecordBytes: 128, TargetSegmentBytes: 512, Handles: 1}
	store, err := openTestStore(t, t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	healthy := func() error { return nil }
	live := memory.New()
	defer live.Close()
	tx := live.Begin()
	for _, value := range []string{"original", "new head"} {
		loc, err := store.Append(format.Put, []byte("key"), []byte(value), healthy)
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Put("key", loc.Address()); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	if err := store.Rotate(); err != nil {
		t.Fatal(err)
	}
	before := store.Current
	source := store.Catalog.Files[0]
	path := store.Files[source.ID].Path
	b, err := os.ReadFile(path)
	if err != nil || sha256.Sum256(b) != source.Digest {
		t.Fatalf("sealed bytes %x: %v", source.Digest, err)
	}
	copy(b[format.SegmentHeaderBytes:], format.EncodeRecord(format.Put, 1, []byte("key"), []byte("tampered")))
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(t.Context(), store.Dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("validation: %v", err)
	}
	capture, err := store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	view := live.View()
	defer view.Close()
	moves := store.NewRelocations()
	defer moves.Close()
	work := memory.New()
	defer work.Close()
	compact := store.NewCheckpoint(capture)
	if err := compact.Copy(t.Context(), work, view, moves, store.ReserveSnapshotFile); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("compaction: %v", err)
	}
	if err := compact.Discard(); err != nil {
		t.Fatal(err)
	}
	clean, err := store.BeginClean([]format.File{source})
	if err != nil {
		t.Fatal(err)
	}
	defer clean.Release()
	if err := clean.Copy(t.Context(), store.IO, moves, work, healthy, func(string, Location) (bool, error) { return true, nil }); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("cleaning: %v", err)
	}
	if err := clean.Discard(store.IO); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "backup")
	if err := Backup(t.Context(), capture, cfg, store.IO, destination, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("backup: %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup visible: %v", err)
	}
	if before != store.Current {
		t.Fatal("selected frontier changed")
	}
}
