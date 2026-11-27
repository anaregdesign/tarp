package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

func TestIncrementalBackupPruningKeepsSharedAndRemovesUnreferencedObjects(t *testing.T) {
	repository := t.TempDir()
	lock, err := platform.Lock(repository)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	objects := filepath.Join(repository, incrementalObjectsDir, "00")
	if err := os.MkdirAll(objects, 0700); err != nil {
		t.Fatal(err)
	}
	object := filepath.Join(objects, "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff-64")
	if err := os.WriteFile(object, make([]byte, format.SegmentHeaderBytes), 0600); err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(repository, "generation-link")
	if err := os.Link(object, shared); err != nil {
		t.Fatal(err)
	}
	if err := PruneIncrementalBackupObjects(t.Context(), repository, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(object); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(shared); err != nil {
		t.Fatal(err)
	}
	if err := PruneIncrementalBackupObjects(t.Context(), repository, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(object); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unreferenced object lookup: %v", err)
	}
}
