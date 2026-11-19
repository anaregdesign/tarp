package storage

import (
	"errors"
	"os"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestCheckpointDiscardsOnlyItsOwnSnapshotFiles(t *testing.T) {
	cfg := Config{CacheBytes: 1024, RecordBytes: 64, TargetSegmentBytes: format.SegmentHeaderBytes + 64, Handles: 1}
	store, _, err := Open(t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, key := range []string{"a", "b"} {
		if _, err := store.Append(format.Put, []byte(key), nil, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Rotate(); err != nil {
		t.Fatal(err)
	}
	capture, err := store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	moves, err := capture.Replay(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := store.NewCheckpoint(capture)
	var reserved []*File
	reserve := func() (*File, error) {
		file, err := store.ReserveSnapshotFile()
		if err != nil {
			return nil, err
		}
		reserved = append(reserved, file)
		if len(reserved) == 2 {
			// The exclusive create must leave this other file untouched.
			err = os.WriteFile(file.Path, []byte("existing"), 0600)
		}
		return file, err
	}
	if err := checkpoint.Copy(t.Context(), moves, reserve); !errors.Is(err, os.ErrExist) || len(reserved) != 2 {
		t.Fatalf("copy with %d reserved files: %v", len(reserved), err)
	}
	if err := checkpoint.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(reserved[0].Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unpublished owned file after discard: %v", err)
	}
	if data, err := os.ReadFile(reserved[1].Path); err != nil || string(data) != "existing" {
		t.Fatalf("preexisting file after discard: %q, %v", data, err)
	}
	for _, file := range reserved {
		if store.Files[file.ID] != nil {
			t.Fatalf("unpublished snapshot %d entered the file registry", file.ID)
		}
	}
}
