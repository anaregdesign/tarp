package engine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestBackupBatchesRecordsAcrossBlockBoundaries(t *testing.T) {
	var reads, writes int
	var copied int64
	fs := &storage.IO{
		CleanerStart: make(chan struct{}),
		Hook: func(point string) error {
			if point == "backup.read:after" {
				reads++
			}
			return nil
		},
		Wrote: func(point string, n int) {
			if point == "backup.copy" {
				writes++
				copied += int64(n)
			}
		},
	}
	db := newTestDB(t, Options{}, fs)
	// This file crosses multiple buffer boundaries inside headers, keys and
	// values. Its physical tail may grow while Backup reads its captured prefix.
	keys := [][]byte{[]byte("first"), bytes.Repeat([]byte("k"), 33<<10), []byte("last")}
	values := [][]byte{bytes.Repeat([]byte("a"), (32<<10)-len(keys[0])-format.RecordHeaderBytes-8),
		bytes.Repeat([]byte("b"), 65<<10), []byte("last value")}
	for i, k := range keys {
		if err := db.Put(k, values[i]); err != nil {
			t.Fatal(err)
		}
	}
	end := db.store.Current.ActiveEnd
	seq := db.store.Current.Sequence
	// An uncommitted active-file suffix is outside the permitted read window.
	fd, err := os.OpenFile(db.index["last"].File.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fd.WriteAt(bytes.Repeat([]byte("tail"), 9000), end); err != nil {
		t.Fatal(err)
	}
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if copied != end || reads == 0 || reads != writes-1 || reads >= 10 {
		t.Fatalf("captured end %d, copied %d with %d payload reads and %d writes", end, copied, reads, writes)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(t.Context(), target, restored, Options{}); err != nil {
		t.Fatal(err)
	}
	clone, err := Open(restored, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	if clone.store.Current.Sequence != seq {
		t.Fatalf("restored sequence %d, want %d", clone.store.Current.Sequence, seq)
	}
	for i, k := range keys {
		v, err := clone.Get(k)
		if err != nil || !bytes.Equal(v, values[i]) {
			t.Fatalf("restored value %d mismatch: %v", i, err)
		}
	}
}

func TestBackupPayloadWriteFailureLeavesSourceHealthy(t *testing.T) {
	writes := 0
	db := newTestDB(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "backup.copy:before" {
			writes++
			if writes == 2 { // The segment header has reached the staging file.
				return syscall.ENOSPC
			}
		}
		return nil
	}})
	mustPut(t, db, "key", "value")
	target := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), target); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("payload write returned %v", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup publication after failed payload write: %v", err)
	}
	mustPut(t, db, "key", "new")
}

func TestBackupRejectsCorruptionInFinalBlock(t *testing.T) {
	db := newTestDB(t, Options{}, nil)
	mustPut(t, db, "multi-block", strings.Repeat("v", 80<<10))
	file := db.index["multi-block"].File
	fd, err := os.OpenFile(file.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fd.WriteAt([]byte("!"), db.store.Current.ActiveEnd-1)
	if err := errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), target); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupted final block returned %v", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup publication after checksum failure: %v", err)
	}
	if _, err := db.Get([]byte("multi-block")); !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("expected unhealthy source after checksum failure, got %v", err)
	}
}
