package qualification

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestVolatileWritesAndFlushedSurvival(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.raw")
	disk, err := CreateDisk(path, 3*pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	flushed := bytes.Repeat([]byte{0x71}, 512)
	volatile := bytes.Repeat([]byte{0xa3}, 512)
	if err := disk.WriteAt(flushed, 1, false); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, make([]byte, len(before))) {
		t.Fatalf("ordinary write escaped volatile RAM: %v", err)
	}
	if err := disk.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := disk.WriteAt(volatile, pageBytes-128, false); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, len(volatile))
	if err := disk.ReadAt(read, pageBytes-128); err != nil || !bytes.Equal(read, volatile) {
		t.Fatalf("volatile read coherence: %x %v", read, err)
	}
	cut := disk.PowerCut()
	if cut.DiscardedBytes != 2*pageBytes || cut.DirtyBytes != 0 || cut.Flushes != 1 {
		t.Fatalf("cut statistics: %+v", cut)
	}
	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]byte, 3*pageBytes)
	copy(expected[1:], flushed)
	if !bytes.Equal(persisted, expected) {
		t.Fatal("flushed bytes did not survive exactly or unflushed bytes survived")
	}
	if err := disk.ReadAt(read, pageBytes-128); err != nil || !bytes.Equal(read, make([]byte, len(read))) {
		t.Fatalf("cut failed to discard volatile bytes: %x %v", read, err)
	}
}

func TestRejectedBlockRequestDoesNotMutate(t *testing.T) {
	disk, err := CreateDisk(filepath.Join(t.TempDir(), "disk.raw"), pageBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	}()
	before := disk.Stats()
	for _, offset := range []int64{-1, pageBytes - 1, pageBytes + 1} {
		if err := disk.WriteAt([]byte("no"), offset, true); err == nil {
			t.Fatalf("accepted out-of-bounds offset %d", offset)
		}
	}
	if disk.Stats() != before {
		t.Fatal("invalid request changed volatile/durable state")
	}
}
