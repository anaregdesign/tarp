package qualification

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVolatileWritesAndFlushedSurvival(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.raw")
	disk, err := CreateDisk(path, 3*overlayPageBytes)
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
	if err := disk.WriteAt(volatile, overlayPageBytes-128, false); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, len(volatile))
	if err := disk.ReadAt(read, overlayPageBytes-128); err != nil || !bytes.Equal(read, volatile) {
		t.Fatalf("volatile read coherence: %x %v", read, err)
	}
	cut := disk.PowerCut()
	if cut.DiscardedOverlayPageBytes != 2*overlayPageBytes || cut.DirtyOverlayPageBytes != 0 || cut.Flushes != 1 {
		t.Fatalf("cut statistics: %+v", cut)
	}
	persisted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]byte, 3*overlayPageBytes)
	copy(expected[1:], flushed)
	if !bytes.Equal(persisted, expected) {
		t.Fatal("flushed bytes did not survive exactly or unflushed bytes survived")
	}
	if err := disk.ReadAt(read, overlayPageBytes-128); err != nil || !bytes.Equal(read, make([]byte, len(read))) {
		t.Fatalf("cut failed to discard volatile bytes: %x %v", read, err)
	}
}

func TestFUAPersistsOnlyItsRequest(t *testing.T) {
	for _, offset := range []int64{1024, overlayPageBytes + 512, overlayPageBytes - 128} {
		t.Run(fmt.Sprint(offset), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "disk.raw")
			disk, err := CreateDisk(path, 3*overlayPageBytes)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := disk.Close(); err != nil {
					t.Error(err)
				}
			})
			for _, at := range []int64{0, overlayPageBytes + 2048} {
				if err := disk.WriteAt(bytes.Repeat([]byte{0xa3}, 512), at, false); err != nil {
					t.Fatal(err)
				}
			}
			fua := bytes.Repeat([]byte{0x71}, 512)
			if err := disk.WriteAt(fua, offset, true); err != nil {
				t.Fatal(err)
			}
			expected := make([]byte, 3*overlayPageBytes)
			copy(expected[offset:], fua)
			for _, cut := range []bool{false, true} {
				if cut {
					disk.PowerCut()
				}
				persisted, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(persisted, expected) {
					t.Fatalf("FUA persisted unrelated data or lost its own acknowledged bytes (cut=%t): %v", cut, err)
				}
			}
			if stats := disk.Stats(); stats.Flushes != 0 || stats.FUAWrites != 1 {
				t.Fatalf("FUA acted as a whole-device FLUSH: %+v", stats)
			}
		})
	}
}

func TestOverlappingFUAStaysCoherentThroughLaterFlush(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.raw")
	disk, err := CreateDisk(path, overlayPageBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	expected := bytes.Repeat([]byte{0xa3}, overlayPageBytes)
	if err := disk.WriteAt(expected, 0, false); err != nil {
		t.Fatal(err)
	}
	fua := bytes.Repeat([]byte{0x71}, 512)
	if err := disk.WriteAt(fua, 512, true); err != nil {
		t.Fatal(err)
	}
	copy(expected[512:], fua)
	read := make([]byte, overlayPageBytes)
	if err := disk.ReadAt(read, 0); err != nil || !bytes.Equal(read, expected) {
		t.Fatalf("FUA left a stale overlay: %v", err)
	}
	if err := disk.Flush(); err != nil {
		t.Fatal(err)
	}
	disk.PowerCut()
	persisted, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(persisted, expected) {
		t.Fatalf("later FLUSH reverted acknowledged FUA data: %v", err)
	}
}

func TestRejectedBlockRequestDoesNotMutate(t *testing.T) {
	disk, err := CreateDisk(filepath.Join(t.TempDir(), "disk.raw"), overlayPageBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	}()
	before := disk.Stats()
	for _, offset := range []int64{-1, overlayPageBytes - 1, overlayPageBytes + 1} {
		if err := disk.WriteAt([]byte("no"), offset, true); err == nil {
			t.Fatalf("accepted out-of-bounds offset %d", offset)
		}
	}
	if disk.Stats() != before {
		t.Fatal("invalid request changed volatile/durable state")
	}
}
