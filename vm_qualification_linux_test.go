package tarp

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestVMBlockControls(t *testing.T) {
	if os.Getenv("TARP_VM_BLOCK_CONTROL") != "disposable-vm-only" ||
		os.Getenv("TARP_QUAL_GUEST_CONFIRM") != "disposable-vm-only" {
		t.Skip("requires explicit disposable guest block-control opt-in")
	}
	initArgs, err := os.ReadFile("/proc/1/cmdline")
	if err != nil || !bytes.Equal(initArgs, []byte("/tarp-init\x00disposable-vm-only\x00")) {
		t.Fatalf("requires the explicitly guarded qualification PID 1: %v", err)
	}
	size, err := os.ReadFile("/sys/block/vdb/size")
	if err != nil || string(bytes.TrimSpace(size)) != "524288" {
		t.Fatalf("requires the disposable 256 MiB vdb: %v", err)
	}
	buffer, err := syscall.Mmap(-1, 0, 4096, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		t.Fatal(err)
	}
	fd, err := os.OpenFile("/dev/vdb", os.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range buffer[:512] {
		buffer[i] = 0x71
	}
	if n, err := fd.WriteAt(buffer[:512], 65536); err != nil || n != 512 {
		t.Fatalf("flushed control write: %d %v", n, err)
	}
	if err := fd.Sync(); err != nil {
		t.Fatal(err)
	}
	for i := range buffer[:512] {
		buffer[i] = 0xa3
	}
	if n, err := fd.WriteAt(buffer[:512], 131072); err != nil || n != 512 {
		t.Fatalf("volatile control write: %d %v", n, err)
	}
	fmt.Println("TARP_CONTROL_READY")
	// Keep the raw descriptor open: closing the last block-device handle can
	// itself flush. Only the external controller terminates this whole VM.
	for {
		time.Sleep(time.Hour)
	}
}
