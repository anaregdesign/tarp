// Package guesttest prepares the disposable Linux guest used by the
// measurement and storage qualification test binaries.
package guesttest

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
)

// RequireDisposableGuest checks the opt-in, PID 1 and disposable block device.
func RequireDisposableGuest(t *testing.T) {
	t.Helper()
	if os.Getenv("TARP_QUAL_GUEST_CONFIRM") != "disposable-vm-only" || runtime.GOARCH != "amd64" {
		t.Fatal("requires the explicitly guarded Linux amd64 guest")
	}
	initArgs, err := os.ReadFile("/proc/1/cmdline")
	if err != nil || !bytes.Equal(initArgs, []byte("/tarp-init\x00disposable-vm-only\x00")) {
		t.Fatalf("requires the explicitly guarded qualification PID 1: %v", err)
	}
	size, err := os.ReadFile("/sys/block/vdb/size")
	if err != nil || string(bytes.TrimSpace(size)) != "524288" {
		t.Fatalf("requires the disposable 256 MiB vdb: %v", err)
	}
}

// MountTarget mounts the qualified filesystem on the guarded guest device.
func MountTarget(t *testing.T, format bool, name string, seed uint64) {
	t.Helper()
	RequireDisposableGuest(t)
	release, err := os.ReadFile("/etc/os-release")
	kernel, kernelErr := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil || kernelErr != nil || !bytes.Contains(release, []byte("\nID=ubuntu\n")) ||
		!bytes.Contains(release, []byte("\nVERSION_ID=\"24.04\"\n")) || !bytes.HasPrefix(kernel, []byte("6.8.")) {
		t.Fatalf("requires pinned Ubuntu 24.04 / 6.8 target: %v", errors.Join(err, kernelErr))
	}
	if format {
		guestCommand(t, "/usr/sbin/mkfs.ext4", "-q", "-F", "-b", "4096", "-m", "0",
			"-E", "lazy_itable_init=0,lazy_journal_init=0", "/dev/vdb")
	}
	if err := os.Mkdir("/mnt/tarp-data", 0700); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mount("/dev/vdb", "/mnt/tarp-data", "ext4", 0, "data=ordered"); err != nil {
		t.Fatal(err)
	}
	mount, err := exec.Command("/usr/bin/findmnt", "-rn", "-o", "SOURCE,FSTYPE,OPTIONS", "/mnt/tarp-data").Output()
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Fields(string(mount))
	if len(fields) != 3 || fields[0] != "/dev/vdb" || fields[1] != "ext4" ||
		!strings.Contains(","+fields[2]+",", ",data=ordered,") {
		t.Fatalf("unexpected selected mount: %s", mount)
	}
	for _, flag := range strings.Split(fields[2], ",") {
		if flag == "nobarrier" || flag == "barrier=0" || flag == "ro" || strings.HasPrefix(flag, "dax") {
			t.Fatalf("unsupported mount option %s", flag)
		}
	}
	fmt.Printf("Go=%s GOMAXPROCS=%d case=%s seed=%d\n%s", runtime.Version(), runtime.GOMAXPROCS(0), name, seed, mount)
	guestCommand(t, "/usr/bin/uname", "-a")
	guestCommand(t, "/usr/sbin/tune2fs", "-l", "/dev/vdb")
	fmt.Println("TARP_ENVIRONMENT_PASS")
}

func guestCommand(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// AvailableBytes reads the available space on the mounted guest filesystem.
func AvailableBytes(t *testing.T) uint64 {
	t.Helper()
	var stat syscall.Statfs_t
	if err := syscall.Statfs("/mnt/tarp-data", &stat); err != nil {
		t.Fatal(err)
	}
	return stat.Bavail * uint64(stat.Bsize)
}
