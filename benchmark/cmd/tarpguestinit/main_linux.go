// Command tarpguestinit is PID 1 only inside the explicitly marked disposable
// qualification guest. It boots the pinned Ubuntu root offline without systemd.
package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "TARP_INIT_FAILURE", err)
		if os.Getpid() != 1 {
			os.Exit(1)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
}

func run() error {
	if os.Getpid() != 1 || len(os.Args) != 2 || os.Args[1] != "disposable-vm-only" {
		return errors.New("requires PID 1 and explicit disposable-VM kernel argument")
	}
	var uname syscall.Utsname
	if err := syscall.Uname(&uname); err != nil {
		return err
	}
	var release strings.Builder
	for _, c := range uname.Release {
		if c == 0 {
			break
		}
		release.WriteByte(byte(c))
	}
	if !strings.HasPrefix(release.String(), "6.8.") {
		return errors.New("requires the pinned Ubuntu 6.8-series guest kernel")
	}
	for _, mount := range []struct{ source, target, kind string }{
		{"proc", "/proc", "proc"}, {"sysfs", "/sys", "sysfs"}, {"devtmpfs", "/dev", "devtmpfs"},
	} {
		if err := os.MkdirAll(mount.target, 0755); err != nil {
			return err
		}
		if err := syscall.Mount(mount.source, mount.target, mount.kind, 0, ""); err != nil {
			return err
		}
	}
	if err := command("/sbin/modprobe", "-a", "virtio_pci", "virtio_blk", "ext4"); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat("/dev/vda1"); err == nil {
			break
		} else if !os.IsNotExist(err) || time.Now().After(deadline) {
			return fmt.Errorf("root device not ready: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := os.Mkdir("/newroot", 0755); err != nil {
		return err
	}
	if err := syscall.Mount("/dev/vda1", "/newroot", "ext4", syscall.MS_RDONLY, ""); err != nil {
		return err
	}
	for _, path := range []string{"proc", "sys", "dev"} {
		if err := syscall.Mount("/"+path, filepath.Join("/newroot", path), "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
			return err
		}
	}
	if err := syscall.Chroot("/newroot"); err != nil {
		return err
	}
	if err := os.Chdir("/"); err != nil {
		return err
	}
	for _, path := range []string{"/mnt", "/tmp", "/run"} {
		if err := syscall.Mount("tmpfs", path, "tmpfs", 0, "size=64m,mode=1777"); err != nil {
			return err
		}
	}
	if err := command("/sbin/modprobe", "-a", "9pnet_virtio", "9p"); err != nil {
		return err
	}
	if err := os.Mkdir("/mnt/tarp-input", 0755); err != nil {
		return err
	}
	if err := syscall.Mount("tarpinput", "/mnt/tarp-input", "9p", syscall.MS_RDONLY, "trans=virtio,version=9p2000.L"); err != nil {
		return err
	}
	if err := os.Setenv("TARP_QUAL_GUEST_CONFIRM", "disposable-vm-only"); err != nil {
		return err
	}
	err := command("/bin/bash", "/mnt/tarp-input/guest.sh")
	if err != nil {
		return err
	}
	syscall.Sync()
	return syscall.Reboot(syscall.LINUX_REBOOT_CMD_POWER_OFF)
}

func command(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr, cmd.Stdin = os.Stdout, os.Stderr, os.Stdin
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	return nil
}
