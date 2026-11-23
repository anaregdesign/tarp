package qualification

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

type VMConfig struct {
	BootDir string
	CaseDir string
	Input   string
	Serial  string
}

type VMResult struct {
	Cut     bool          `json:"cut"`
	Elapsed time.Duration `json:"elapsed_ns"`
	Before  Stats         `json:"before_cut"`
	After   Stats         `json:"after_cut"`
	Exit    string        `json:"qemu_exit"`
	Backend string        `json:"backend_exit"`
}

// RunVM boots only file-backed TCG devices, never KVM or a host block device.
// Observe must durably record its witness before asking for a cut. That cut
// kills the complete guest/QEMU, waits for NBD quiescence, then loses dirty RAM.
func RunVM(ctx context.Context, cfg VMConfig, disk *Disk, observe func(string) (bool, error)) (result VMResult, err error) {
	if ctx == nil || observe == nil {
		return result, errors.New("qualification: context and observer are required")
	}
	if _, ok := ctx.Deadline(); !ok {
		return result, errors.New("qualification: a finite VM deadline is required")
	}
	socket := filepath.Join(cfg.CaseDir, "nbd.sock")
	if !filepath.IsAbs(socket) || len(socket) > 100 {
		return result, errors.New("qualification: private absolute socket path must be below 101 bytes")
	}
	if err := os.Mkdir(cfg.CaseDir, 0700); err != nil {
		return result, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	root := filepath.Join(cfg.CaseDir, "root.qcow2")
	create := exec.CommandContext(ctx, "qemu-img", "create", "-f", "qcow2", "-F", "qcow2",
		"-b", filepath.Join(cfg.BootDir, "base.qcow2"), root)
	if output, err := create.CombinedOutput(); err != nil {
		return result, fmt.Errorf("qualification: create root overlay: %w: %s", err, output)
	}
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return result, err
	}
	defer func() {
		if closeErr := listener.Close(); !errors.Is(closeErr, net.ErrClosed) {
			err = errors.Join(err, closeErr)
		}
	}()
	serverDone := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		serverDone <- ServeNBD(ctx, conn, disk)
	}()
	stopAccept := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopAccept()
	serial, err := os.OpenFile(cfg.Serial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, serial.Sync(), serial.Close()) }()
	reader, writer, err := os.Pipe()
	if err != nil {
		return result, err
	}
	defer reader.Close()
	uri := (&url.URL{Scheme: "nbd+unix", Path: "/tarp", RawQuery: url.Values{"socket": {socket}}.Encode()}).String()
	cmd := exec.CommandContext(ctx, "qemu-system-x86_64",
		"-machine", "q35,accel=tcg", "-cpu", "max", "-smp", "2", "-m", "1024",
		"-display", "none", "-serial", "stdio", "-monitor", "none", "-nic", "none", "-no-reboot",
		"-kernel", filepath.Join(cfg.BootDir, "vmlinuz"), "-initrd", filepath.Join(cfg.BootDir, "initrd"),
		"-append", "console=ttyS0 rdinit=/tarp-init -- disposable-vm-only",
		"-drive", "file="+root+",format=qcow2,if=virtio,cache=writeback",
		"-drive", "file="+uri+",format=raw,if=virtio,cache=writeback",
		"-virtfs", "local,path="+cfg.Input+",mount_tag=tarpinput,security_model=none,readonly=on")
	cmd.Stdout, cmd.Stderr = writer, serial
	if err := cmd.Start(); err != nil {
		_ = writer.Close()
		return result, err
	}
	if err := writer.Close(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return result, err
	}
	begin := time.Now()
	wait := make(chan error, 1)
	go func() { wait <- cmd.Wait() }()
	trigger := make(chan error, 1)
	readDone := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 4096), 1<<20)
		triggered := false
		for scanner.Scan() {
			if _, err := fmt.Fprintln(serial, scanner.Text()); err != nil {
				readDone <- err
				return
			}
			if payload, ok := ConsolePayload(scanner.Text()); ok && !triggered {
				cut, err := observe(payload)
				if err != nil || cut {
					trigger <- err
					triggered = true
				}
			}
		}
		readDone <- scanner.Err()
	}()
	var waitErr error
	select {
	case triggerErr := <-trigger:
		result.Before = disk.Stats()
		killErr := cmd.Process.Kill()
		waitErr = <-wait
		result.Cut = triggerErr == nil && killErr == nil
		err = errors.Join(triggerErr, killErr)
	case waitErr = <-wait:
	case <-ctx.Done():
		cancel()
		waitErr = <-wait
		err = ctx.Err()
	}
	cancel()
	readErr := <-readDone
	serverErr := <-serverDone
	result.Backend = fmt.Sprint(serverErr)
	if errors.Is(serverErr, io.EOF) || errors.Is(serverErr, net.ErrClosed) || errors.Is(serverErr, context.Canceled) {
		serverErr = nil
	}
	if result.Cut && (errors.Is(serverErr, syscall.EPIPE) || errors.Is(serverErr, syscall.ECONNRESET) ||
		errors.Is(serverErr, io.ErrUnexpectedEOF)) {
		serverErr = nil
	}
	result.Elapsed, result.Exit = time.Since(begin), fmt.Sprint(waitErr)
	if result.Cut {
		var exited *exec.ExitError
		if !errors.As(waitErr, &exited) {
			err = errors.Join(err, errors.New("qualification: cut did not terminate QEMU abnormally"), waitErr)
		} else if status, ok := exited.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			err = errors.Join(err, errors.New("qualification: QEMU did not exit from the requested abrupt kill"), waitErr)
		}
		result.After = disk.PowerCut()
	} else {
		err = errors.Join(err, waitErr)
	}
	return result, errors.Join(err, readErr, serverErr)
}
