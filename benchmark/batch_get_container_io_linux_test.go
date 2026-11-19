package benchmark

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// containerBatchIO operates only on the regular DATA files of a private,
// already populated fixture. Keep the same descriptors across read-only trials.
type containerBatchIO struct {
	Filesystem string
	Device     string
	files      []*os.File
	deviceID   string // mountinfo major:minor for optional whole-device statistics
}

func openContainerBatchIO(dir string) (*containerBatchIO, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	b := &containerBatchIO{}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "DATA-") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if !entry.Type().IsRegular() {
			return nil, errors.Join(fmt.Errorf("non-regular fixture DATA file %s", path), b.Close())
		}
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return nil, errors.Join(err, b.Close())
		}
		b.files = append(b.files, file)
	}
	if len(b.files) == 0 {
		return nil, fmt.Errorf("no DATA files in fixture %s", dir)
	}
	if err := b.identifyMount(); err != nil {
		b.Filesystem = "unknown"
		var fs syscall.Statfs_t
		if syscall.Fstatfs(int(b.files[0].Fd()), &fs) == nil {
			b.Filesystem = fmt.Sprintf("magic %#x", fs.Type)
		}
		b.Device = "unavailable: " + err.Error()
	}
	return b, nil
}

func (b *containerBatchIO) identifyMount() error {
	// The mount ID identifies the actual mount, including a bind-mounted
	// volume whose mountpoint shares a path prefix with another mount.
	fdinfo, err := os.ReadFile(fmt.Sprintf("/proc/self/fdinfo/%d", b.files[0].Fd()))
	if err != nil {
		return err
	}
	mountID, err := containerBatchCounter(fdinfo, "mnt_id")
	if err != nil {
		return err
	}
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	want := strconv.FormatUint(mountID, 10)
	for line := range strings.SplitSeq(string(mounts), "\n") {
		left, right, ok := strings.Cut(line, " - ")
		before, after := strings.Fields(left), strings.Fields(right)
		if ok && len(before) >= 3 && len(after) >= 2 && before[0] == want {
			b.Filesystem = after[0]
			b.deviceID = before[2]
			b.Device = after[1] + " (" + b.deviceID + ")"
			return nil
		}
	}
	return fmt.Errorf("fixture mount ID %d absent from /proc/self/mountinfo", mountID)
}

func (b *containerBatchIO) Close() error {
	var err error
	for _, file := range b.files {
		err = errors.Join(err, file.Close())
	}
	b.files = nil
	return err
}

func (b *containerBatchIO) Warm() error {
	for _, file := range b.files {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.Copy(io.Discard, file); err != nil {
			return err
		}
	}
	return nil
}

func (b *containerBatchIO) Evict() error {
	for _, file := range b.files {
		// Linux can retain dirty pages despite POSIX_FADV_DONTNEED.
		if err := file.Sync(); err != nil {
			return err
		}
		if err := adviseContainerBatchDontNeed(file.Fd()); err != nil {
			return fmt.Errorf("POSIX_FADV_DONTNEED %s: %w", file.Name(), err)
		}
	}
	return nil
}

func containerBatchCounter(data []byte, name string) (uint64, error) {
	for line := range strings.SplitSeq(string(data), "\n") {
		field, value, ok := strings.Cut(line, ":")
		if ok && field == name {
			return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
		}
	}
	return 0, fmt.Errorf("missing Linux counter %q", name)
}

// ReadBytes is storage read I/O charged to this process by the Linux kernel.
// A delta covers all threads and read-ahead, and counts bytes at the kernel's
// accounting layer. It does not measure physical media operations per key.
func (b *containerBatchIO) ReadBytes() (uint64, error) {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, err
	}
	return containerBatchCounter(data, "read_bytes")
}

// DeviceReadBytes is optional corroboration. It includes all other activity
// on this backing block device. Some mounts have no block device in sysfs.
func (b *containerBatchIO) DeviceReadBytes() (uint64, error) {
	if b.deviceID == "" {
		return 0, fmt.Errorf("fixture block device unavailable: %w", errors.ErrUnsupported)
	}
	data, err := os.ReadFile(filepath.Join("/sys/dev/block", b.deviceID, "stat"))
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 3 {
		return 0, fmt.Errorf("missing sectors-read field for device %s", b.deviceID)
	}
	sectors, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return 0, err
	}
	// Linux block layer statistics always express sectors in 512-byte units.
	return sectors * 512, nil
}

func TestContainerBatchCacheControlRejectsLinkedDATA(t *testing.T) {
	other := filepath.Join(t.TempDir(), "other")
	if err := os.WriteFile(other, []byte("outside the fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	fixture := t.TempDir()
	if err := os.Symlink(other, filepath.Join(fixture, "DATA-link")); err != nil {
		t.Fatal(err)
	}
	if got, err := openContainerBatchIO(fixture); err == nil {
		got.Close()
		t.Fatal("opened a fixture DATA symlink")
	}
}

func TestContainerBatchCounterRequiresStorageAccounting(t *testing.T) {
	// rchar also counts cache hits. An absent read_bytes field must never be
	// interpreted as a successful read of zero charged storage bytes.
	const cached = "rchar: 4096\nsyscr: 2\n"
	if _, err := containerBatchCounter([]byte(cached), "read_bytes"); err == nil {
		t.Fatal("missing storage I/O counter was accepted")
	}
	got, err := containerBatchCounter([]byte(cached+"read_bytes: 0\n"), "read_bytes")
	if err != nil || got != 0 {
		t.Fatalf("explicit zero read_bytes: got %d, %v", got, err)
	}
}
