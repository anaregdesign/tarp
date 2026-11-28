//go:build darwin || linux

package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
)

const descriptorRows = 80

func TestDataReadFileDescriptorPressure(t *testing.T) {
	const fixtureEnv = "TARP_DESCRIPTOR_PRESSURE_FIXTURE"
	opts := Options{MaxRecordBytes: 128, TargetSegmentBytes: 192, OpenValidation: Indexed}
	value := []byte(strings.Repeat("v", 70))
	if dir := os.Getenv(fixtureEnv); dir != "" {
		checkDescriptorPressure(t, dir, opts, value)
		return
	}
	db := newTestDBWithHooks(t, opts, nil, parkedMaintenance())
	for i := range descriptorRows {
		if err := db.Put(descriptorKey(i), value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Lower the process-global limit after Go has initialized in a child.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestDataReadFileDescriptorPressure$", "-test.v")
	cmd.Env = append(os.Environ(), fixtureEnv+"="+db.store.Dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("descriptor-pressure child: %v\n%s", err, out)
	}
	t.Logf("descriptor-pressure child:\n%s", out)
}

func descriptorKey(i int) []byte { return fmt.Appendf(nil, "%03d", i) }

func checkDescriptorPressure(t *testing.T, dir string, opts Options, value []byte) {
	t.Helper()
	const descriptorLimit = 64
	var original syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &original); err != nil {
		t.Fatal(err)
	}
	if original.Cur < descriptorLimit {
		t.Skipf("file descriptor limit %d cannot support fixture setup", original.Cur)
	}
	limit := original
	limit.Cur = descriptorLimit
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &original); err != nil {
			t.Error(err)
		}
	}()
	open := func() *DB {
		db, err := OpenWithHooks(dir, opts, nil, parkedMaintenance())
		if err != nil {
			t.Fatal(err)
		}
		if db.opts.ReadFileHandles <= descriptorLimit {
			t.Fatalf("fixture requires the default cache limit above %d", descriptorLimit)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		return db
	}
	db := open()
	t.Run("evict idle handles during scan and concurrent reads", func(t *testing.T) {
		for range 12 {
			file, err := os.Open(os.DevNull)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = file.Close() })
		}
		rows := 0
		for row, err := range db.ScanRange(t.Context(), nil, nil, false) {
			if err != nil || !bytes.Equal(row.Key, descriptorKey(rows)) || !bytes.Equal(row.Value, value) {
				t.Fatalf("scan row %d: %+v, %v", rows, row, err)
			}
			rows++
		}
		if rows != descriptorRows {
			t.Fatalf("scanned %d records, want %d", rows, descriptorRows)
		}
		const readers = 4
		start := make(chan struct{})
		done := make(chan error, readers)
		for i := range readers {
			go func() {
				<-start
				keys := [][]byte{descriptorKey(i), descriptorKey(40 + i), descriptorKey(76 + i)}
				rows, err := db.BatchGet(keys)
				if err == nil {
					if len(rows) != len(keys) {
						err = fmt.Errorf("reader %d returned %d results", i, len(rows))
					}
					for _, row := range rows {
						if !row.Found || !bytes.Equal(row.Value, value) {
							err = fmt.Errorf("reader %d returned a wrong value", i)
						}
					}
				}
				done <- err
			}()
		}
		close(start)
		for range readers {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// A new handle starts with no cached DATA descriptor to evict.
	db = open()
	var held []*os.File
	defer func() {
		for _, file := range held {
			_ = file.Close()
		}
	}()
	for {
		file, err := os.Open(os.DevNull)
		if errors.Is(err, syscall.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, file)
		if len(held) > descriptorLimit {
			t.Fatal("process did not enforce its descriptor limit")
		}
	}
	key := descriptorKey(0)
	if _, err := db.Get(key); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("Get with no evictable descriptors: %v", err)
	}
	if rows, err := db.BatchGet([][]byte{key}); !errors.Is(err, syscall.EMFILE) || len(rows) != 0 {
		t.Fatalf("BatchGet with no evictable descriptors: %v, %v", rows, err)
	}
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if _, err := snapshot.Get(key); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("Snapshot.Get with no evictable descriptors: %v", err)
	}
	if err := db.health(); err != nil {
		t.Fatalf("descriptor pressure invalidated the DB: %v", err)
	}
	if len(held) == 0 {
		t.Fatal("fixture did not reserve any descriptors")
	}
	last := len(held) - 1
	if err := held[last].Close(); err != nil {
		t.Fatal(err)
	}
	held = held[:last]
	if got, err := db.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("retry after releasing a descriptor: %q, %v", got, err)
	}
}
