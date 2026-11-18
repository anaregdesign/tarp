package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestProcessCrashRecoveryMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip("subprocess crash sweep")
	}
	cases := []struct {
		mode, point string
		occurrence  int
	}{
		{"put", "data.write:before", 1}, {"put", "data.write:after", 1},
		{"put", "data.sync:after", 1}, {"put", "current.write:after", 1},
		{"put", "current.sync:after", 1}, {"put", "current.rename:before", 1},
		{"put", "current.rename:after", 1}, {"put", "current.dirsync:after", 1},
		{"rotate", "segment.write:before", 1}, {"rotate", "data.write:after", 1}, {"rotate", "segment.sync:after", 1},
		{"rotate", "catalog.write:after", 1}, {"rotate", "catalog.sync:after", 1},
		{"rotate", "catalog.dirsync:after", 1}, {"rotate", "current.rename:before", 1},
		{"rotate", "current.rename:after", 1}, {"rotate", "current.dirsync:after", 1},
		{"compact", "compact.segment.write:after", 1}, {"compact", "compact.write:after", 1},
		{"compact", "compact.sync:after", 1}, {"compact", "current.rename:after", 2},
		{"compact", "compact.remap:after", 1}, {"compact", "cleanup.unlink:after", 1},
		{"backup", "backup.copy:after", 1}, {"backup", "backup.file.sync:after", 1},
		{"backup", "backup.manifest.sync:after", 1}, {"backup", "backup.rename:after", 1},
		{"backup", "backup.parent.sync:after", 1},
		{"restore", "restore.copy:after", 1}, {"restore", "restore.file.sync:after", 1},
		{"restore", "restore.stage.sync:after", 1}, {"restore", "restore.rename:after", 1},
		{"restore", "restore.parent.sync:after", 1},
	}
	for _, tc := range cases {
		t.Run(tc.mode+"/"+tc.point, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "db")
			db, err := Open(dir, testOptions())
			if err != nil {
				t.Fatal(err)
			}
			mustPut(t, db, "k", "old")
			if tc.mode == "rotate" {
				mustPut(t, db, "pad", string(make([]byte, 60)))
			}
			if tc.mode == "restore" {
				if err := db.Backup(context.Background(), filepath.Join(parent, "backup")); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashHelper$")
			cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0", "TARP_CRASH_CHILD=1", "TARP_CRASH_DIR="+parent,
				"TARP_CRASH_MODE="+tc.mode, "TARP_CRASH_POINT="+tc.point, "TARP_CRASH_N="+strconv.Itoa(tc.occurrence))
			output, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 91 {
				t.Fatalf("fault point not exercised: %v\n%s", err, output)
			}
			reopened, err := Open(dir, testOptions())
			if err != nil {
				t.Fatalf("recovery: %v\n%s", err, output)
			}
			// This child exits without stopping the kernel. A completed rename is
			// visible on reopen even if the process exited before directory sync.
			want := "old"
			if (tc.mode == "put" || tc.mode == "rotate") && (tc.point == "current.rename:after" || tc.point == "current.dirsync:after") {
				want = "new"
			}
			wantValue(t, reopened, "k", want)
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if tc.mode == "backup" {
				if _, err := os.Stat(filepath.Join(parent, "output")); err == nil {
					if err := Restore(context.Background(), filepath.Join(parent, "output"), filepath.Join(parent, "restored"), testOptions()); err != nil {
						t.Fatalf("published incomplete backup: %v", err)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
			}
			if tc.mode == "restore" {
				if _, err := os.Stat(filepath.Join(parent, "output")); err == nil {
					got, err := Open(filepath.Join(parent, "output"), testOptions())
					if err != nil {
						t.Fatal(err)
					}
					wantValue(t, got, "k", "old")
					if err := got.Close(); err != nil {
						t.Fatal(err)
					}
				} else if !errors.Is(err, os.ErrNotExist) {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestCrashHelper(t *testing.T) {
	if os.Getenv("TARP_CRASH_CHILD") != "1" {
		t.Skip("subprocess helper")
	}
	n, err := strconv.Atoi(os.Getenv("TARP_CRASH_N"))
	if err != nil || n < 1 {
		t.Fatal("invalid occurrence")
	}
	parent := os.Getenv("TARP_CRASH_DIR")
	var armed atomic.Bool
	var count atomic.Int64
	fs := &storage.IO{Hook: func(point string) error {
		if armed.Load() && point == os.Getenv("TARP_CRASH_POINT") {
			if count.Add(1) == int64(n) {
				os.Exit(91)
			}
		}
		return nil
	}}
	if os.Getenv("TARP_CRASH_MODE") == "restore" {
		armed.Store(true)
		t.Fatal(restoreWithIO(context.Background(), filepath.Join(parent, "backup"), filepath.Join(parent, "output"), testOptions(), fs))
	}
	db, err := OpenWithIO(filepath.Join(parent, "db"), testOptions(), fs)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	switch os.Getenv("TARP_CRASH_MODE") {
	case "put", "rotate":
		err = db.Put([]byte("k"), []byte("new"))
	case "compact":
		_, err = db.Compact(context.Background())
	case "backup":
		err = db.Backup(context.Background(), filepath.Join(parent, "output"))
	}
	t.Fatalf("fault was not reached: %v", err)
}

// TestPowerCutGuest is an opt-in phase barrier for a DISPOSABLE external VM.
// It never cuts power itself; the controller and acknowledgment log stay outside.
func TestPowerCutGuest(t *testing.T) {
	if os.Getenv("TARP_POWERCUT_CONFIRM") != "disposable-vm-only" {
		t.Skip("requires explicit disposable-VM opt-in")
	}
	dir, point := os.Getenv("TARP_POWERCUT_DIR"), os.Getenv("TARP_POWERCUT_POINT")
	if dir == "" || point == "" {
		t.Fatal("explicit directory and phase required")
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a fresh, nonexistent database directory is required")
	}
	mode := os.Getenv("TARP_POWERCUT_MODE")
	if mode == "" {
		mode = "put"
	}
	switch mode {
	case "init", "put", "insert", "delete", "reinsert", "rotate", "compact", "backup", "restore", "recover", "overlap":
	default:
		t.Fatal("unknown phase-controlled power-cut mode")
	}
	output := os.Getenv("TARP_POWERCUT_OUTPUT")
	if (mode == "backup" || mode == "restore" || mode == "overlap") && output == "" {
		t.Fatal("backup/restore requires an explicit new output directory")
	}
	occurrence := 1
	if text := os.Getenv("TARP_POWERCUT_N"); text != "" {
		n, err := strconv.Atoi(text)
		if err != nil || n < 1 {
			t.Fatal("positive occurrence required")
		}
		occurrence = n
	}
	var count, reads atomic.Int64
	var armed, captured atomic.Bool
	armed.Store(mode == "init")
	captureReady, releaseBackup := make(chan struct{}), make(chan struct{})
	var db *DB
	pause := func(p string) {
		if armed.Load() && p == point && count.Add(1) == int64(occurrence) {
			if mode == "overlap" {
				fmt.Printf("TARP_READS %d\n", reads.Load())
			}
			fmt.Printf("TARP_PHASE %s\n", p)
			for {
				time.Sleep(time.Hour)
			}
		}
	}
	faults := &storage.IO{Hook: func(p string) error {
		if mode == "overlap" && p == "backup.copy:before" && captured.CompareAndSwap(false, true) {
			fmt.Printf("TARP_CAPTURE %d\n", Observe(db).Frontier().Sequence)
			close(captureReady)
			<-releaseBackup
		}
		pause(p)
		return nil
	}}
	var err error
	db, err = OpenWithIO(dir, testOptions(), faults)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "init" {
		fmt.Println("TARP_OPERATION_ACK init")
		pause("operation.ack:after")
		t.Fatal("initialization phase was not reached")
	}
	put := func(key, value string) {
		token := fmt.Sprintf("%x", value)
		if value == "" {
			token = "-"
		}
		fmt.Printf("TARP_INTENT %x %s\n", key, token)
		mustPut(t, db, key, value)
		fmt.Printf("TARP_ACK %x %s\n", key, token)
	}
	remove := func(key string) {
		fmt.Printf("TARP_INTENT_DELETE %x\n", key)
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("TARP_ACK_DELETE %x\n", key)
	}
	seed := uint64(1)
	if text := os.Getenv("TARP_POWERCUT_SEED"); text != "" {
		seed, err = strconv.ParseUint(text, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewPCG(seed, 0x100))
	for i := 0; i < 8; i++ {
		put(fmt.Sprintf("k%d", i), fmt.Sprintf("%016x", rng.Uint64()))
	}
	for i := 0; i < 8; i++ {
		put("history", fmt.Sprintf("%016x", rng.Uint64()))
	}
	put("deleted", "must-not-resurrect")
	remove("deleted")
	put("oracle", "acknowledged")
	if mode == "rotate" {
		pendingBytes := int64(format.RecordHeaderBytes + len("oracle") + len("possible"))
		for Observe(db).Frontier().ActiveEnd+pendingBytes <= db.opts.TargetSegmentBytes {
			put("p", "")
		}
	}
	if mode == "reinsert" {
		remove("oracle")
	}
	backup := filepath.Join(filepath.Dir(dir), filepath.Base(dir)+"-restore-source")
	if mode == "restore" {
		fmt.Printf("TARP_CAPTURE %d\n", Observe(db).Frontier().Sequence)
		if err := db.Backup(context.Background(), backup); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "recover" {
		db.writer.Lock()
		active := db.store.Files[db.store.Current.ActiveID].Path
		db.writer.Unlock()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		fd, err := os.OpenFile(active, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := fd.Write([]byte("uncommitted-tail"))
		if err := errors.Join(writeErr, fd.Sync(), fd.Close()); err != nil {
			t.Fatal(err)
		}
	}
	armed.Store(true)
	switch mode {
	case "put", "rotate":
		put("oracle", "possible")
	case "insert":
		put("pending", "possible")
	case "delete":
		remove("oracle")
	case "reinsert":
		put("oracle", "reinserted")
	case "compact":
		_, err = db.Compact(context.Background())
	case "backup":
		fmt.Printf("TARP_CAPTURE %d\n", Observe(db).Frontier().Sequence)
		err = db.Backup(context.Background(), output)
	case "restore":
		err = restoreWithIO(context.Background(), backup, output, testOptions(), faults)
	case "recover":
		db, err = OpenWithIO(dir, testOptions(), faults)
	case "overlap":
		done := make(chan error, 1)
		go func() { done <- db.Backup(context.Background(), output) }()
		<-captureReady
		readerReady := make(chan struct{})
		go func() {
			for {
				value, err := db.Get([]byte("oracle"))
				if err != nil || string(value) != "acknowledged" && string(value) != "possible" {
					fmt.Printf("TARP_READ_FAILURE %x %v\n", value, err)
					return
				}
				if _, err := db.Get([]byte("deleted")); !errors.Is(err, ErrNotFound) {
					fmt.Printf("TARP_READ_FAILURE deleted %v\n", err)
					return
				}
				if reads.Add(1) == 1 {
					close(readerReady)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}()
		<-readerReady
		put("oracle", "possible")
		remove("k0")
		result, compactErr := db.Compact(context.Background())
		if compactErr != nil || result.PinnedBytes == 0 {
			t.Fatalf("overlap did not preserve pinned generations: %+v %v", result, compactErr)
		}
		fmt.Printf("TARP_PINNED %d\n", result.PinnedBytes)
		close(releaseBackup)
		err = <-done
	}
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("TARP_OPERATION_ACK %s\n", mode)
	pause("operation.ack:after")
	t.Fatal("phase was not reached")
}
