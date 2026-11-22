package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/benchmark/qualification"
	"github.com/anaregdesign/tarp/internal/storage"
)

func vmCaseOptions(mode string) Options {
	if mode == "exhaustion" {
		return Options{ValueCacheBudgetBytes: 64 << 10, MaxRecordBytes: 8 << 10, TargetSegmentBytes: 1 << 20, ReadFileHandles: 4}
	}
	return testOptions()
}

func vmFill(t *testing.T, fd *os.File) (int64, string) {
	t.Helper()
	if _, err := fd.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1<<20)
	for attempts := 0; attempts < 257; attempts++ {
		info, err := fd.Stat()
		if err != nil || info.Size() > qualification.MaxDisk {
			t.Fatalf("bounded filler stat: %v", err)
		}
		n, writeErr := fd.Write(buffer)
		point, cause := "write", writeErr
		if writeErr == nil {
			point, cause = "sync", fd.Sync()
		}
		if cause == nil {
			if n != len(buffer) {
				t.Fatal("short filler write without an error")
			}
			continue
		}
		if !errors.Is(cause, syscall.ENOSPC) {
			t.Fatalf("real filesystem exhaustion must be ENOSPC, not %v", cause)
		}
		info, err = fd.Stat()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("real filler ENOSPC at %s: %v; length=%d available=%d\n", point, cause, info.Size(), guesttest.AvailableBytes(t))
		return info.Size(), point
	}
	t.Fatal("filler exceeded the disposable device bound without real ENOSPC")
	return 0, ""
}

func vmReleaseFiller(t *testing.T, fd *os.File, additional uint64) uint64 {
	t.Helper()
	target := guesttest.AvailableBytes(t) + additional
	// Small decrements first remove any unallocated delayed-allocation suffix,
	// then release a measured amount of actual filesystem headroom.
	for attempts := 0; attempts < 128 && guesttest.AvailableBytes(t) < target; attempts++ {
		info, err := fd.Stat()
		if err != nil || info.Size() < 64<<10 {
			t.Fatalf("bounded filler release: %v", err)
		}
		if err := errors.Join(fd.Truncate(info.Size()-64<<10), fd.Sync()); err != nil {
			t.Fatal(err)
		}
	}
	available := guesttest.AvailableBytes(t)
	if available < target {
		t.Fatal("bounded filler release did not establish the requested headroom")
	}
	return available
}

func vmExhaustion(t *testing.T, dir, output string, seed uint64) {
	t.Helper()
	guesttest.RequireDisposableGuest(t)
	opts := vmCaseOptions("exhaustion")
	ready, release := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	// This fault campaign attributes ENOSPC, retry and reclaimed bytes to three
	// explicit checkpoints. Isolate those calls from automatic reclamation.
	ioHooks := &storage.IO{Hook: func(point string) error {
		if point == "backup.copy:before" && held.CompareAndSwap(false, true) {
			close(ready)
			<-release
		}
		return nil
	}}
	db, err := OpenWithHooks(dir, opts, ioHooks, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	keys := make([][]byte, 128)
	old, current := make([][]byte, len(keys)), make([][]byte, len(keys))
	put := func(key, value []byte) {
		fmt.Printf("TARP_INTENT %x %x\n", key, value)
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("TARP_ACK %x %x\n", key, value)
	}
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%016x", i))
		old[i] = bytes.Repeat([]byte{byte(seed + uint64(i))}, 4096)
		current[i] = bytes.Repeat([]byte{byte(seed + uint64(i) + 1)}, 4096)
		put(keys[i], old[i])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	backupDone := make(chan error, 1)
	go func() { backupDone <- db.Backup(ctx, output) }()
	select {
	case <-ready:
	case err := <-backupDone:
		t.Fatalf("backup completed before the pinned-generation barrier: %v", err)
	}
	fmt.Printf("TARP_CAPTURE %d\n", Observe(db).Frontier().LastUserSequence)
	for i := range keys {
		put(keys[i], current[i])
	}
	fillerPath := "/mnt/tarp-data/filler.bin"
	filler, err := os.OpenFile(fillerPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var verdict qualification.ExhaustionVerdict
	verdict.DeviceBytes = qualification.MaxDisk
	verdict.FirstFillerBytes, verdict.FirstFailurePoint = vmFill(t, filler)
	info, err := filler.Stat()
	if err != nil {
		t.Fatal(err)
	}
	verdict.FirstFillerAllocated = info.Sys().(*syscall.Stat_t).Blocks * 512
	verdict.FirstErrno, verdict.FullAvailableBytes = int(syscall.ENOSPC), guesttest.AvailableBytes(t)
	verdict.CheckpointAvailable = vmReleaseFiller(t, filler, 128<<10)
	if verdict.CheckpointAvailable >= 256<<10 {
		t.Fatalf("insufficiently constrained checkpoint headroom: %d", verdict.CheckpointAvailable)
	}
	_, checkpointErr := db.Compact(ctx)
	var operation *OpError
	if !errors.Is(checkpointErr, syscall.ENOSPC) || !errors.Is(checkpointErr, ErrNoSpace) ||
		!errors.As(checkpointErr, &operation) || operation.Outcome != NotApplied || db.health() != nil {
		t.Fatalf("pre-publication checkpoint exhaustion changed source health/outcome: %v / %v", checkpointErr, db.health())
	}
	verdict.CheckpointError, verdict.CheckpointNotApplied = checkpointErr.Error(), true
	for i := range keys {
		value, err := db.Get(keys[i])
		if err != nil || !bytes.Equal(value, current[i]) {
			t.Fatalf("checkpoint exhaustion damaged acknowledged key %x: %v", keys[i], err)
		}
	}
	vmReleaseFiller(t, filler, 2<<20)
	compacted, err := db.Compact(ctx)
	if err != nil || compacted.PinnedRetiredDataBytes == 0 {
		t.Fatalf("retry must retain the slow backup's old generation: %+v %v", compacted, err)
	}
	verdict.CheckpointRetryPassed, verdict.PinnedBytes = true, compacted.PinnedRetiredDataBytes
	close(release)
	if err := <-backupDone; err != nil {
		t.Fatalf("slow backup after checkpoint retry: %v", err)
	}
	cleaned, err := db.Compact(ctx)
	if err != nil || cleaned.PinnedRetiredDataBytes != 0 || cleaned.UnlinkedBytes == 0 {
		t.Fatalf("released backup pins must permit safe reclamation: %+v %v", cleaned, err)
	}
	verdict.ReleasedPinnedBytes, verdict.ReclaimedAfterRelease = cleaned.PinnedRetiredDataBytes, cleaned.UnlinkedBytes
	verdict.FinalFillerBytes, _ = vmFill(t, filler)
	if _, err := db.Get(keys[0]); err != nil {
		t.Fatal(err)
	}
	rejected := bytes.Repeat([]byte{0x9d}, 4096)
	fmt.Printf("TARP_INTENT %x %x\n", keys[0], rejected)
	mutationErr := db.Put(keys[0], rejected)
	if !errors.Is(mutationErr, syscall.ENOSPC) || !errors.Is(mutationErr, ErrNoSpace) ||
		!errors.As(mutationErr, &operation) {
		t.Fatalf("admitted write on the actually exhausted filesystem: %v", mutationErr)
	}
	switch operation.Outcome {
	case NotApplied:
		fmt.Printf("TARP_NOT_APPLIED %x\n", keys[0])
	case Committed:
		fmt.Printf("TARP_ERROR_COMMITTED %x\n", keys[0])
	case Unknown:
	default:
		t.Fatalf("invalid mutation outcome %s", operation.Outcome)
	}
	verdict.MutationError, verdict.MutationOutcome = mutationErr.Error(), string(operation.Outcome)
	verdict.MutationErrno = int(syscall.ENOSPC)
	for _, key := range [][]byte{keys[0], keys[64], []byte("absent")} {
		if _, err := db.Get(key); !errors.Is(err, ErrUnhealthy) || !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("real write failure made health cache-dependent for %x: %v", key, err)
		}
	}
	verdict.HotAndMissFailStop = true
	closeErr := db.Close()
	if !errors.Is(closeErr, syscall.ENOSPC) || db.Close() != closeErr {
		t.Fatalf("close lost the first real write failure: %v", closeErr)
	}
	verdict.RepeatedCloseSameError = true
	if err := filler.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(fillerPath); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(filepath.Dir(fillerPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("ownership/recovery retry after freeing only the filler: %v", err)
	}
	for i := range keys {
		value, err := db.Get(keys[i])
		allowed := bytes.Equal(value, current[i])
		if i == 0 && operation.Outcome != NotApplied {
			allowed = bytes.Equal(value, rejected) || operation.Outcome == Unknown && allowed
		}
		if err != nil || !allowed {
			t.Fatalf("reopened acknowledged/outcome state for %x: %v", keys[i], err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	verdict.ReopenedAndVerified = true
	if err := verdict.Validate(); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(verdict)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("TARP_EXHAUSTION_PASS %s\nTARP_PHASE exhaustion.complete\n", data)
	for {
		time.Sleep(time.Hour)
	}
}
