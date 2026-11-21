package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	db, err := OpenWithHooks(dir, opts, ioHooks, &Hooks{CleanerStart: make(chan struct{}), ParityStart: make(chan struct{})})
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
	fmt.Printf("TARP_CAPTURE %d\n", Observe(db).Frontier().Sequence)
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

// vmPagerExhaustion is an additional live-index failure case on the same bounded
// ext4 filesystem as the standard oracle fixture. The untouched source fixture,
// capture and control verdict are still checked by the campaign host.
func vmPagerExhaustion(t *testing.T, fill bool) {
	t.Helper()
	guesttest.RequireDisposableGuest(t)
	const dir = "/mnt/tarp-data/pager-case"
	opts := Options{IndexMode: IndexBTree, BTreeMemoryBudgetBytes: 16 << 20, ValueCacheBudgetBytes: 64 << 10, MaxRecordBytes: 8 << 10, TargetSegmentBytes: 16 << 20}
	open := func() *DB {
		db, err := OpenWithHooks(dir, opts, nil, &Hooks{CleanerStart: make(chan struct{}), ParityStart: make(chan struct{})})
		if err != nil {
			t.Fatal(err)
		}
		got := Observe(db).Options()
		if got.IndexMode != IndexBTree || got.BTreeMemoryBudgetBytes != 16<<20 {
			t.Fatalf("wrong options: %+v", got)
		}
		return db
	}
	db := open()
	entries := make([]Entry, 1536)
	for i := range entries {
		key := bytes.Repeat([]byte{0x53}, 512)
		binary.BigEndian.PutUint64(key[:8], uint64(i))
		entries[i] = Entry{Key: key, Value: []byte(fmt.Sprintf("value-%04d", i))}
	}
	prior := []byte("prior-key")
	priorValue := []byte("durable-before-full")
	verify := func(expected bool) {
		target := 1
		if expected {
			target += len(entries)
		}
		if n := Observe(db).LiveKeys(); n != target {
			t.Fatalf("wrong key count %d want %d", n, target)
		}
		got, err := db.Get(prior)
		if err != nil || !bytes.Equal(got, priorValue) {
			t.Fatalf("wrong prior value %x %v", got, err)
		}
		for i, entry := range entries {
			got, err := db.Get(entry.Key)
			if expected {
				if err != nil || !bytes.Equal(got, entry.Value) {
					t.Fatalf("retried key %d: %x %v", i, got, err)
				}
			} else if !errors.Is(err, ErrNotFound) {
				t.Fatalf("rejected key %d: %x %v", i, got, err)
			}
		}
	}
	if !fill {
		verify(true)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		fmt.Println("PAGER_PROBE_POST_REBOOT {" + "\"verified\":1537,\"mode\":\"btree\",\"budget_bytes\":16777216}")
		return
	}
	if err := db.Put(prior, priorValue); err != nil {
		t.Fatal(err)
	}
	// Identify the tree's own unlinked pager before the transaction opens id logs.
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	pagerPath := ""
	for _, fd := range fds {
		link, err := os.Readlink("/proc/self/fd/" + fd.Name())
		if err != nil {
			continue
		}
		p := strings.TrimSuffix(link, " (deleted)")
		if p != link && filepath.Dir(p) == filepath.Join(dir, ".tarp-work") && strings.HasPrefix(filepath.Base(p), "tarp-index-") {
			if pagerPath != "" {
				t.Fatalf("ambiguous initial pager: %s and %s", pagerPath, p)
			}
			pagerPath = p
		}
	}
	if pagerPath == "" {
		t.Fatal("private pager path unavailable")
	}
	fmt.Printf("PAGER_PROBE_BEGIN pager=%s\n", pagerPath)
	filler, err := os.OpenFile("/mnt/tarp-data/pager-filler", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, at := vmFill(t, filler)
	full := guesttest.AvailableBytes(t)
	// Leave room for the few KiB of transaction bookkeeping that precedes the
	// first dirty pager eviction. The attempted live index needs >12 MiB of pages.
	available := vmReleaseFiller(t, filler, 128<<10)
	before := Observe(db).Frontier()
	beforeMetrics := Observe(db).IndexMetrics()
	failure := db.BatchPut(entries)
	var operation *OpError
	var pathError *os.PathError
	if !errors.As(failure, &operation) || !errors.As(failure, &pathError) || !errors.Is(failure, syscall.ENOSPC) || !errors.Is(failure, ErrNoSpace) ||
		pathError.Path != pagerPath || pathError.Op != "write" || operation.Operation != "batch-put" || operation.Outcome != NotApplied {
		t.Fatalf("expected actual NotApplied private pager write ENOSPC: %v (pager %s) before=%+v after=%+v", failure, pagerPath, beforeMetrics, Observe(db).IndexMetrics())
	}
	after := Observe(db).Frontier()
	if before.Sequence != after.Sequence || before.Generation != after.Generation {
		t.Fatalf("frontier changed: %+v -> %+v", before, after)
	}
	healthy := db.health() == nil
	if healthy {
		verify(false)
	} else if !errors.Is(db.health(), ErrUnhealthy) || !errors.Is(db.health(), syscall.ENOSPC) {
		t.Fatal(db.health())
	}
	if err := errors.Join(filler.Close(), os.Remove(filler.Name())); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open("/mnt/tarp-data")
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		t.Fatal(err)
	}
	afterMetrics := Observe(db).IndexMetrics()
	if err := db.Close(); err != nil && (healthy || !errors.Is(err, syscall.ENOSPC)) {
		t.Fatal(err)
	}
	db = open()
	verify(false)
	if err := db.BatchPut(entries); err != nil {
		t.Fatalf("retry: %v", err)
	}
	verify(true)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = open()
	verify(true)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	v := map[string]any{"mode": "btree", "budget_bytes": 16 << 20, "key_bytes": 512, "batch": len(entries), "live": len(entries) + 1, "errno": int(syscall.ENOSPC), "operation": operation.Operation, "phase": operation.Phase, "outcome": operation.Outcome, "path": pathError.Path, "io_op": pathError.Op, "full_available_bytes": full, "filler_failure_point": at, "staging_available_bytes": available, "healthy_after_failure": healthy, "sequence_before": before.Sequence, "sequence_after": after.Sequence, "pager_spills_delta": afterMetrics.SpillWrites - beforeMetrics.SpillWrites, "absent_after_reopen": len(entries), "retried_and_reopened": len(entries), "prior_preserved": true}
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("PAGER_PROBE_VERDICT %s\n", encoded)
}
