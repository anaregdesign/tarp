package benchmark

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/benchmark/scaleprobe"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
)

type scaleBackup struct {
	dir       string
	keys      int
	info      tarp.BackupInfo
	dataBytes uint64
	digest    string
}

func prepareScaleBackup(t *testing.T, n int) scaleBackup {
	t.Helper()
	root := t.TempDir()
	opts := tarp.Options{TargetSegmentBytes: 32 << 20, MaxRecordBytes: 4096, ValueCacheBudgetBytes: 1 << 20}
	db, err := tarp.Open(filepath.Join(root, "db"), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	for base := 0; base < n; base += 512 {
		entries := make([]tarp.Entry, min(512, n-base))
		for i := range entries {
			key, value := make([]byte, 16), make([]byte, 64)
			copy(key, "verify00")
			binary.BigEndian.PutUint64(key[8:], uint64(base+i))
			binary.BigEndian.PutUint64(value, uint64(base+i))
			entries[i] = tarp.Entry{Key: key, Value: value}
		}
		if err := db.BatchPut(entries); err != nil {
			t.Fatal(err)
		}
	}
	fixture := scaleBackup{dir: filepath.Join(root, "backup"), keys: n}
	var actual int
	fixture.digest, actual = measurementState(t, (*engine.DB)(db))
	if actual != n {
		t.Fatalf("seed has %d keys, want %d", actual, n)
	}
	if err := db.Backup(t.Context(), fixture.dir); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(fixture.dir, "BACKUP"))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := format.DecodeBackup(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range meta.Files {
		if strings.HasPrefix(f.Name, "DATA-") {
			fixture.dataBytes += uint64(f.Bytes)
		}
	}
	if fixture.dataBytes == 0 || meta.Sequence != uint64(n) {
		t.Fatalf("invalid fixture: DATA bytes %d, sequence %d, keys %d", fixture.dataBytes, meta.Sequence, n)
	}
	// Establish the expected complete report and warm the backup before rotating
	// the order of measured calls. The same immutable backup serves every arm.
	fixture.info, err = tarp.VerifyBackup(t.Context(), fixture.dir)
	if err != nil || fixture.info.DatabaseID != meta.UUID || fixture.info.Sequence != uint64(n) || fixture.info.LiveKeys != uint64(n) {
		t.Fatalf("fixture report %+v: %v", fixture.info, err)
	}
	return fixture
}

// Only the opt-in Linux campaign uses /proc. The verifier unlinks its scratch
// files immediately. Directory listings therefore cannot measure their sizes.
func verifyScratchBytes(parent string) (uint64, error) {
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	var total uint64
	for _, fd := range fds {
		path := filepath.Join("/proc/self/fd", fd.Name())
		target, err := os.Readlink(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		name := filepath.Base(target)
		if filepath.Dir(target) != parent || !(strings.HasPrefix(name, "tarp-index-") || strings.HasPrefix(name, "tarp-replay-")) {
			continue
		}
		st, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, err
		}
		// A descriptor might close and get reused between Readlink and Stat.
		again, err := os.Readlink(path)
		if err == nil && again == target {
			total += uint64(st.Size())
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
	}
	return total, nil
}

type verifyResources struct {
	baselineHeap, peakHeap, peakScratch uint64
	samples                             int
	err                                 error
}

func sampleVerifyResources(parent string, call func() error) (verifyResources, error) {
	var r verifyResources
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	collect := func() {
		metrics.Read(sample)
		heap := sample[0].Value.Uint64()
		if r.samples == 0 {
			r.baselineHeap = heap
		}
		r.peakHeap = max(r.peakHeap, heap)
		scratch, err := verifyScratchBytes(parent)
		if err != nil && r.err == nil {
			r.err = err
		}
		r.peakScratch = max(r.peakScratch, scratch)
		r.samples++
	}
	collect()
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				collect()
			case <-stop:
				return
			}
		}
	}()
	err := call()
	close(stop)
	<-done
	collect()
	return r, errors.Join(err, r.err)
}

func (f scaleBackup) verifyArm(t *testing.T, round int, arm string) {
	t.Helper()
	parent := t.TempDir()
	ctx := t.Context()
	measure := func(dest string) error {
		var info tarp.BackupInfo
		var err error
		switch arm {
		case "read_only":
			info, err = tarp.VerifyBackup(ctx, f.dir)
		case "with_scratch":
			info, err = tarp.VerifyBackupWithScratch(ctx, f.dir, parent)
		case "restore":
			return tarp.Restore(ctx, tarp.RestoreRequest{Source: f.dir, Destination: dest})
		default:
			return fmt.Errorf("unknown verification arm %q", arm)
		}
		if err == nil && info != f.info {
			return fmt.Errorf("backup report %+v differs from expected %+v", info, f.info)
		}
		return err
	}
	dest := filepath.Join(parent, "timed")
	runtime.GC()
	beforePasses, beforeBytes := scaleprobe.ReplayPasses.Load(), scaleprobe.ReplayBytes.Load()
	start := time.Now()
	err := measure(dest)
	elapsed := time.Since(start)
	passes, readBytes := scaleprobe.ReplayPasses.Load()-beforePasses, scaleprobe.ReplayBytes.Load()-beforeBytes
	if err != nil {
		t.Fatal(err)
	}
	// Each unique 16-byte key consumes 128+16 logical window bytes.
	const keysPerWindow = (4 << 20) / (128 + 16)
	windows := (f.keys + keysPerWindow - 1) / keysPerWindow
	wantPasses := windows
	var copyBytes uint64
	switch arm {
	case "with_scratch":
		wantPasses = 1
	case "restore":
		// The source can retry with scratch. The stage independently replays
		// all records once using the strategy established by the source.
		wantPasses = min(windows, 2) + 1
		copyBytes = f.dataBytes
	}
	if passes != uint64(wantPasses) || readBytes != passes*f.dataBytes {
		t.Fatalf("%s/%d: got %d replay passes and %d bytes, want %d passes over %d DATA bytes", arm, f.keys, passes, readBytes, wantPasses, f.dataBytes)
	}
	diagnostic := filepath.Join(parent, "sampled")
	runtime.GC()
	res, err := sampleVerifyResources(parent, func() error { return measure(diagnostic) })
	if err != nil {
		t.Fatal(err)
	}
	if arm == "restore" {
		for _, d := range []string{dest, diagnostic} {
			db, err := tarp.OpenExisting(ctx, d, tarp.Options{})
			if err != nil {
				t.Fatal(err)
			}
			digest, keys := measurementState(t, (*engine.DB)(db))
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if keys != f.keys || digest != f.digest {
				t.Fatalf("restore: %d keys digest %s, want %d keys digest %s", keys, digest, f.keys, f.digest)
			}
		}
	}
	storageScaleRow(t, round, "verify", arm, map[string]any{
		"keys": f.keys, "elapsed_ns": elapsed.Nanoseconds(), "selected_data_bytes": f.dataBytes,
		"replay_passes": passes, "replay_data_bytes": readBytes,
		// Hash and copy volumes follow the verified BACKUP manifest. Restore
		// also hashes during its copy; that read is counted once under copy.
		"manifest_hash_read_bytes": f.dataBytes, "manifest_copy_read_bytes": copyBytes,
		"baseline_heap_bytes": res.baselineHeap, "sampled_peak_heap_bytes": res.peakHeap,
		"sampled_peak_scratch_bytes": res.peakScratch, "resource_samples": res.samples,
		"fixture_state_sha256": f.digest,
	})
}

func TestStorageScaleVerify(t *testing.T) {
	round := storageScaleRound(t)
	arms := []string{"read_only", "with_scratch", "restore"}
	for _, n := range []int{20_000, 40_000, 100_000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			f := prepareScaleBackup(t, n)
			for i := range arms {
				f.verifyArm(t, round, arms[(round-1+i)%len(arms)])
			}
		})
	}
}
