package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/storage"
)

// VMReplayOpenExisting uses the root package in an external-test adapter.
var VMReplayOpenExisting func(context.Context, string) (*DB, error)

const migrationKeys = 32768
const migrationSequence = migrationKeys + migrationKeys/4 + migrationKeys/8

func migrationKey(id int) []byte {
	key := make([]byte, 96)
	binary.BigEndian.PutUint64(key, uint64(id))
	for i := 8; i < len(key); i++ {
		key[i] = byte(31*id + 17*i)
	}
	return key
}

func migrationValue(id int, revision byte) []byte {
	return []byte(fmt.Sprintf("migration-%08d-%d", id, revision))
}

func migrationSeed(t *testing.T, dir string) {
	t.Helper()
	db, err := OpenWithHooks(dir, Options{IndexMode: IndexMemory, TargetSegmentBytes: 32 << 20}, nil, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	var batch []Entry
	var deletes [][]byte
	for i := range migrationKeys {
		id := (4051 * i) % migrationKeys
		batch = append(batch, Entry{Key: migrationKey(id), Value: migrationValue(id, 0)})
		if len(batch) == 512 {
			if err := db.BatchPut(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	for id := 0; id < migrationKeys; id += 4 {
		batch = append(batch, Entry{Key: migrationKey(id), Value: migrationValue(id, 1)})
		if len(batch) == 512 {
			if err := db.BatchPut(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	for id := 0; id < migrationKeys; id += 8 {
		deletes = append(deletes, migrationKey(id))
		if len(deletes) == 512 {
			if err := db.BatchDelete(deletes); err != nil {
				t.Fatal(err)
			}
			deletes = deletes[:0]
		}
	}
	if got := Observe(db).Frontier().Sequence; got != migrationSequence {
		t.Fatalf("seed sequence %d", got)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	indexes, err := filepath.Glob(filepath.Join(dir, "INDEX-*"))
	if err != nil || len(indexes) != 0 {
		t.Fatalf("seed INDEX %v %v", indexes, err)
	}
}

func migrationHash(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][32]byte{}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		fd, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		_, err = io.Copy(h, fd)
		if err := errors.Join(err, fd.Close()); err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = [32]byte(h.Sum(nil))
	}
	return out
}

func migrationCheck(t *testing.T, db *DB, fresh bool) {
	t.Helper()
	seq := uint64(migrationSequence)
	want := migrationKeys - migrationKeys/8
	if fresh {
		seq++
		want++
	}
	probe := Observe(db)
	if probe.Options().IndexMode != IndexBTree || probe.Options().BTreeMemoryBudgetBytes != 16<<20 || probe.Options().OpenValidation != Full || probe.Frontier().Sequence != seq || probe.LiveKeys() != want {
		t.Fatalf("unexpected recovered configuration or state: %+v seq=%d count=%d", probe.Options(), probe.Frontier().Sequence, probe.LiveKeys())
	}
	seen := make([]bool, migrationKeys)
	n := 0
	newSeen := false
	for e, err := range db.Scan(t.Context()) {
		if err != nil {
			t.Fatal(err)
		}
		n++
		if bytes.Equal(e.Key, []byte("post-recovery-key")) {
			if !fresh || newSeen || !bytes.Equal(e.Value, []byte("post-recovery-value")) {
				t.Fatal("unexpected fresh record")
			}
			newSeen = true
			continue
		}
		if len(e.Key) != 96 {
			t.Fatal("unexpected key")
		}
		id := binary.BigEndian.Uint64(e.Key)
		if id >= migrationKeys || id%8 == 0 || seen[id] || !bytes.Equal(e.Key, migrationKey(int(id))) {
			t.Fatalf("unexpected, deleted, or duplicate key %d", id)
		}
		var rev byte
		if id%4 == 0 {
			rev = 1
		}
		if !bytes.Equal(e.Value, migrationValue(int(id), rev)) {
			t.Fatalf("value mismatch %d", id)
		}
		seen[id] = true
	}
	if n != want || newSeen != fresh {
		t.Fatalf("scan count=%d fresh=%v", n, newSeen)
	}
	for id, present := range seen {
		if present != (id%8 != 0) {
			t.Fatalf("missing or resurrected record %d", id)
		}
	}
}

type migrationSpace struct {
	Samples           int
	MaxLogicalBytes   int64
	MaxAllocatedBytes int64
	MaxReplayBytes    int64
	MinAvailableBytes uint64
}

// Sample the unlinked work files by their open descriptors. The maximum of
// these samples is a lower bound on the instantaneous space used by replay.
func migrationMeter(dir string) func() migrationSpace {
	stop := make(chan struct{})
	ready := make(chan struct{})
	result := make(chan migrationSpace, 1)
	go func() {
		st := migrationSpace{MinAvailableBytes: ^uint64(0)}
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			var fs syscall.Statfs_t
			if syscall.Statfs(dir, &fs) == nil {
				st.MinAvailableBytes = min(st.MinAvailableBytes, fs.Bavail*uint64(fs.Bsize))
			}
			var logical, allocated, replay int64
			fds, _ := os.ReadDir("/proc/self/fd")
			for _, fd := range fds {
				path := filepath.Join("/proc/self/fd", fd.Name())
				target, err := os.Readlink(path)
				if err != nil || !strings.HasPrefix(target, dir+"/.tarp-work/") {
					continue
				}
				var stat syscall.Stat_t
				if syscall.Stat(path, &stat) != nil {
					continue
				}
				logical += stat.Size
				allocated += stat.Blocks * 512
				if strings.HasPrefix(filepath.Base(target), "tarp-replay-") {
					replay += stat.Size
				}
			}
			st.MaxLogicalBytes = max(st.MaxLogicalBytes, logical)
			st.MaxAllocatedBytes = max(st.MaxAllocatedBytes, allocated)
			st.MaxReplayBytes = max(st.MaxReplayBytes, replay)
			st.Samples++
			if st.Samples == 1 {
				close(ready)
			}
			select {
			case <-stop:
				result <- st
				return
			case <-ticker.C:
			}
		}
	}()
	<-ready
	return func() migrationSpace { close(stop); return <-result }
}

func migrationLog(t *testing.T, stage string, values any) {
	t.Helper()
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("migration145 %s %s\n", stage, raw)
}

func vmLowSpaceReplay(t *testing.T, firstBoot bool) {
	t.Helper()
	guesttest.RequireDisposableGuest(t)
	dir := "/mnt/tarp-data/replay-db"
	if !firstBoot {
		db, err := VMReplayOpenExisting(t.Context(), dir)
		if err != nil {
			t.Fatal(err)
		}
		migrationCheck(t, db, true)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		migrationLog(t, "cold-boot", map[string]any{"sequence": migrationSequence + 1, "live": migrationKeys - migrationKeys/8 + 1, "deleted": migrationKeys / 8})
		return
	}
	migrationSeed(t, dir)
	sealed := migrationHash(t, dir)
	fillerPath := "/mnt/tarp-data/replay-filler.bin"
	f, err := os.OpenFile(fillerPath, os.O_EXCL|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	fillerBytes, _ := vmFill(t, f)
	full := guesttest.AvailableBytes(t)
	if full != 0 {
		t.Fatalf("expected full disk, available=%d", full)
	}
	headroom := vmReleaseFiller(t, f, 256<<10)
	if headroom >= 1<<20 {
		t.Fatalf("too much failure headroom %d", headroom)
	}
	stopMeter := migrationMeter(dir)
	db, openErr := VMReplayOpenExisting(t.Context(), dir)
	pressure := stopMeter()
	var ioerr *os.PathError
	var work *storage.IndexWorkError
	var op *OpError
	if db != nil || !errors.Is(openErr, syscall.ENOSPC) || !errors.Is(openErr, ErrNoSpace) || !errors.As(openErr, &ioerr) || ioerr.Op != "write" || !strings.HasPrefix(ioerr.Path, dir+"/.tarp-work/tarp-replay-") || !errors.As(openErr, &work) || !errors.As(openErr, &op) || op.Outcome != NotApplied {
		t.Fatalf("unexpected failure: handle=%p, err=%v, meter=%+v, headroom=%d", db, openErr, pressure, headroom)
	}
	same := maps.Equal(sealed, migrationHash(t, dir))
	if !same {
		t.Fatal("durable DB names or bytes changed during failed open")
	}
	lock, err := platform.Lock(dir)
	if err != nil {
		t.Fatalf("directory lock survived failed open: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	migrationLog(t, "failure", map[string]any{"filler_bytes": fillerBytes, "available_at_full": full, "headroom": headroom, "error": openErr.Error(), "errno": int(syscall.ENOSPC), "io_op": ioerr.Op, "io_path": ioerr.Path, "index_work_error": true, "outcome": op.Outcome, "lock_reacquired": true, "durable_files_unchanged": same, "hashed_files": len(sealed), "runtime": pressure})
	if err := errors.Join(f.Close(), os.Remove(fillerPath)); err != nil {
		t.Fatal(err)
	}
	available := guesttest.AvailableBytes(t)
	stopMeter = migrationMeter(dir)
	db, err = VMReplayOpenExisting(t.Context(), dir)
	retry := stopMeter()
	if err != nil {
		t.Fatal(err)
	}
	if retry.MaxReplayBytes == 0 || retry.MaxAllocatedBytes == 0 {
		t.Fatalf("failed to see temporary replay files: %+v", retry)
	}
	migrationCheck(t, db, false)
	migrationLog(t, "recovered", map[string]any{"headroom": available, "runtime": retry, "sequence": migrationSequence, "live": migrationKeys - migrationKeys/8, "seed_keys": migrationKeys, "overwritten": migrationKeys / 4, "deleted": migrationKeys / 8, "key_bytes": 96, "mode": "default-BTree", "budget": Observe(db).Options().BTreeMemoryBudgetBytes})
	if err := db.Put([]byte("post-recovery-key"), []byte("post-recovery-value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = VMReplayOpenExisting(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	migrationCheck(t, db, true)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	migrationLog(t, "reopen", map[string]any{"sequence": migrationSequence + 1, "live": migrationKeys - migrationKeys/8 + 1})
}
