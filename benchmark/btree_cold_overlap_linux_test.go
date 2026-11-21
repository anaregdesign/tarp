package benchmark

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

const qualKeys = 131072
const qualSamples = 8192

func qualKey(id int) [16]byte {
	var k [16]byte
	copy(k[:], "account/")
	binary.BigEndian.PutUint64(k[8:], uint64(id))
	return k
}

func qualValue(buf []byte, id int, version int) {
	binary.BigEndian.PutUint64(buf[:8], uint64(id))
	binary.BigEndian.PutUint64(buf[8:16], uint64(version))
}

type qualCall struct{ Start, End, ReadBytes int64 }
type qualDevice struct{ Read, Write uint64 }
type qualResult struct {
	Round                                          int
	Arm, Mode, FS, Device, DeviceID                string
	Budget                                         int64
	Get, Put                                       []qualCall
	CompactStart, CompactEnd                       int64
	CompactResult                                  tarp.CompactResult
	CopyFirst, CopyLast                            int64
	CopyRecords                                    uint64
	ReadStarted, ReadFinished                      int64
	WindowCopyBefore, WindowCopyAfter              uint64
	ExpectedGets, ValueMisses, ValueHits, Verified int
	IndexPageLoads                                 uint64
	BlockBefore, BlockAfter                        qualDevice
	MatchedBlockAfter                              qualDevice
}

func qualSectors(device string) (qualDevice, error) {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return qualDevice{}, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) >= 10 && f[0]+":"+f[1] == device {
			a, e := strconv.ParseUint(f[5], 10, 64)
			b, ee := strconv.ParseUint(f[9], 10, 64)
			return qualDevice{a * 512, b * 512}, errors.Join(e, ee)
		}
	}
	return qualDevice{}, fmt.Errorf("backing device %q unavailable in /proc/diskstats", device)
}

func qualThreadBytes(file *os.File, buf []byte) (int64, error) {
	n, err := file.ReadAt(buf, 0)
	if err != nil && n <= 0 {
		return 0, err
	}
	prefix := []byte("\nread_bytes: ")
	pos := bytes.Index(buf[:n], prefix)
	if pos < 0 {
		return 0, errors.New("missing thread read_bytes")
	}
	var v int64
	for _, c := range buf[pos+len(prefix) : n] {
		if c < '0' || c > '9' {
			break
		}
		v = v*10 + int64(c-'0')
	}
	return v, nil
}

func TestBTreeColdOverlap(t *testing.T) {
	evidence := os.Getenv("TARP_COLD_OVERLAP")
	if evidence == "" {
		t.Skip("opt-in scratch-DB qualification")
	}
	opts := engine.Options{IndexMode: engine.IndexMemory, ValueCacheBudgetBytes: 64 << 10, TargetSegmentBytes: 32 << 20}
	fixture := filepath.Join(t.TempDir(), "seed")
	parked := &engine.Hooks{CleanerStart: make(chan struct{}), ParityStart: make(chan struct{})}
	core, err := engine.OpenWithHooks(fixture, opts, nil, parked)
	if err != nil {
		t.Fatal(err)
	}
	db := (*tarp.DB)(core)
	const chunk = 1024
	keys := make([][16]byte, chunk)
	values := make([][4096]byte, chunk)
	entries := make([]tarp.Entry, chunk)
	for base := 0; base < qualKeys; base += chunk {
		for j := range chunk {
			keys[j] = qualKey(base + j)
			qualValue(values[j][:], base+j, 0)
			entries[j] = tarp.Entry{Key: keys[j][:], Value: values[j][:]}
		}
		if err := db.BatchPut(entries); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.IndexMode = engine.IndexBTree
	opts.BTreeMemoryBudgetBytes = 16 << 20
	arms := []string{"warm", "cold", "write", "compact-write"}
	for round := 1; round <= 5; round++ {
		for step := range arms {
			arm := arms[(round-1+step)%len(arms)]
			if !t.Run(fmt.Sprintf("%d-%s", round, arm), func(t *testing.T) { qualOverlapArm(t, fixture, evidence, opts, parked, round, arm) }) {
				t.FailNow()
			}
		}
	}
}

func qualOverlapArm(t *testing.T, fixture, evidence string, opts engine.Options, parked *engine.Hooks, round int, arm string) {
	dir := filepath.Join(t.TempDir(), "db")
	measurementCloneClosed(t, fixture, dir)
	clock := time.Now()
	stamp := func() int64 { return time.Since(clock).Nanoseconds() }
	var recs atomic.Uint64
	var copyFirst, copyLast, compactStart, compactEnd atomic.Int64
	firstCopy := make(chan struct{})
	hook := &storage.IO{Hook: func(point string) error {
		if point == "compact.record:after" {
			n := recs.Add(1)
			when := stamp()
			if n == 1 {
				copyFirst.Store(when)
				close(firstCopy)
			}
			copyLast.Store(when)
		}
		return nil
	}}
	core, err := engine.OpenWithHooks(dir, opts, hook, parked)
	if err != nil {
		t.Fatal(err)
	}
	db := (*tarp.DB)(core)
	obs := engine.Observe(core)
	r := qualResult{Arm: arm, Round: round, Mode: "btree", Budget: obs.Options().BTreeMemoryBudgetBytes}
	if obs.Options().IndexMode != engine.IndexBTree || r.Budget != 16<<20 || db.Stats().LiveKeys != qualKeys {
		t.Fatal("fixture/options")
	}
	fixtureIO, err := openContainerBatchIO(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fixtureIO.Close()
	r.FS, r.Device, r.DeviceID = fixtureIO.Filesystem, fixtureIO.Device, fixtureIO.deviceID
	if arm == "warm" {
		err = fixtureIO.Warm()
	} else {
		err = fixtureIO.Evict()
	}
	if err != nil {
		t.Fatal(err)
	}
	trace := rand.New(rand.NewPCG(42, uint64(round))).Perm(qualKeys / 2)
	obs.ClearCache()
	runtime.GC()
	wctx, stopWriter := context.WithCancel(context.Background())
	defer stopWriter()
	var wg sync.WaitGroup
	var writeErr, compactErr error
	var writes []qualCall
	writeStarted := make(chan struct{})
	if arm == "write" || arm == "compact-write" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var buf [4096]byte
			for seq := 1; wctx.Err() == nil; seq++ {
				id := (seq - 1) % 64
				k := qualKey(id)
				qualValue(buf[:], id, seq)
				st := stamp()
				err := db.Put(k[:], buf[:])
				end := stamp()
				if err != nil {
					writeErr = err
					if seq == 1 {
						close(writeStarted)
					}
					return
				}
				writes = append(writes, qualCall{st, end, 0})
				if seq == 1 {
					close(writeStarted)
				}
			}
		}()
		<-writeStarted
	}
	compactDone := make(chan struct{})
	var compactResult tarp.CompactResult
	if arm == "compact-write" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(compactDone)
			compactStart.Store(stamp())
			compactResult, compactErr = db.Compact(context.Background())
			compactEnd.Store(stamp())
		}()
		select {
		case <-firstCopy:
		case <-compactDone:
			t.Fatal("no actual checkpoint copy", compactErr)
		}
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	io, err := os.Open("/proc/thread-self/io")
	if err != nil {
		t.Fatal(err)
	}
	defer io.Close()
	var iobuf [1024]byte
	quota := qualSamples
	r.Get = make([]qualCall, 0, quota)
	beforeCache := db.Stats().ValueCache
	beforeIdx := obs.IndexMetrics()
	r.BlockBefore, err = qualSectors(r.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	r.WindowCopyBefore = recs.Load()
	r.ReadStarted = stamp()
	var want [4096]byte
	for i := 0; i < quota || arm == "compact-write" && compactEnd.Load() == 0; i++ {
		// Repeat the same physical positions only after an entire 65,536-key permutation.
		id := qualKeys/2 + trace[i%len(trace)]
		k := qualKey(id)
		qualValue(want[:], id, 0)
		before, e := qualThreadBytes(io, iobuf[:])
		if e != nil {
			t.Fatal(e)
		}
		st := stamp()
		got, e := db.Get(k[:])
		end := stamp()
		after, ioErr := qualThreadBytes(io, iobuf[:])
		if ioErr != nil || e != nil || !bytes.Equal(got, want[:]) {
			t.Fatalf("Get %d failed: %v %v", id, e, ioErr)
		}
		r.Get = append(r.Get, qualCall{st, end, after - before})
		if i+1 == quota {
			r.MatchedBlockAfter, err = qualSectors(r.DeviceID)
			if err != nil {
				t.Fatal(err)
			}
		}
		if i >= 2000000 {
			t.Fatal("2M-get active window cap reached")
		}
	}
	r.ReadFinished = stamp()
	r.WindowCopyAfter = recs.Load()
	r.BlockAfter, err = qualSectors(r.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	stopWriter()
	wg.Wait()
	if writeErr != nil || compactErr != nil {
		t.Fatal(writeErr, compactErr)
	}
	r.CompactResult = compactResult
	r.CompactStart = compactStart.Load()
	r.CompactEnd = compactEnd.Load()
	r.CopyFirst = copyFirst.Load()
	r.CopyLast = copyLast.Load()
	r.CopyRecords = recs.Load()
	r.Put = writes
	afterCache := db.Stats().ValueCache
	// The writer calls Put and does not increment ValueCache.Get counters.
	r.ValueMisses = int(afterCache.Misses - beforeCache.Misses)
	r.ValueHits = int(afterCache.Hits - beforeCache.Hits)
	r.ExpectedGets = len(r.Get)
	r.IndexPageLoads = obs.IndexMetrics().PageReads - beforeIdx.PageReads
	if r.ValueMisses != len(r.Get) || r.ValueHits != 0 {
		t.Fatal("value-cache counter mismatch", r.ValueMisses, len(r.Get), r.ValueHits)
	}
	var storageGets int
	for _, call := range r.Get[:quota] {
		if call.ReadBytes > 0 {
			storageGets++
		}
	}
	if arm != "warm" && storageGets < 256 {
		t.Fatal("insufficient storage reads", storageGets)
	}
	if arm == "compact-write" && (r.CompactResult.DroppedKeys != 0 || r.CompactResult.UnlinkedBytes <= 0 || r.WindowCopyAfter <= r.WindowCopyBefore || len(r.Put) == 0) {
		t.Fatal("compaction/writes not verified", r.CompactResult, r.WindowCopyBefore, r.WindowCopyAfter, len(r.Put))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	check, err := engine.OpenWithHooks(dir, opts, nil, parked)
	if err != nil {
		t.Fatal(err)
	}
	verify := (*tarp.DB)(check)
	if verify.Stats().LiveKeys != qualKeys {
		t.Fatal("wrong row count")
	}
	seen := make([]bool, qualKeys)
	for entry, err := range verify.Scan(context.Background()) {
		if err != nil || len(entry.Key) != 16 {
			t.Fatal(err)
		}
		id := int(binary.BigEndian.Uint64(entry.Key[8:]))
		if id < 0 || id >= qualKeys || seen[id] || qualKey(id) != [16]byte(entry.Key) {
			t.Fatal("bad/duplicated key", id)
		}
		seq := 0
		if id < 64 && id < len(writes) {
			seq = id + 1 + 64*((len(writes)-1-id)/64)
		}
		qualValue(want[:], id, seq)
		if !bytes.Equal(entry.Value, want[:]) {
			t.Fatalf("reopen content %d seq %d", id, seq)
		}
		seen[id] = true
		r.Verified++
	}
	if r.Verified != qualKeys {
		t.Fatal("incomplete enumeration", r.Verified)
	}
	if err := verify.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(evidence, fmt.Sprintf("%02d-%s.json.gz", round, arm)))
	if err != nil {
		t.Fatal(err)
	}
	zip := gzip.NewWriter(out)
	if err := errors.Join(json.NewEncoder(zip).Encode(r), zip.Close(), out.Close()); err != nil {
		t.Fatal(err)
	}
	d := make([]int64, quota)
	for i, x := range r.Get[:quota] {
		d[i] = x.End - x.Start
	}
	slices.Sort(d)
	t.Logf("matched Get p99 %.1f us, %d/%d task storage reads; full %d Gets/%d Puts/%d copied, %d rows verified", float64(d[(quota*99+99)/100-1])/1000, storageGets, quota, len(r.Get), len(writes), r.CopyRecords, r.Verified)
}
