package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

// TestMaintenanceForeground is an opt-in equal-count diagnostic for issues 30
// and 35. Each arm clones the same closed fixture. The ordinary test suite
// skips fixture preparation and all measurements.
func TestMaintenanceForeground(t *testing.T) {
	if os.Getenv("TARP_MAINTENANCE_BENCH") != "1" {
		t.Skip("set TARP_MAINTENANCE_BENCH=1 to measure foreground Put")
	}
	keys, value, opts := measurementInput(4096, 4096)
	opts.ValueCacheBudgetBytes, opts.TargetSegmentBytes, opts.ReadFileHandles = 64<<10, 1<<20, 4
	fixture := os.Getenv("TARP_MAINTENANCE_FIXTURE")
	if fixture == "" {
		dir := filepath.Join(t.TempDir(), "working")
		db, err := engine.OpenWithHooks(dir, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range keys {
			if err := db.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		fixture = filepath.Join(t.TempDir(), "fixture")
		measurementPrepareFixture(t, dir, fixture, opts, keys, value)
	}
	for _, mode := range []string{"idle", "compact", "compact-paced", "backup"} {
		t.Run(mode, func(t *testing.T) { measureForeground(t, fixture, opts, keys, value, mode) })
	}
}

type foregroundPut struct {
	StartNS   int64
	ElapsedNS int64
	WaitNS    int64
	SyncNS    int64
	Syncs     int
	Overlaps  bool
}

type maintenanceWindow struct {
	Mode            string
	StartNS         int64
	ElapsedNS       int64
	DATABytes       int
	CopyWrites      int
	BackupReads     int
	SourceOpens     int
	Records         int
	PaceSleeps      int
	CaptureSequence uint64
	StateSHA256     string
	Puts            []foregroundPut
}

func measureForeground(t *testing.T, fixture string, opts engine.Options, keys [][]byte, value []byte, mode string) {
	t.Helper()
	root := t.TempDir()
	dir, target := filepath.Join(root, "db"), filepath.Join(root, "backup")
	measurementCloneClosed(t, fixture, dir)
	fs := &storage.IO{}
	engineHooks := &engine.Hooks{CleanerStart: make(chan struct{})}
	db, err := engine.OpenWithHooks(dir, opts, fs, engineHooks)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writerKey := []byte("writer-key-00000")
	if err := db.Put(writerKey, value); err != nil {
		t.Fatal(err)
	}
	digest, count := measurementState(t, db)
	initial := engine.Observe(db).Frontier()
	if initial.LastUserSequence != 5633 || count != 4097 {
		t.Fatalf("unexpected foreground fixture: sequence=%d keys=%d", initial.LastUserSequence, count)
	}
	hot := keys[:8]
	for _, k := range hot {
		if _, err := db.Get(k); err != nil {
			t.Fatal(err)
		}
	}

	beginPuts := make(chan struct{})
	startPuts := sync.OnceFunc(func() { close(beginPuts) })
	var pending atomic.Bool
	var ownGate bool // Shared catalog/CURRENT events run under this same writer gate.
	var waitStart, syncStart time.Time
	var put *foregroundPut
	window := maintenanceWindow{Mode: mode, StateSHA256: digest, Puts: make([]foregroundPut, 128)}
	engineHooks.WriterWait = func(acquired bool) {
		if !acquired {
			waitStart = time.Now()
			pending.Store(true)
			return
		}
		put.WaitNS = time.Since(waitStart).Nanoseconds()
		ownGate = true
	}
	engineHooks.PutCopied = func(_, _ int) {
		// Append and all of its syncs have completed, and Put still owns the gate.
		ownGate = false
		pending.Store(false)
	}
	toPace := 0
	pace := func() {
		if toPace >= 128<<10 {
			toPace = 0
			if pending.Load() {
				window.PaceSleeps++
				time.Sleep(2 * time.Millisecond)
			}
		}
	}
	fs.Wrote = func(point string, n int) {
		switch point {
		case "backup.copy", "compact.segment.write", "compact.write":
			window.DATABytes += n
			window.CopyWrites++
			if point == "compact.write" {
				toPace += n
			}
		}
	}
	fs.Hook = func(point string) error {
		switch point {
		case "backup.metadata.write:before", "segment.write:before":
			// Backup has captured the frontier, or Compact holds the writer gate
			// for sealing and capture. Neither can capture a timed Put.
			startPuts()
		case "compact.source.open:after":
			window.SourceOpens++
		case "compact.record:after":
			window.Records++
			if mode == "compact-paced" {
				pace()
			}
		case "backup.paced:after":
			window.PaceSleeps++
		case "backup.read:after":
			window.BackupReads++
		case "data.sync:before", "segment.sync:before", "catalog.sync:before", "catalog.dirsync:before", "current.sync:before", "current.dirsync:before":
			if ownGate {
				syncStart = time.Now()
			}
		case "data.sync:after", "segment.sync:after", "catalog.sync:after", "catalog.dirsync:after", "current.sync:after", "current.dirsync:after":
			if ownGate {
				put.SyncNS += time.Since(syncStart).Nanoseconds()
				put.Syncs++
			}
		}
		return nil
	}
	stop := make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				readerDone <- nil
				return
			default:
			}
			if _, err := db.Get(hot[i%len(hot)]); err != nil {
				readerDone <- err
				return
			}
		}
	}()
	epoch := time.Now()
	done := make(chan error, 1)
	go func() {
		var e error
		if mode == "idle" {
			startPuts()
		} else {
			start := time.Now()
			window.StartNS = start.Sub(epoch).Nanoseconds()
			if mode == "backup" {
				e = db.Backup(context.Background(), target)
			} else {
				_, e = db.Compact(context.Background())
			}
			window.ElapsedNS = time.Since(start).Nanoseconds()
			startPuts() // release the writer on an early error
		}
		done <- e
	}()
	<-beginPuts
	for i := range window.Puts {
		put = &window.Puts[i]
		start := time.Now()
		put.StartNS = start.Sub(epoch).Nanoseconds()
		err = db.Put(writerKey, value)
		put.ElapsedNS = time.Since(start).Nanoseconds()
		if err != nil {
			break
		}
	}
	maintenanceErr := <-done
	close(stop)
	if err := errors.Join(err, maintenanceErr, <-readerDone); err != nil {
		t.Fatal(err)
	}
	for i := range window.Puts {
		p := &window.Puts[i]
		p.Overlaps = mode != "idle" && p.StartNS < window.StartNS+window.ElapsedNS && p.StartNS+p.ElapsedNS > window.StartNS
		if p.Syncs < 3 || p.WaitNS+p.SyncNS > p.ElapsedNS {
			t.Fatalf("invalid Put timing %+v", p)
		}
	}
	if end := engine.Observe(db).Frontier().LastUserSequence; end != initial.LastUserSequence+uint64(len(window.Puts)) {
		t.Fatalf("unexpected tail %d", end)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	checkDir, seq := dir, initial.LastUserSequence+uint64(len(window.Puts))
	if mode == "backup" {
		checkDir, seq = filepath.Join(root, "restored"), initial.LastUserSequence
		if err := engine.Restore(context.Background(), engine.RestoreRequest{
			Source:      target,
			Destination: checkDir,
			Limits:      opts.RestoreLimits(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	verified, err := engine.OpenWithHooks(checkDir, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	actual, live := measurementState(t, verified)
	frontier, observed := engine.Observe(verified).Frontier(), observedLayout(verified)
	if actual != digest || live != count || frontier.LastUserSequence != seq || frontier.UUID != initial.UUID {
		t.Fatal("foreground digest or sequence mismatch")
	}
	if mode == "backup" {
		window.CaptureSequence = frontier.LastUserSequence
	}
	if strings.HasPrefix(mode, "compact") {
		window.CaptureSequence = observed.CheckpointCutoff
		if observed.CheckpointCutoff != initial.LastUserSequence || window.Records != count {
			t.Fatal("incorrect checkpoint prefix")
		}
	}
	encoded, err := json.Marshal(window)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("MAINTENANCE_FOREGROUND %s\n", encoded)
}
