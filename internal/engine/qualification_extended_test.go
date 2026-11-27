package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/qualification"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func extendedCaseOptions(cfg qualification.PowerCase) Options {
	opts := Options{IndexMode: IndexBTree, ValueCacheBudgetBytes: 64 << 10, MaxRecordBytes: 4096,
		TargetSegmentBytes: 16 << 10, ReadFileHandles: 2}
	if cfg.IndexMode == "memory" {
		opts.IndexMode = IndexMemory
	}
	switch cfg.Mode {
	case "clean-group":
		opts.MaxRecordBytes, opts.TargetSegmentBytes = 768, 1024
	case "ordered-initial", "ordered-delta", "ordered-reuse":
		opts.MaxRecordBytes, opts.TargetSegmentBytes = 128, 256
	}
	return opts
}

func emitBatch(kind string, mutations []qualification.OracleMutation) {
	data, err := json.Marshal(mutations)
	if err != nil {
		panic(err)
	}
	fmt.Printf("TARP_BATCH_%s %s\n", kind, hex.EncodeToString(data))
}

// The same fixtures run in guarded VMs and ordinary process-cut reachability
// tests. Only the VM controller discards volatile device state.
func runExtendedPowerCase(t *testing.T, cfg qualification.PowerCase, dir, output string, barrier func(string)) {
	t.Helper()
	opts := extendedCaseOptions(cfg)
	startCleaner := make(chan struct{})
	hooks := &Hooks{CleanerStart: startCleaner, ParityStart: make(chan struct{})}
	var armed, interleaved, retiredOnce atomic.Bool
	retired := make(chan struct{})
	var db *DB
	var capture *storage.Capture
	var put func(string, string)
	fs := &storage.IO{Hook: func(point string) error {
		if !armed.Load() {
			return nil
		}
		overlap := strings.HasPrefix(cfg.Mode, "clean-") && point == "clean.scan:before" ||
			strings.HasPrefix(cfg.Mode, "ordered-") && point == "compact.write:before" ||
			cfg.Mode == "index-explicit" && point == "index.scan:before"
		if overlap && interleaved.CompareAndSwap(false, true) {
			// These scans hold no writer gate. Complete the concurrent tail
			// before allowing any selected maintenance publication to stop.
			armed.Store(false)
			put("tail", "concurrent-acknowledged-tail")
			if _, err := db.Get([]byte("keep")); err != nil {
				t.Fatal(err)
			}
			fmt.Println("TARP_READS 1")
			armed.Store(true)
		}
		if point == "clean.remap:after" && capture != nil {
			var pinned int64
			for _, source := range capture.Segments {
				if _, err := os.Stat(source.Segment.Path); err != nil {
					t.Fatalf("captured DATA retired before pin release: %v", err)
				}
				pinned += source.Entry.Bytes
			}
			fmt.Printf("TARP_PINNED %d\n", pinned)
			capture.Release()
			capture = nil
		}
		barrier(point)
		if point == "clean.retire.dirsync:after" && strings.HasPrefix(cfg.Mode, "clean-") && retiredOnce.CompareAndSwap(false, true) {
			close(retired)
		}
		return nil
	}}
	var err error
	db, err = OpenWithHooks(dir, opts, fs, hooks)
	if err != nil {
		t.Fatal(err)
	}
	put = func(key, value string) {
		token := hex.EncodeToString([]byte(value))
		if token == "" {
			token = "-"
		}
		fmt.Printf("TARP_INTENT %x %s\n", key, token)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("TARP_ACK %x %s\n", key, token)
	}
	remove := func(key string) {
		fmt.Printf("TARP_INTENT_DELETE %x\n", key)
		if err := db.Delete([]byte(key)); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("TARP_ACK_DELETE %x\n", key)
	}
	switch cfg.Mode {
	case "batch-put", "batch-delete", "batch-rotate":
		put("keep", "preserved")
		put("hot", "old")
		put("gone", "old")
		remove("gone")
		put("gone", "reinserted")
		mutations := []qualification.OracleMutation{
			{Key: []byte("hot"), Value: []byte("intermediate")},
			{Key: []byte("new"), Value: []byte("inserted")},
			{Key: []byte("hot"), Value: []byte("final")},
		}
		if cfg.Mode == "batch-delete" {
			mutations = []qualification.OracleMutation{
				{Key: []byte("hot"), Delete: true}, {Key: []byte("gone"), Delete: true},
				{Key: []byte("hot"), Delete: true},
			}
		}
		if cfg.Mode == "batch-rotate" {
			for db.store.Current.ActiveCommittedEndOffset < opts.TargetSegmentBytes-2048 {
				put("padding", strings.Repeat("x", 3000))
			}
			for i := range mutations {
				mutations[i].Value = bytes.Repeat([]byte{byte('a' + i)}, 3000)
			}
		}
		emitBatch("INTENT", mutations)
		armed.Store(true)
		if cfg.Mode == "batch-delete" {
			var keys [][]byte
			for _, m := range mutations {
				keys = append(keys, m.Key)
			}
			err = db.BatchDelete(keys)
		} else {
			var entries []Entry
			for _, m := range mutations {
				entries = append(entries, Entry{Key: m.Key, Value: m.Value})
			}
			err = db.BatchPut(entries)
		}
		if err != nil {
			t.Fatal(err)
		}
		emitBatch("ACK", mutations)
	case "clean-single", "clean-group", "clean-repack":
		switch cfg.Mode {
		case "clean-single":
			put("hot", strings.Repeat("x", 3000))
			put("keep", "preserved")
			put("hot", strings.Repeat("y", 3000))
			put("hot", strings.Repeat("z", 3000))
			sealForCleaning(t, db)
			put("hot", "tail")
		case "clean-group":
			put("gone", strings.Repeat("x", 600))
			put("keep", "preserved")
			sealForCleaning(t, db)
			remove("gone")
			put("hot", strings.Repeat("y", 600))
			sealForCleaning(t, db)
			put("hot", "tail")
		case "clean-repack":
			for _, key := range []string{"keep", "a", "b", "c"} {
				put(key, strings.Repeat(key[:1], 256))
				sealForCleaning(t, db)
			}
		}
		if _, _, err := db.store.RetireAfterCheckpoint(); err != nil {
			t.Fatal(err)
		}
		inputs, _, blocked := selectCleaningInputs(db.store, opts.TargetSegmentBytes, db.store.CatalogEditBytes())
		if cfg.Mode == "clean-repack" {
			if len(inputs) != 0 {
				t.Fatal("repack fixture admits ordinary cleaning")
			}
			inputs, _ = smallSegmentRepackGroup(db.store.CatalogSnapshot().Entries, opts.TargetSegmentBytes, blocked)
		}
		if len(inputs) == 0 || cfg.Mode == "clean-single" && len(inputs) != 1 ||
			cfg.Mode == "clean-group" && len(inputs) < 2 || cfg.Mode == "clean-repack" && len(inputs) < 4 {
			t.Fatalf("fixture did not select its claimed policy: %s %+v", cfg.Mode, inputs)
		}
		var sources []string
		for _, entry := range inputs {
			if entry.HasPhysicalFile() {
				sources = append(sources, db.store.Segments[entry.ID].Path)
			}
		}
		capture, err = db.store.Capture()
		if err != nil {
			t.Fatal(err)
		}
		storageCfg := storage.Config{ValueCacheBudgetBytes: opts.ValueCacheBudgetBytes, MaxRecordBytes: opts.MaxRecordBytes,
			TargetSegmentBytes: opts.TargetSegmentBytes, Handles: opts.ReadFileHandles}
		if err := storage.Backup(t.Context(), capture, storageCfg, fs, output, nil, db.newIndex, nil); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("TARP_CAPTURE %d\n", Observe(db).Frontier().LastUserSequence)
		fmt.Printf("TARP_OPERATION_ACK %s\n", cfg.Mode)
		armed.Store(true)
		close(startCleaner)
		for _, path := range sources {
			waitForUnlink(t, db, path)
		}
		select {
		case <-retired:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
		if !interleaved.Load() {
			t.Fatal("automatic cleaner did not overlap an acknowledged tail")
		}
	case "ordered-initial", "ordered-delta", "ordered-reuse":
		want := make(map[string]uint64)
		previous := make(map[uint64]bool)
		for _, key := range []string{"keep", "z", "a", "m", "c", "s", "e", "q"} {
			put(key, strings.Repeat(key[:1], 36))
			want[key] = Observe(db).Frontier().LastUserSequence
		}
		if cfg.Mode != "ordered-initial" {
			if _, err := db.CompactByKey(t.Context()); err != nil {
				t.Fatal(err)
			}
			checkKeyOrderedSnapshot(t, db, want)
			for _, entry := range db.store.CatalogSnapshot().Entries {
				if entry.Kind == format.SnapshotKind {
					previous[entry.ID] = true
				}
			}
			if cfg.Mode == "ordered-delta" {
				put("a", "updated")
				want["a"] = Observe(db).Frontier().LastUserSequence
				remove("c")
				put("c", "reinserted")
				want["c"] = Observe(db).Frontier().LastUserSequence
			}
			put("zz", "new-range")
			want["zz"] = Observe(db).Frontier().LastUserSequence
		}
		sealForCleaning(t, db)
		armed.Store(true)
		if _, err := db.CompactByKey(t.Context()); err != nil {
			t.Fatal(err)
		}
		checkKeyOrderedSnapshot(t, db, want)
		var reused int
		for _, entry := range db.store.CatalogSnapshot().Entries {
			if previous[entry.ID] {
				reused++
			}
		}
		if len(previous) != 0 && reused == 0 || cfg.Mode == "ordered-reuse" && reused != len(previous) {
			t.Fatalf("ordered fixture did not reuse its unaffected ranges: %d of %d", reused, len(previous))
		}
		fmt.Printf("TARP_REUSED %d\n", reused)
	case "index-explicit", "index-close":
		put("keep", "preserved")
		put("hot", "old")
		if err := db.CheckpointIndex(t.Context()); err != nil {
			t.Fatal(err)
		}
		put("hot", "new")
		put("gone", "old")
		remove("gone")
		sealForCleaning(t, db)
		armed.Store(true)
		if cfg.Mode == "index-close" {
			err = db.Close()
		} else {
			err = db.CheckpointIndex(t.Context())
		}
		if err != nil {
			t.Fatal(err)
		}
	case "repair-data", "repair-parity":
		put("keep", strings.Repeat("v", 3000))
		put("hot", "preserved")
		if err := db.ProtectParity(t.Context()); err != nil {
			t.Fatal(err)
		}
		entry := db.store.CatalogSnapshot().Entries[0]
		path := db.store.Segments[entry.ID].Path
		offset := int64(format.SegmentHeaderBytes + format.RecordHeaderBytes + len("keep"))
		if cfg.Mode == "repair-parity" {
			path, offset = selectedParity(t, db, entry), 32
		}
		fd, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		var b [1]byte
		_, readErr := fd.ReadAt(b[:], offset)
		b[0] ^= 1
		_, writeErr := fd.WriteAt(b[:], offset)
		if err := errors.Join(readErr, writeErr, fd.Sync(), fd.Close()); err != nil {
			t.Fatal(err)
		}
		Observe(db).ClearCache()
		armed.Store(true)
		if cfg.Mode == "repair-data" {
			value, err := db.Get([]byte("keep"))
			if err != nil || !bytes.Equal(value, bytes.Repeat([]byte("v"), 3000)) {
				t.Fatalf("online repair: %v", err)
			}
		} else if err := db.ProtectParity(t.Context()); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown extended qualification mode %q", cfg.Mode)
	}
	barrier("operation.ack:before")
	fmt.Printf("TARP_OPERATION_ACK %s\n", cfg.Mode)
	barrier("operation.ack:after")
	t.Fatalf("selected phase not reached: %s %s", cfg.Name, cfg.Phase)
}

func TestExtendedQualificationPhaseReachability(t *testing.T) {
	if name := os.Getenv("TARP_EXTENDED_PROCESS_CASE"); name != "" {
		all, err := qualification.CampaignCases("all")
		if err != nil {
			t.Fatal(err)
		}
		for _, cfg := range all {
			if cfg.Name == name {
				root := os.Getenv("TARP_EXTENDED_PROCESS_ROOT")
				runExtendedPowerCase(t, cfg, filepath.Join(root, "db"), filepath.Join(root, "output"), func(point string) {
					if point == cfg.Phase {
						fmt.Printf("TARP_PHASE %s\n", point)
						os.Exit(86)
					}
				})
				return
			}
		}
		t.Fatal("unknown process-cut case")
	}
	for _, group := range []string{"batches", "cleaner", "ordered", "indexes", "repair"} {
		cases, err := qualification.CampaignCases(group)
		if err != nil {
			t.Fatal(err)
		}
		for _, cfg := range cases {
			if os.Getenv("TARP_EXTENDED_PHASE_MATRIX") != "all" &&
				(cfg.IndexMode != "btree" || !strings.HasSuffix(cfg.Phase, ".rename:after")) {
				continue
			}
			t.Run(cfg.Name, func(t *testing.T) {
				root := t.TempDir()
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExtendedQualificationPhaseReachability$")
				cmd.Env = append(os.Environ(), "TARP_EXTENDED_PROCESS_CASE="+cfg.Name, "TARP_EXTENDED_PROCESS_ROOT="+root)
				data, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != 86 {
					t.Fatalf("phase fixture: %v\n%s", err, data)
				}
				var events []qualification.OracleEvent
				for _, line := range strings.Split(string(data), "\n") {
					if !strings.HasPrefix(line, "TARP_") {
						continue
					}
					event, ok, err := qualification.ParseOracle(line)
					if err != nil || !ok {
						t.Fatalf("invalid witness %q: %v", line, err)
					}
					events = append(events, event)
				}
				oracle, err := qualification.ReplayOracle(events, false)
				if err != nil || oracle.Phase != cfg.Phase {
					t.Fatalf("phase witness: %v\n%s", err, data)
				}
				if cfg.Mode == "repair-data" {
					if err := storage.RepairDataFromParity(t.Context(), filepath.Join(root, "db"), nil); err != nil {
						t.Fatal(err)
					}
				}
				db, err := OpenWithHooks(filepath.Join(root, "db"), extendedCaseOptions(cfg), nil, parkedMaintenance())
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				sequence := Observe(db).Frontier().LastUserSequence
				if _, err := oracle.VerifyAt(sequence, Observe(db).LiveKeys(), func(key []byte) ([]byte, bool, error) {
					value, err := db.Get(key)
					if errors.Is(err, ErrNotFound) {
						return nil, false, nil
					}
					return value, err == nil, err
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
