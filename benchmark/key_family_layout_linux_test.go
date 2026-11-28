package benchmark

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

const (
	familyPrefixBytes = 10 // domain, generation, family
	familyVertices    = 1536
	familyEdges       = 4096
	familyRelays      = 32
	familyVertexValue = 16 << 10
	familyEdgeValue   = 1 << 10
	familyRelayValue  = 128
)

var familyOptions = engine.Options{
	IndexMode: engine.IndexBTree, TargetSegmentBytes: 32 << 20, MaxRecordBytes: 32 << 10,
	BTreeMemoryBudgetBytes: 8 << 20, ValueCacheBudgetBytes: 1 << 20, ReadFileHandles: 16,
}

func familyKey(domain, family byte, id int) []byte {
	prefix := append(binary.BigEndian.AppendUint64([]byte{domain}, 1), family)
	return binary.BigEndian.AppendUint64(prefix, uint64(id))
}

func familyFixtureRows() (seed, edges, relays []tarp.Entry) {
	rng := rand.New(rand.NewPCG(194, 2253))
	add := func(key []byte, size int) tarp.Entry {
		value := make([]byte, size)
		for offset := 0; offset < size; offset += 8 {
			binary.LittleEndian.PutUint64(value[offset:], rng.Uint64())
		}
		row := tarp.Entry{Key: key, Value: value}
		seed = append(seed, row)
		return row
	}
	for id := range familyVertices {
		add(familyKey('G', 1, id), familyVertexValue)
	}
	for id := range familyEdges {
		edges = append(edges, add(familyKey('G', 2, id), familyEdgeValue))
	}
	for id := range familyRelays {
		relays = append(relays, add(familyKey('S', 1, id), familyRelayValue))
	}
	rng.Shuffle(len(seed), func(i, j int) { seed[i], seed[j] = seed[j], seed[i] })
	return seed, edges, relays
}

func familyLayoutRow(t *testing.T, row map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("KEY_FAMILY_LAYOUT %s", encoded)
}

func familyOpen(t *testing.T, dir string, cleanerStart <-chan struct{}, fs *storage.IO) *engine.DB {
	t.Helper()
	db, err := engine.OpenWithHooks(dir, familyOptions, fs, &engine.Hooks{
		CleanerStart: cleanerStart, ParityStart: make(chan struct{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func familyClose(t *testing.T, db *engine.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func familyBatch(version byte, sets ...[]tarp.Entry) *tarp.WriteBatch {
	batch := new(tarp.WriteBatch)
	for _, rows := range sets {
		for _, row := range rows {
			value := bytes.Clone(row.Value)
			value[0] ^= version
			batch.Put(row.Key, value)
		}
	}
	return batch
}

func familyApply(t *testing.T, db *engine.DB, batch *tarp.WriteBatch) {
	t.Helper()
	if err := (*tarp.DB)(db).Apply(batch); err != nil {
		t.Fatal(err)
	}
}

type familySelectedFile struct {
	path   string
	length int64
}

func familySelected(db *engine.DB) map[uint64]familySelectedFile {
	files := make(map[uint64]familySelectedFile)
	engine.Observe(db).Store(func(s *storage.Store) {
		for entry := range s.CatalogEntries().All() {
			if entry.HasPhysicalFile() {
				files[entry.ID] = familySelectedFile{s.Segments[entry.ID].Path, entry.Bytes}
			}
		}
		if s.Current.ActiveCommittedEndOffset > format.SegmentHeaderBytes {
			files[s.Current.ActiveID] = familySelectedFile{s.Segments[s.Current.ActiveID].Path, s.Current.ActiveCommittedEndOffset}
		}
	})
	return files
}

func familyMembership(t *testing.T, files map[uint64]familySelectedFile) int {
	t.Helper()
	mixed := 0
	for _, entry := range files {
		file, err := os.Open(entry.path)
		if err != nil {
			t.Fatal(err)
		}
		var first string
		var other bool
		err = format.Scan(t.Context(), file, entry.length, func(_ int64, _ format.RecordHeader, key string) error {
			if len(key) < familyPrefixBytes {
				return fmt.Errorf("short benchmark key %x", key)
			}
			prefix := key[:familyPrefixBytes]
			if first == "" {
				first = prefix
			}
			other = other || prefix != first
			return nil
		})
		if err := errors.Join(err, file.Close()); err != nil {
			t.Fatal(err)
		}
		if other {
			mixed++
		}
	}
	return mixed
}

func familyValueScans(t *testing.T, db *engine.DB, round int, layout string) {
	t.Helper()
	disk, err := openContainerBatchIO(engine.Observe(db).Dir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	}()
	for offset := range 2 {
		index := (offset + round) % 2
		family := byte(index + 1)
		names := [2]string{"vertex", "edge"}
		records := [2]int{familyVertices, familyEdges}
		sizes := [2]int{familyVertexValue, familyEdgeValue}
		prefix := familyKey('G', family, 0)[:familyPrefixBytes]
		for _, cache := range []string{"advised", "warm-repeat"} {
			var adviceErr error
			if cache == "advised" {
				adviceErr = disk.Evict()
			}
			engine.Observe(db).ClearCache()
			before, err := disk.ReadBytes()
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			var count, valueBytes int
			for row, err := range db.ScanPrefix(t.Context(), prefix) {
				if err != nil || !bytes.HasPrefix(row.Key, prefix) {
					t.Fatalf("%s scan returned key %x: %v", names[index], row.Key, err)
				}
				count++
				valueBytes += len(row.Value)
			}
			elapsed := time.Since(start)
			after, err := disk.ReadBytes()
			if err != nil {
				t.Fatal(err)
			}
			if count != records[index] || valueBytes != records[index]*sizes[index] || after < before {
				t.Fatalf("%s scan rows/bytes %d/%d, read counter %d to %d", names[index], count, valueBytes, before, after)
			}
			row := map[string]any{"kind": "scan", "round": round + 1, "layout": layout, "family": names[index], "cache": cache,
				"elapsed_ns": elapsed.Nanoseconds(), "records": count, "value_bytes": valueBytes,
				"process_read_bytes": after - before, "filesystem": disk.Filesystem}
			if adviceErr != nil {
				row["advice_error"] = adviceErr.Error()
			}
			familyLayoutRow(t, row)
		}
	}
}

type familyCleanerProbe struct {
	idle        chan struct{}
	done        sync.Once
	scans       atomic.Int64
	dataBytes   atomic.Int64
	parityBytes atomic.Int64
}

func (p *familyCleanerProbe) io() *storage.IO {
	return &storage.IO{
		Hook: func(point string) error {
			switch point {
			case "measure.family.cleaner.idle":
				p.done.Do(func() { close(p.idle) })
			case "clean.scan:after":
				p.scans.Add(1)
			}
			return nil
		},
		Wrote: func(point string, bytes int) {
			switch point {
			case "clean.header", "clean.copy":
				p.dataBytes.Add(int64(bytes))
			case "clean.parity.write":
				p.parityBytes.Add(int64(bytes))
			}
		},
	}
}

func familyMaintenance(t *testing.T, db *engine.DB, round int, layout string, gate chan struct{}, probe *familyCleanerProbe) {
	t.Helper()
	before := db.Stats()
	inputs := familySelected(db)
	start := time.Now()
	close(gate)
	select {
	case <-probe.idle:
	case <-time.After(30 * time.Second):
		t.Fatalf("cleaner did not become idle: %+v", db.AutoCompactionStatus())
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	elapsed := time.Since(start)
	if status := db.AutoCompactionStatus(); status.LastError != nil || status.PinnedRetiredDataBytes != 0 {
		t.Fatalf("unexpected cleaner status: %+v", status)
	}
	after := db.Stats()
	outputs := familySelected(db)
	var unlinked int64
	for id, entry := range inputs {
		if _, selected := outputs[id]; !selected {
			if _, err := os.Stat(entry.path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retired DATA not unlinked: %s: %v", entry.path, err)
			}
			unlinked += entry.length
		}
	}
	if layout == "family-boundary" && (unlinked == 0 || after.ObsoletePutBytes != 0 || probe.dataBytes.Load() != 0) {
		t.Fatalf("family-aligned Edge DATA was not reclaimed without a rewrite: before %+v, after %+v", before, after)
	}
	if layout == "ordered" && (unlinked != 0 || after.ObsoletePutBytes != before.ObsoletePutBytes) {
		t.Fatalf("mixed cold-majority snapshot should remain ineligible: before %+v, after %+v", before, after)
	}
	familyLayoutRow(t, map[string]any{"kind": "cleaner", "round": round + 1, "layout": layout, "elapsed_ns": elapsed.Nanoseconds(),
		"scanned_files": probe.scans.Load(), "unlinked_data_bytes": unlinked,
		"written_data_bytes": probe.dataBytes.Load(), "written_parity_bytes": probe.parityBytes.Load(),
		"obsolete_before": before.ObsoletePutBytes, "obsolete_after": after.ObsoletePutBytes})
}

// TestKeyFamilyLayout evaluates stock and test-only layouts with a shared
// durable history. The runner's overlay affects ordered output rotation and
// adds an idle observation point, without changing the committed format.
func TestKeyFamilyLayout(t *testing.T) {
	if os.Getenv("TARP_KEY_FAMILY_LAYOUT") != "1" {
		t.Skip("run benchmark/scripts/container-key-family-layout.sh")
	}
	seed, edges, relays := familyFixtureRows()
	seedDir := filepath.Join(t.TempDir(), "seed")
	seedDB := familyOpen(t, seedDir, make(chan struct{}), nil)
	for start := 0; start < len(seed); start += 128 {
		if err := seedDB.BatchPut(seed[start:min(start+128, len(seed))]); err != nil {
			t.Fatal(err)
		}
	}
	// Add an overwrite and a delete/reinsert cycle before taking the common
	// cut so the initial full checkpoint replays mutation history.
	familyApply(t, seedDB, familyBatch(1, edges, relays))
	var deletes tarp.WriteBatch
	for _, row := range edges[:128] {
		deletes.Delete(row.Key)
	}
	familyApply(t, seedDB, &deletes)
	familyApply(t, seedDB, familyBatch(1, edges[:128]))
	baseDigest, keys := measurementState(t, seedDB)
	baseSequence := engine.Observe(seedDB).Frontier().LastUserSequence
	familyClose(t, seedDB)
	update := familyBatch(2, edges, relays)
	layouts := [...]string{"interleaved", "ordered", "family-boundary"}
	var finalDigest string
	for round := range 3 {
		for offset := range layouts {
			layout := layouts[(offset+round)%len(layouts)]
			t.Run(fmt.Sprintf("round%d/%s", round+1, layout), func(t *testing.T) {
				dir := filepath.Join(t.TempDir(), "db")
				measurementCloneClosed(t, seedDir, dir)
				gate := make(chan struct{})
				probe := &familyCleanerProbe{idle: make(chan struct{})}
				db := familyOpen(t, dir, gate, probe.io())
				boundaries := "0"
				if layout == "family-boundary" {
					boundaries = "1"
				}
				t.Setenv("TARP_KEY_FAMILY_BOUNDARIES", boundaries)
				var compactNS int64
				if layout != "interleaved" {
					start := time.Now()
					if _, err := db.CompactByKey(t.Context()); err != nil {
						t.Fatal(err)
					}
					compactNS = time.Since(start).Nanoseconds()
				}
				digest, count := measurementState(t, db)
				if digest != baseDigest || count != keys || engine.Observe(db).Frontier().LastUserSequence != baseSequence {
					t.Fatal("layout changed logical contents or user sequence")
				}
				files := familySelected(db)
				mixed := familyMembership(t, files)
				if (layout == "family-boundary") != (mixed == 0) {
					t.Fatalf("layout %s has %d mixed DATA files", layout, mixed)
				}
				familyLayoutRow(t, map[string]any{"kind": "initial_layout", "round": round + 1, "layout": layout,
					"compact_ns": compactNS, "data_files": len(files), "mixed_files": mixed,
					"selected_data_bytes": db.Stats().SelectedDataBytes, "keys": count, "state_sha256": digest})
				familyValueScans(t, db, round, layout)
				pinned, err := db.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				defer pinned.Close()
				start := time.Now()
				familyApply(t, db, update)
				commit := time.Since(start)
				current, err := db.Snapshot()
				if err != nil {
					t.Fatal(err)
				}
				defer current.Close()
				if pinned.Sequence() != baseSequence || current.Sequence() != baseSequence+uint64(update.Len()) {
					t.Fatal("cross-family commit did not publish one complete new sequence")
				}
				for _, row := range []tarp.Entry{edges[0], relays[0]} {
					familyCheckSnapshot(t, pinned, row, 1)
					familyCheckSnapshot(t, current, row, 2)
				}
				pinned.Close()
				current.Close()
				familyLayoutRow(t, map[string]any{"kind": "mixed_commit", "round": round + 1, "layout": layout,
					"elapsed_ns": commit.Nanoseconds(), "records": update.Len(), "sequence": baseSequence + uint64(update.Len())})
				familyMaintenance(t, db, round, layout, gate, probe)
				familyClose(t, db)
				reopened := familyOpen(t, dir, make(chan struct{}), nil)
				digest, count = measurementState(t, reopened)
				if finalDigest == "" {
					finalDigest = digest
				}
				if digest != finalDigest || count != keys || engine.Observe(reopened).Frontier().LastUserSequence != baseSequence+uint64(update.Len()) {
					t.Fatal("reopen changed cross-family contents or user sequence")
				}
				for _, row := range []tarp.Entry{edges[0], relays[0]} {
					value, err := reopened.Get(row.Key)
					familyCheckValue(t, row, 2, value, err)
				}
				familyLayoutRow(t, map[string]any{"kind": "reopen", "round": round + 1, "layout": layout, "state_sha256": digest, "keys": count})
			})
		}
	}
}

func familyCheckSnapshot(t *testing.T, s *engine.Snapshot, row tarp.Entry, version byte) {
	t.Helper()
	value, err := s.GetContext(t.Context(), row.Key)
	familyCheckValue(t, row, version, value, err)
}

func familyCheckValue(t *testing.T, row tarp.Entry, version byte, value []byte, err error) {
	t.Helper()
	if err != nil || len(value) != len(row.Value) || value[0] != row.Value[0]^version || !bytes.Equal(value[1:], row.Value[1:]) {
		t.Fatalf("key %x did not retain version %d: %v", row.Key, version, err)
	}
}

// Independent DBs are a comparison control for commit latency and failure
// semantics. They are not a prototype for a shared-frontier storage layout.
func TestKeyFamilyCommitControl(t *testing.T) {
	if os.Getenv("TARP_KEY_FAMILY_LAYOUT") != "1" {
		t.Skip("run benchmark/scripts/container-key-family-layout.sh")
	}
	_, edges, relays := familyFixtureRows()
	edge, relay := edges[:32], relays[:1]
	root := t.TempDir()
	graphDir, serviceDir := filepath.Join(root, "graph"), filepath.Join(root, "service")
	combined := familyOpen(t, filepath.Join(root, "combined"), make(chan struct{}), nil)
	graph := familyOpen(t, graphDir, make(chan struct{}), nil)
	service := familyOpen(t, serviceDir, make(chan struct{}), nil)
	var samples [2][]int64
	for i := range 16 {
		version := byte(i + 1)
		one, g, s := familyBatch(version, edge, relay), familyBatch(version, edge), familyBatch(version, relay)
		for offset := range 2 {
			arm := (i + offset) % 2
			start := time.Now()
			if arm == 0 {
				familyApply(t, combined, one)
			} else {
				familyApply(t, graph, g)
				familyApply(t, service, s)
			}
			samples[arm] = append(samples[arm], time.Since(start).Nanoseconds())
		}
	}
	for i, arm := range []string{"single-db-apply", "two-db-sequential-apply"} {
		slices.Sort(samples[i])
		familyLayoutRow(t, map[string]any{"kind": "commit_control", "arm": arm, "requests": len(samples[i]),
			"latency_ns": samples[i], "p50_ns": samples[i][(len(samples[i])-1)/2]})
	}
	// Model a failure between the two independent calls. Reopen both DBs and
	// verify that the independently committed graph update has no matching relay.
	familyClose(t, service)
	familyApply(t, graph, familyBatch(17, edge))
	if err := (*tarp.DB)(service).Apply(familyBatch(17, relay)); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("expected the second independent commit to fail before publication: %v", err)
	}
	familyClose(t, graph)
	graph = familyOpen(t, graphDir, make(chan struct{}), nil)
	service = familyOpen(t, serviceDir, make(chan struct{}), nil)
	value, err := graph.Get(edge[0].Key)
	familyCheckValue(t, edge[0], 17, value, err)
	value, err = service.Get(relay[0].Key)
	familyCheckValue(t, relay[0], 16, value, err)
	familyLayoutRow(t, map[string]any{"kind": "commit_control_second_call_failure", "graph_version": 17, "relay_version": 16})
}
