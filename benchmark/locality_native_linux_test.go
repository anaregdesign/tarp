package benchmark

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"iter"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

const localityNativeHeads = 64
const localityNativeTails = 256
const localityNativeKeys = localityNativeHeads * localityNativeTails

func localityNativeKey(id int) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint32(key, uint32(id/localityNativeHeads))
	binary.BigEndian.PutUint32(key[4:], uint32(id%localityNativeHeads))
	return key
}

func localityNativeID(key []byte) int {
	return int(binary.BigEndian.Uint32(key))*localityNativeHeads + int(binary.BigEndian.Uint32(key[4:]))
}

func localityNativeValue(id, version int) []byte {
	value := make([]byte, 456)
	binary.BigEndian.PutUint64(value, uint64(id))
	binary.BigEndian.PutUint64(value[8:], uint64(version))
	return value
}

func localityNativeWrites(t *testing.T, db *tarp.DB, ids []int, version []int) {
	t.Helper()
	for chunk := range slices.Chunk(ids, 128) {
		batch := make([]tarp.Entry, 0, len(chunk))
		for _, id := range chunk {
			version[id]++
			batch = append(batch, tarp.Entry{Key: localityNativeKey(id), Value: localityNativeValue(id, version[id])})
		}
		if err := db.BatchPut(batch); err != nil {
			t.Fatal(err)
		}
	}
}

func localityNativeOpen(t *testing.T, path string, opts tarp.Options) *tarp.DB {
	t.Helper()
	park := make(chan struct{})
	raw, err := engine.OpenWithHooks(path, engine.Options(opts), nil, &engine.Hooks{CleanerStart: park, ParityStart: park})
	if err != nil {
		t.Fatal(err)
	}
	return (*tarp.DB)(raw)
}

type localityNativeRecordPos struct {
	file      uint64
	off, size int64
}

func localityNativeLayout(t *testing.T, db *tarp.DB) (locs []localityNativeRecordPos, sorted bool, snapshotBytes int64) {
	t.Helper()
	locs = make([]localityNativeRecordPos, localityNativeKeys)
	sorted = true
	prev := -1
	engine.Observe((*engine.DB)(db)).Store(func(s *storage.Store) {
		files := slices.Clone(s.Catalog.Files)
		active, err := s.Catalog.ActiveFile(s.Current)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range append(files, active) {
			if !e.HasPhysicalFile() {
				continue
			}
			f, err := os.Open(s.Files[e.ID].Path)
			if err != nil {
				t.Fatal(err)
			}
			if e.Kind == format.Snapshot {
				snapshotBytes += e.Bytes
			}
			err = format.Scan(t.Context(), f, e.Bytes, func(off int64, r format.Record, key string) error {
				if len(key) != 8 || r.Kind != format.Put {
					return fmt.Errorf("unexpected record in locality fixture")
				}
				id := localityNativeID([]byte(key))
				if id >= len(locs) {
					return fmt.Errorf("invalid logical key %d", id)
				}
				if e.Kind == format.Snapshot {
					sorted = sorted && id > prev
					prev = id
				}
				locs[id] = localityNativeRecordPos{e.ID, off, r.Bytes()}
				return nil
			})
			closeErr := f.Close()
			if err != nil || closeErr != nil {
				t.Fatalf("scan physical fixture: %v, close %v", err, closeErr)
			}
		}
	})
	return
}

func localityNativePages(locs []localityNativeRecordPos, tail int) int {
	pages := make(map[[2]uint64]bool)
	for _, l := range locs[tail*localityNativeHeads : (tail+1)*localityNativeHeads] {
		for page := l.off / 4096; page <= (l.off+l.size-1)/4096; page++ {
			pages[[2]uint64{l.file, uint64(page)}] = true
		}
	}
	return len(pages)
}

func localityNativeVerify(t *testing.T, db *tarp.DB, versions []int) {
	t.Helper()
	seen := make([]bool, len(versions))
	count := 0
	for entry, err := range db.Scan(t.Context()) {
		if err != nil {
			t.Fatal(err)
		}
		id := localityNativeID(entry.Key)
		if id >= len(versions) || seen[id] || !bytes.Equal(entry.Value, localityNativeValue(id, versions[id])) {
			t.Fatalf("wrong or duplicate row %d", id)
		}
		seen[id] = true
		count++
	}
	if count != len(versions) {
		t.Fatalf("found %d rows, want %d", count, len(versions))
	}
}

func localityNativeLog(t *testing.T, row map[string]any) {
	t.Helper()
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("LOCALITY_NATIVE %s", data)
}

func localityNativeColdRead(t *testing.T, db *tarp.DB, dir, arm, trace string, trial int, versions []int) {
	t.Helper()
	locs, sorted, _ := localityNativeLayout(t, db)
	if sorted != (arm == "sorted") {
		t.Fatalf("snapshot order = %t for %s", sorted, arm)
	}
	sum := 0
	for tail := range localityNativeTails {
		sum += localityNativePages(locs, tail)
	}
	files, err := openContainerBatchIO(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := files.Evict(); err != nil {
		t.Fatal(err)
	}
	before, err := files.ReadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := files.Warm(); err != nil {
		t.Fatal(err)
	}
	after, err := files.ReadBytes()
	if err != nil || after <= before {
		t.Fatalf("cold read control %d -> %d, %v", before, after, err)
	}
	type prefixReader interface {
		ScanPrefix(context.Context, []byte) iter.Seq2[tarp.Entry, error]
	}
	reader, ok := any(db).(prefixReader)
	if !ok {
		t.Fatal("use pinned prefix reader build")
	}
	for _, tail := range []int{0, localityNativeTails / 2, localityNativeTails - 1} {
		if err := files.Evict(); err != nil {
			t.Fatal(err)
		}
		read0, err := files.ReadBytes()
		if err != nil {
			t.Fatal(err)
		}
		p := localityNativeKey(tail * localityNativeHeads)[:4]
		var seen [localityNativeHeads]bool
		n := 0
		start := time.Now()
		for entry, err := range reader.ScanPrefix(t.Context(), p) {
			if err != nil {
				t.Fatal(err)
			}
			id := localityNativeID(entry.Key)
			if id/localityNativeHeads != tail || seen[id%localityNativeHeads] || len(entry.Value) != 456 || binary.BigEndian.Uint64(entry.Value) != uint64(id) || binary.BigEndian.Uint64(entry.Value[8:]) != uint64(versions[id]) {
				t.Fatalf("bad prefix result %d", id)
			}
			seen[id%localityNativeHeads] = true
			n++
		}
		elapsed := time.Since(start)
		read1, err := files.ReadBytes()
		if err != nil || read1 < read0 || n != localityNativeHeads {
			t.Fatalf("returned %d, counter/error %v", n, err)
		}
		localityNativeLog(t, map[string]any{"kind": "read", "arm": arm, "trace": trace, "trial": trial, "tail": tail, "matches": n, "file_pages": localityNativePages(locs, tail), "mean_file_pages": float64(sum) / localityNativeTails, "read_bytes": read1 - read0, "ns": elapsed.Nanoseconds(), "full_read_control_bytes": after - before})
	}
}

// TestLocalityNative runs only in the research build with the pinned prefix
// reader. Each arm uses the genuine Compact, backup, restore and reopen flows.
func TestLocalityNative(t *testing.T) {
	arm := os.Getenv("TARP_LOCALITY_NATIVE")
	if arm == "" {
		t.Skip("set TARP_LOCALITY_NATIVE via container-key-locality.sh")
	}
	if arm != "physical" && arm != "staged" && arm != "sorted" {
		t.Fatal("invalid native arm")
	}
	opts := tarp.Options{IndexMode: tarp.IndexBTree, TargetSegmentBytes: 1 << 20, MaxRecordBytes: 64 << 10}
	for trial := 1; trial <= 3; trial++ {
		t.Run(fmt.Sprint(trial), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			db := localityNativeOpen(t, dir, opts)
			want := make([]int, localityNativeKeys)
			randSource := rand.New(rand.NewPCG(150, 1))
			localityNativeWrites(t, db, randSource.Perm(localityNativeKeys), want)
			var m0, m1 runtime.MemStats
			runtime.ReadMemStats(&m0)
			start := time.Now()
			result, err := db.Compact(t.Context())
			elapsed := time.Since(start)
			runtime.ReadMemStats(&m1)
			if err != nil || result.DroppedKeys != 0 {
				t.Fatalf("Compact = %+v, %v", result, err)
			}
			_, sorted, output := localityNativeLayout(t, db)
			if sorted != (arm == "sorted") || output == 0 {
				t.Fatalf("unexpected checkpoint physical order %t or bytes %d", sorted, output)
			}
			localityNativeLog(t, map[string]any{"kind": "checkpoint", "arm": arm, "trial": trial, "ns": elapsed.Nanoseconds(), "snapshot_bytes": output, "allocated_bytes": m1.TotalAlloc - m0.TotalAlloc, "sequence": engine.Observe((*engine.DB)(db)).Frontier().Sequence})
			backup := filepath.Join(t.TempDir(), "base")
			if err := db.Backup(t.Context(), backup); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, trace := range []string{"unchanged", "first", "last", "spread"} {
				t.Run(trace, func(t *testing.T) {
					dir := filepath.Join(t.TempDir(), "work")
					if err := tarp.Restore(t.Context(), tarp.RestoreRequest{Source: backup, Destination: dir, Limits: opts.RestoreLimits()}); err != nil {
						t.Fatal(err)
					}
					d := localityNativeOpen(t, dir, opts)
					versions := slices.Clone(want)
					if trace != "unchanged" {
						ids := make([]int, 4096)
						rng := rand.New(rand.NewPCG(150, 2))
						for i := range ids {
							switch trace {
							case "last":
								ids[i] = localityNativeKeys - 1
							case "spread":
								ids[i] = rng.IntN(localityNativeKeys)
							}
						}
						localityNativeWrites(t, d, ids, versions)
					}
					if err := d.Close(); err != nil {
						t.Fatal(err)
					}
					d = localityNativeOpen(t, dir, opts)
					defer func() {
						if err := d.Close(); err != nil {
							t.Error(err)
						}
					}()
					localityNativeVerify(t, d, versions)
					localityNativeColdRead(t, d, dir, arm, trace, trial, versions)
				})
			}
		})
	}
}
