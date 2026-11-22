package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

// Set TARP_EDGE_PREFIX_IO=seed to write both immutable backups into the
// shared TARP_EDGE_PREFIX_FIXTURES directory. Run filter on original main
// and prefix on the prototype. Both restore the same DATA bytes per layout.
func TestEdgePrefixDataIO(t *testing.T) {
	arm := os.Getenv("TARP_EDGE_PREFIX_IO")
	switch arm {
	case "":
		t.Skip("set TARP_EDGE_PREFIX_IO and TARP_EDGE_PREFIX_FIXTURES to run")
	case "seed", "filter", "prefix":
	default:
		t.Fatal("TARP_EDGE_PREFIX_IO must be seed, filter or prefix")
	}
	root := os.Getenv("TARP_EDGE_PREFIX_FIXTURES")
	if root == "" {
		t.Fatal("TARP_EDGE_PREFIX_FIXTURES must name a directory for the shared backups")
	}
	if arm == "seed" {
		if err := os.MkdirAll(root, 0700); err != nil {
			t.Fatal(err)
		}
	}
	opts := tarp.Options{IndexMode: tarp.IndexBTree, TargetSegmentBytes: 256 << 10, MaxRecordBytes: 64 << 10}
	for _, layout := range []string{"grouped", "interleaved"} {
		t.Run(layout, func(t *testing.T) {
			source := filepath.Join(root, layout)
			edges := edgePrefixData(layout == "interleaved")
			if arm == "seed" {
				db := newEdgePrefixFixture(t, opts, edges)
				if err := db.Backup(t.Context(), source); err != nil {
					t.Fatal(err)
				}
				return
			}
			info, err := tarp.VerifyBackup(t.Context(), source)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "restored")
			if err := tarp.Restore(t.Context(), tarp.RestoreRequest{Source: source, Destination: dir, Limits: opts.RestoreLimits()}); err != nil {
				t.Fatal(err)
			}
			db, err := tarp.OpenExisting(t.Context(), dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			native := edgePrefixArm(t, db, arm == "prefix")
			// Warm and verify all logical rows once. Evictions below affect
			// DATA only; index files and the B+tree user cache stay eligible for reuse.
			all := edgePrefixQuery{}
			all.verify(t, db, edges, true, nil)
			storage, err := openContainerBatchIO(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := storage.Close(); err != nil {
					t.Error(err)
				}
			})
			if len(storage.files) < 2 {
				t.Fatalf("expected multiple DATA files, got %d", len(storage.files))
			}

			// Check that a full-file read after advice produces observable
			// process I/O on this filesystem. Record even a zero control so
			// an unsupported cold-storage inference is identifiable in the data.
			if err := storage.Evict(); err != nil {
				t.Fatal(err)
			}
			before := readPrefixIOBytes(t, storage)
			if err := storage.Warm(); err != nil {
				t.Fatal(err)
			}
			control := prefixIOBytesSince(t, storage, before)
			for _, op := range []string{"Keys", "Scan"} {
				for _, query := range []struct {
					name string
					tail int
				}{{"medium", 192}, {"broad", 384}} {
					t.Run(op+"/"+query.name, func(t *testing.T) {
						q := edgePrefixQuery{prefix: edgeTailPrefix(query.tail)}
						values := op == "Scan"
						n := q.verify(t, db, edges, values, native)
						for trial := 1; trial <= 3; trial++ {
							if err := storage.Evict(); err != nil {
								t.Fatal(err)
							}
							before := readPrefixIOBytes(t, storage)
							start := time.Now()
							counts, err := q.run(t.Context(), db, values, native, nil)
							ns := time.Since(start).Nanoseconds()
							charged := prefixIOBytesSince(t, storage, before)
							if err != nil || counts.matches != n {
								t.Fatalf("got %d matches, want %d: %v", counts.matches, n, err)
							}
							row := map[string]any{
								"arm": arm, "layout": layout, "operation": op, "query": query.name, "trial": trial,
								"database_id": fmt.Sprintf("%x", info.DatabaseID), "sequence": info.Sequence,
								"data_files": len(storage.files), "filesystem": storage.Filesystem, "device": storage.Device,
								"matches": n, "yielded": counts.candidates, "yielded_value_bytes": counts.valueBytes,
								"elapsed_ns": ns, "process_read_bytes": charged, "full_read_control_bytes": control,
							}
							data, err := json.Marshal(row)
							if err != nil {
								t.Fatal(err)
							}
							t.Logf("EDGE_PREFIX_IO %s", data)
						}
					})
				}
			}
		})
	}
}

func readPrefixIOBytes(t *testing.T, storage *containerBatchIO) uint64 {
	t.Helper()
	bytes, err := storage.ReadBytes()
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

func prefixIOBytesSince(t *testing.T, storage *containerBatchIO, before uint64) uint64 {
	t.Helper()
	after := readPrefixIOBytes(t, storage)
	if after < before {
		t.Fatal("process read_bytes decreased")
	}
	return after - before
}
