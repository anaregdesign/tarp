package benchmark

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

type repackFixture struct {
	dir       string
	opts      engine.Options
	readKey   []byte
	writeKey  []byte
	value     []byte
	inputs    []format.CatalogEntry
	frontier  format.Frontier
	stateHash string
}

// A small creation target produces eight full logs of eight distinct live
// keys. Reopen the same closed image at the default target so these logs
// qualify for packing. The foreground writer updates only the active tail.
func prepareRepackFixture(t testing.TB, size int) repackFixture {
	t.Helper()
	const logs, recordsPerLog = 8, 8
	keys, value, opts := measurementInput(size, logs*recordsPerLog+1)
	opts.MaxRecordBytes = int64(format.RecordHeaderBytes + len(keys[0]) + size)
	opts.TargetSegmentBytes = format.SegmentHeaderBytes + recordsPerLog*opts.MaxRecordBytes
	dir := filepath.Join(t.TempDir(), "seed")
	// Closing the seed must leave its last record in the active log.
	// The timed clones use the default index.
	seedOpts := opts
	seedOpts.IndexMode = engine.IndexMemory
	db, err := engine.OpenWithHooks(dir, seedOpts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	writerKey := keys[len(keys)-1]
	for i, key := range keys[:len(keys)-1] {
		record := bytes.Clone(value)
		record[0] ^= byte(i)
		if err := db.Put(key, record); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Put(writerKey, value); err != nil {
		t.Fatal(err)
	}
	probe := engine.Observe(db)
	fixture := repackFixture{dir: dir, opts: opts, readKey: keys[0], writeKey: writerKey, value: value, frontier: probe.Frontier()}
	probe.Store(func(s *storage.Store) {
		fixture.inputs = slices.Clone(s.CatalogSnapshot().Entries)
		if len(fixture.inputs) != logs {
			t.Fatalf("fixture has %d sealed logs, want %d", len(fixture.inputs), logs)
		}
		for _, entry := range fixture.inputs {
			if entry.Kind != format.RawLogKind || entry.Bytes != opts.TargetSegmentBytes ||
				s.Segments[entry.ID].LiveBytes.Load() != entry.Bytes-format.SegmentHeaderBytes {
				t.Fatalf("fixture log %d is not full of live records", entry.ID)
			}
		}
	})
	if fixture.frontier.LastUserSequence != uint64(len(keys)) || fixture.frontier.ActiveCommittedEndOffset != format.SegmentHeaderBytes+opts.MaxRecordBytes {
		t.Fatalf("unexpected seed frontier: %+v", fixture.frontier)
	}
	fixture.stateHash, _ = measurementState(t, db)
	fixture.opts.TargetSegmentBytes = 4 << 20
	return fixture
}

// openClone warms exactly the reader's key before the caller starts its timer.
// The caller defers cleanup for this sample, including on benchmark failure.
func (f repackFixture) openClone(t testing.TB, fs *storage.IO, hooks *engine.Hooks) (_ *engine.DB, cleanup func()) {
	t.Helper()
	temp, err := os.MkdirTemp(filepath.Dir(f.dir), "repack-")
	if err != nil {
		t.Fatal(err)
	}
	var db *engine.DB
	release := func() {
		if db != nil {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := os.RemoveAll(temp); err != nil {
			t.Error(err)
		}
	}
	defer func() {
		if cleanup == nil { // Setup failed before the caller took ownership.
			release()
		}
	}()
	dir := filepath.Join(temp, "db")
	measurementCloneClosed(t, f.dir, dir)
	db, err = engine.OpenWithHooks(dir, f.opts, fs, hooks)
	if err != nil {
		t.Fatal(err)
	}
	if engine.Observe(db).Frontier() != f.frontier {
		t.Fatal("clone started at a different frontier")
	}
	if got := repackDataFileCount(t, dir); got != len(f.inputs)+1 {
		t.Fatalf("clone has %d physical DATA files, want %d", got, len(f.inputs)+1)
	}
	if v, err := db.Get(f.readKey); err != nil || !bytes.Equal(v, f.value) {
		t.Fatalf("could not warm the fixture read key: %v", err)
	}
	return db, release
}

func repackDataFileCount(t testing.TB, dir string) int {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "DATA-*"))
	if err != nil {
		t.Fatal(err)
	}
	return len(files)
}

// Inspect the repacked layout before Close seals the foreground tail for its
// index checkpoint. Verify the complete contents again on reopen.
func (f repackFixture) verify(t testing.TB, db *engine.DB, repacked bool, puts int) (int, string) {
	t.Helper()
	probe := engine.Observe(db)
	dir := probe.Dir()
	fileCount := repackDataFileCount(t, dir)
	wantFiles := len(f.inputs) + 1
	if repacked {
		wantFiles = 2
	}
	if fileCount != wantFiles {
		t.Fatalf("repacked=%t: found %d physical DATA files; want %d", repacked, fileCount, wantFiles)
	}
	frontier := probe.Frontier()
	if frontier.ActiveID != f.frontier.ActiveID || frontier.LastUserSequence != f.frontier.LastUserSequence+uint64(puts) ||
		frontier.ActiveCommittedEndOffset != f.frontier.ActiveCommittedEndOffset+int64(puts)*f.opts.MaxRecordBytes {
		t.Fatalf("foreground writes changed the expected frontier: %+v after %d Puts", frontier, puts)
	}
	probe.Store(func(s *storage.Store) {
		entries := s.CatalogSnapshot().Entries
		if !repacked {
			if !slices.Equal(entries, f.inputs) {
				t.Fatalf("parked worker changed selected logs: %+v", entries)
			}
			return
		}
		if len(entries) != 1 || entries[0].Kind != format.CleanedLogKind || entries[0].ID == 0 ||
			entries[0].FirstSequence != f.inputs[0].FirstSequence || entries[0].LastSequence != f.inputs[len(f.inputs)-1].LastSequence || s.Catalog.CheckpointCutoffSequence != 0 {
			t.Fatalf("unexpected repacked catalog: %+v", s.Catalog)
		}
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := engine.OpenWithHooks(dir, f.opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	}()
	if got := engine.Observe(reopened).Frontier(); got.LastUserSequence != frontier.LastUserSequence || got.UUID != frontier.UUID {
		t.Fatalf("reopened clone has unexpected identity or sequence: %+v", got)
	}
	hash, count := measurementState(t, reopened)
	if hash != f.stateHash || uint64(count) != f.frontier.LastUserSequence {
		t.Fatalf("state after reopen: %d keys, digest %s", count, hash)
	}
	return fileCount, hash
}
