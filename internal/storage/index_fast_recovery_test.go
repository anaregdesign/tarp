package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func installFastTestIndex(t *testing.T, s *Store) {
	t.Helper()
	work, err := s.BeginIndexCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	tree, err := btree.NewIn(t.TempDir(), btree.MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	if err := work.Write(t.Context(), tree, btree.ExportPacked); err != nil {
		t.Fatal(err)
	}
	if err := work.PublishFile(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(tree.Close(), work.Install(), work.Release()); err != nil {
		t.Fatal(err)
	}
}

func openFastTestIndex(t *testing.T, dir string, cfg Config, probe *IO) (*Store, index.Backend) {
	t.Helper()
	scratch := t.TempDir()
	fresh := func(*Store) (index.Backend, error) { return memory.New(), nil }
	importer := func(ctx context.Context, _ *Store, r io.Reader, size int64, visit func(string, index.Address) error) (index.Backend, error) {
		return btree.ImportPacked(ctx, r, size, scratch, btree.MinBudget, visit)
	}
	fast := func(ctx context.Context, _ *Store, pages io.ReaderAt, closer io.Closer, size int64) (index.Backend, error) {
		return btree.OpenPacked(ctx, pages, closer, size, scratch, btree.MinBudget)
	}
	s, keys, err := OpenIndexed(t.Context(), dir, cfg, true, probe, fresh, importer, fast)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := errors.Join(keys.Close(), s.Close()); err != nil {
			t.Error(err)
		}
	})
	return s, keys
}

func appendFastTestBase(t *testing.T, s *Store) {
	t.Helper()
	base := make([]Entry, 1500)
	for i := range base {
		base[i] = Entry{Key: []byte(fmt.Sprintf("base-%06d", i)), Value: []byte("value")}
	}
	if _, err := s.AppendBatch(format.Put, base, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
}

func TestFastIndexReopensAfterCoveredAndSuffixSegmentsAreCleaned(t *testing.T) {
	cfg := Config{IndexedOpen: true, Handles: 1, MaxRecordBytes: 1 << 20, ValueCacheBudgetBytes: 4 << 20, TargetSegmentBytes: 4 << 20}
	dir := t.TempDir()
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendFastTestBase(t, s)
	healthy := func() error { return nil }
	for _, key := range []string{"retained", "overwrite", "deleted"} {
		if _, err := s.Append(format.Put, []byte(key), []byte("old"), healthy); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	installFastTestIndex(t, s)
	checkpointID := s.Catalog.Index.ID
	overwritten, err := s.Append(format.Put, []byte("overwrite"), []byte("new"), healthy)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(format.Delete, []byte("deleted"), nil, healthy); err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	original := []format.CatalogEntry{s.CatalogEntries().At(1), s.CatalogEntries().At(2)}
	workspace, err := s.BeginClean(original)
	if err != nil || workspace == nil {
		t.Fatalf("begin cleaning = %v, %v", workspace, err)
	}
	moves := s.NewRelocations()
	if err := workspace.Copy(t.Context(), s.IO, moves, nil, healthy, func(key string, addr Location) (bool, error) {
		return key == "retained" || key == "overwrite" && addr.Sequence == overwritten.Sequence, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ProtectCleaned(t.Context(), workspace, healthy); err != nil {
		t.Fatal(err)
	}
	if err := s.InstallCleaned(workspace, nil); err != nil {
		t.Fatal(err)
	}
	outputID := workspace.output.ID
	workspace.Release()
	if _, _, _, err := s.RetireCleaned(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(moves.Close(), s.Close()); err != nil {
		t.Fatal(err)
	}

	reads := make(map[string]int64)
	s, keys := openFastTestIndex(t, dir, cfg, &IO{OpenRead: func(kind string, n int) { reads[kind] += int64(n) }})
	if !s.IndexCheckpointValidated || s.Catalog.Index.ID != checkpointID || keys.Count() != 1502 ||
		reads["covered_encoded_records"] != 0 || reads["replay_encoded_records"] != 0 ||
		reads["index_checkpoint_file"] >= s.Catalog.Index.Bytes {
		t.Fatalf("checkpoint=%t id=%d want %d live=%d reads=%v INDEX size=%d",
			s.IndexCheckpointValidated, s.Catalog.Index.ID, checkpointID, keys.Count(), reads, s.Catalog.Index.Bytes)
	}
	for key, expected := range map[string]string{"base-000000": "value", "retained": "old", "overwrite": "new"} {
		a, ok, err := keys.Lookup(key)
		if !ok || err != nil {
			t.Fatalf("lookup %q: %t, %v", key, ok, err)
		}
		if key != "base-000000" && a.FileID != outputID {
			t.Fatalf("%q still points to %d, want cleaned file %d", key, a.FileID, outputID)
		}
		got, err := s.Read(At(s.Segments[a.FileID], a), key)
		if string(got) != expected || err != nil {
			t.Fatalf("read %q = %q, %v", key, got, err)
		}
	}
	if _, ok, err := keys.Lookup("deleted"); ok || err != nil {
		t.Fatalf("deleted key = %t, %v", ok, err)
	}
	if got := s.Segments[outputID].LiveKeys.Load(); got != 2 {
		t.Fatalf("cleaned file has %d live keys, want 2", got)
	}
}

func TestFastIndexVerifiesColdPagesOnAccess(t *testing.T) {
	cfg := Config{IndexedOpen: true, Handles: 1, MaxRecordBytes: 1 << 20, ValueCacheBudgetBytes: 4 << 20, TargetSegmentBytes: 4 << 20}
	dir := t.TempDir()
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendFastTestBase(t, s)
	installFastTestIndex(t, s)
	file, err := s.openIndexFile(t.Context(), s.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	if file.packedSize <= btree.PackedHeaderBytes+btree.PackedPageBytes {
		t.Fatal("fixture needs a cold leaf page")
	}
	if err := errors.Join(file.fd.Close(), s.Close()); err != nil {
		t.Fatal(err)
	}
	fd, err := os.OpenFile(filepath.Join(dir, IndexName(s.Current.UUID, s.Catalog.Index.ID)), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	offset := file.packedStart + btree.PackedHeaderBytes
	var page [btree.PackedPageBytes]byte
	if _, err := fd.ReadAt(page[:], offset); err != nil {
		t.Fatal(err)
	}
	// Change a same-size field and recompute the ordinary page CRC. The
	// committed INDEX summary must still reject this page.
	page[100] ^= 0xff
	binary.LittleEndian.PutUint32(page[:4], crc32.Checksum(page[4:], crc32.MakeTable(crc32.Castagnoli)))
	_, err = fd.WriteAt(page[:], offset)
	if err := errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	t.Run("Indexed", func(t *testing.T) {
		reopened, keys := openFastTestIndex(t, dir, cfg, nil)
		if !reopened.IndexCheckpointValidated {
			t.Fatal("Indexed Open unexpectedly traversed the cold leaf")
		}
		if _, _, err := keys.Lookup("base-000000"); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("cold leaf read = %v, want integrity failure", err)
		}
	})
	t.Run("Full", func(t *testing.T) {
		cfg.IndexedOpen = false
		reopened, keys := openFastTestIndex(t, dir, cfg, nil)
		if reopened.IndexCheckpointValidated {
			t.Fatal("Full Open reused the checkpoint with an altered leaf")
		}
		if _, found, err := keys.Lookup("base-000000"); !found || err != nil {
			t.Fatalf("Full fallback lookup = %t, %v", found, err)
		}
	})
}

func TestFastIndexKeepsReadingAfterItsFileIsRetired(t *testing.T) {
	cfg := Config{IndexedOpen: true, Handles: 1, MaxRecordBytes: 1 << 20, ValueCacheBudgetBytes: 4 << 20, TargetSegmentBytes: 4 << 20}
	dir := t.TempDir()
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendFastTestBase(t, s)
	installFastTestIndex(t, s)
	previous := s.Catalog.Index.ID
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, keys := openFastTestIndex(t, dir, cfg, nil)
	if !s.IndexCheckpointValidated {
		t.Fatal("expected fast INDEX")
	}
	installFastTestIndex(t, s)
	if s.Catalog.Index.ID == previous {
		t.Fatal("the new checkpoint did not replace the older one")
	}
	previousPath := filepath.Join(dir, IndexName(s.Current.UUID, previous))
	if _, err := os.Stat(previousPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("obsolete INDEX has not been retired: %v", err)
	}
	// The open immutable base can serve pages that were not read at Open.
	if _, ok, err := keys.Lookup("base-000000"); !ok || err != nil {
		t.Fatalf("lookup from retired INDEX = %t, %v", ok, err)
	}
}

func TestFastIndexFallsBackWhenDeletedMaximumWouldRejectSmallerOptions(t *testing.T) {
	cfg := Config{IndexedOpen: true, Handles: 1, MaxRecordBytes: 1 << 20, ValueCacheBudgetBytes: 4 << 20, TargetSegmentBytes: 4 << 20}
	dir := t.TempDir()
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	appendFastTestBase(t, s)
	healthy := func() error { return nil }
	if _, err := s.Append(format.Put, []byte("large"), bytes.Repeat([]byte("x"), 4096), healthy); err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	installFastTestIndex(t, s)
	if _, err := s.Append(format.Delete, []byte("large"), nil, healthy); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.MaxRecordBytes = 128
	reopened, keys := openFastTestIndex(t, dir, cfg, nil)
	if reopened.IndexCheckpointValidated || keys.Count() != 1500 {
		t.Fatalf("checkpoint=%t live=%d; want exact replay without deleted key", reopened.IndexCheckpointValidated, keys.Count())
	}
}

func TestFastIndexReplayBudgetBeforeOpeningLargeSuffix(t *testing.T) {
	image := &Image{
		Current: format.Frontier{ActiveID: 1, LastUserSequence: 1, ActiveCommittedEndOffset: maxIndexedReplayBytes + 1},
		Catalog: format.Catalog{ActiveID: 1, Index: format.IndexCheckpoint{ID: 2}},
		Segments: map[uint64]*DataSegment{
			1: {ID: 1, Kind: format.RawLogKind},
		},
	}
	if _, _, err := prepareIndexReplay(image, &selectedIndexFile{}); !errors.Is(err, ErrIndexCheckpointUnusable) {
		t.Fatalf("replay over the fast-path budget = %v", err)
	}
}
