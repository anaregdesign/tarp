package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

// A late invalid Delete must discard all provisional live addresses, including
// a history longer than the generic fallback's transaction chunk. Retrying the
// build starts from the same empty backend.
func TestUnpublishedReplayDropsCorruptHistoryAndCanRetry(t *testing.T) {
	dir := t.TempDir()
	src, err := openTestStore(t, dir, Config{Handles: 1, TargetSegmentBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	var puts []Entry
	for i := range 257 {
		puts = append(puts, Entry{Key: []byte(fmt.Sprint(i)), Value: []byte{42}})
	}
	if _, err = src.AppendBatch(format.Put, puts, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	m := memory.New()
	defer m.Close()
	// A full replay against the image produces exact key, byte and limit totals.
	image, err := readImage(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	err = image.rebuildIndexFromData(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := validateReadOnly(t.Context(), dir, verifyWindowBytes)
	if err != nil || image.LiveRecordSizes != raw.LiveRecordSizes || image.LiveKeys != uint64(len(puts)) || m.Count() != len(puts) {
		t.Fatalf("replayed %d, limits %+v, err=%v", image.LiveKeys, image.LiveRecordSizes, err)
	}
	var live int64
	for _, f := range image.Segments {
		live += f.LiveKeys.Load()
	}
	if live != int64(len(puts)) {
		t.Fatalf("file live keys=%d", live)
	}

	if _, err := src.Append(format.Delete, []byte("missing"), nil, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	failing := memory.New()
	defer failing.Close()
	for range 2 {
		image, err := readImage(t.Context(), dir)
		if err != nil {
			t.Fatal(err)
		}
		err = image.rebuildIndexFromData(t.Context(), failing)
		var source *SourceError
		if !errors.As(err, &source) || !errors.Is(err, ErrCorrupt) || failing.Count() != 0 {
			t.Fatalf("failed replay: count=%d, err=%v", failing.Count(), err)
		}
	}
}

// A suffix can remove the record that determined the required limits and can
// refer to an earlier key in a different physical file. Neither state is
// recoverable by treating the suffix as an independent initial bulk build.
func TestIndexCheckpointSuffixMatchesAuthoritativeReplay(t *testing.T) {
	dir, scratch := t.TempDir(), t.TempDir()
	cfg := Config{Handles: 1, ValueCacheBudgetBytes: 2048, MaxRecordBytes: 1024, TargetSegmentBytes: 4096}
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	healthy := func() error { return nil }
	old := []Entry{{Key: []byte("largest"), Value: bytes.Repeat([]byte("x"), 700)}, {Key: []byte("survives"), Value: []byte("yes")}, {Key: []byte("gone")}}
	if _, err := s.AppendBatch(format.Put, old, healthy); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(format.Delete, []byte("gone"), nil, healthy); err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	build, err := s.BeginIndexCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	tree, err := btree.NewIn(scratch, btree.MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	if err := build.Write(t.Context(), tree, btree.ExportPacked); err != nil {
		t.Fatal(err)
	}
	if err := build.PublishFile(); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(tree.Close(), build.Install(), build.Release()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(format.Delete, []byte("largest"), nil, healthy); err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendBatch(format.Put, []Entry{{Key: []byte("new"), Value: []byte("open")}, {Key: []byte("new"), Value: []byte("v")}}, healthy); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(dir, IndexName(s.Current.UUID, s.Catalog.Index.ID))
	coveredPath := s.Segments[s.Catalog.Entries[0].ID].Path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	want, err := readImage(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	oracle := memory.New()
	defer oracle.Close()
	if err := want.rebuildIndexFromData(t.Context(), oracle); err != nil {
		t.Fatal(err)
	}
	cfg.MaxRecordBytes, cfg.ValueCacheBudgetBytes = want.LargestLiveRecordBytes, want.LargestLiveCacheChargeBytes
	fresh := func(*Store) (index.Backend, error) { return memory.New(), nil }
	load := func(ctx context.Context, _ *Store, r io.Reader, n int64, visit func(string, index.Address) error) (index.Backend, error) {
		return btree.ImportPacked(ctx, r, n, scratch, btree.MinBudget, visit)
	}
	for _, indexed := range []bool{false, true} {
		cfg.IndexedOpen = indexed
		reads := make(map[string]int64)
		probe := &IO{OpenRead: func(kind string, n int) { reads[kind] += int64(n) }}
		db, keys, err := OpenIndexed(t.Context(), dir, cfg, true, probe, fresh, load)
		if err != nil {
			t.Fatal(err)
		}
		coveredRecordBytes := db.Catalog.Entries[0].Bytes - format.SegmentHeaderBytes
		coveredSegmentHeaderBytes := int64(2 * format.SegmentHeaderBytes)
		if indexed {
			coveredRecordBytes = 0
			coveredSegmentHeaderBytes = format.SegmentHeaderBytes
		}
		logSuffixRecordBytes := db.Catalog.Entries[1].Bytes + db.Current.ActiveCommittedEndOffset - 2*format.SegmentHeaderBytes
		if reads["covered_encoded_records"] != coveredRecordBytes || reads["log_suffix_encoded_records"] != logSuffixRecordBytes ||
			reads["covered_segment_header"] != coveredSegmentHeaderBytes || reads["log_suffix_segment_header"] < 2*format.SegmentHeaderBytes ||
			reads["replay_segment_header"] != 0 || reads["replay_encoded_records"] != 0 || reads["index_checkpoint_file"] != db.Catalog.Index.Bytes {
			t.Fatalf("indexed=%t: Open read byte counts %v, want %d covered record bytes, %d log suffix record bytes and %d INDEX bytes",
				indexed, reads, coveredRecordBytes, logSuffixRecordBytes, db.Catalog.Index.Bytes)
		}
		if !db.IndexCheckpointValidated || keys.Count() != oracle.Count() || db.LargestLiveValueBytes != want.LargestLiveValueBytes {
			t.Fatalf("indexed=%t: hit=%t, live=%d, max value=%d", indexed, db.IndexCheckpointValidated, keys.Count(), db.LargestLiveValueBytes)
		}
		for _, key := range []string{"largest", "survives", "gone", "new"} {
			got, found, err := keys.Lookup(key)
			addr, live, oracleErr := oracle.Lookup(key)
			if err != nil || oracleErr != nil || found != live || got != addr {
				t.Fatalf("%s: checkpoint (%+v, %t, %v), oracle (%+v, %t, %v)", key, got, found, err, addr, live, oracleErr)
			}
		}
		for id, f := range db.Segments {
			expect := want.Segments[id]
			if f.LiveBytes.Load() != expect.LiveBytes.Load() || f.LiveKeys.Load() != expect.LiveKeys.Load() || f.DeleteBytes.Load() != expect.DeleteBytes.Load() {
				t.Fatalf("file %d: checkpoint live keys/bytes/deletes %d/%d/%d, full replay %d/%d/%d", id, f.LiveKeys.Load(), f.LiveBytes.Load(), f.DeleteBytes.Load(), expect.LiveKeys.Load(), expect.LiveBytes.Load(), expect.DeleteBytes.Load())
			}
		}
		if err := errors.Join(keys.Close(), db.Close()); err != nil {
			t.Fatal(err)
		}
	}

	// Replace an obsolete record with a different, correctly CRC-encoded value
	// of the same size and sequence. Record-local CRCs and file identity pass.
	first := format.EncodeRecord(format.Put, 1, old[0].Key, bytes.Repeat([]byte("y"), 700))
	fd, err := os.OpenFile(coveredPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := fd.WriteAt(first, format.SegmentHeaderBytes)
	if err := errors.Join(writeErr, fd.Close()); err != nil {
		t.Fatal(err)
	}
	cfg.IndexedOpen = false
	if got, keys, err := OpenIndexed(t.Context(), dir, cfg, true, nil, fresh, load); got != nil || keys != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Full open with altered covered DATA: db=%v keys=%v err=%v", got, keys, err)
	}
	cfg.IndexedOpen = true
	db, keys, err := OpenIndexed(t.Context(), dir, cfg, true, nil, fresh, load)
	if err != nil {
		t.Fatal(err)
	}
	if !db.IndexCheckpointValidated {
		t.Fatal("Indexed open did not reuse the checkpoint")
	}
	// A subsequent checkpoint must validate the previously attested DATA
	// before it can replace the layout selected by this indexed Open.
	if err := db.Rotate(); err != nil {
		t.Fatal(err)
	}
	before := db.Current
	refresh, err := db.BeginIndexCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	work, err := btree.NewIn(scratch, btree.MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	refreshErr := refresh.Write(t.Context(), work, btree.ExportPacked)
	var source *SourceError
	if !errors.As(refreshErr, &source) || !errors.Is(refreshErr, ErrCorrupt) || db.Current != before {
		t.Fatalf("refresh of substituted DATA changed selection or lost source error: %v", refreshErr)
	}
	if err := errors.Join(work.Close(), refresh.Release()); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(keys.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	if got, keys, err := OpenIndexed(t.Context(), dir, cfg, true, nil, fresh, load); got != nil || keys != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Indexed fallback with altered covered DATA: db=%v keys=%v err=%v", got, keys, err)
	}
}

func TestOpenRejectsIndexFromDivergentCopyAndRetriesFromDATA(t *testing.T) {
	cfg := Config{Handles: 1, ValueCacheBudgetBytes: 1024, MaxRecordBytes: 200, TargetSegmentBytes: 1024, IndexedOpen: true}
	dir, clone, scratch := t.TempDir(), t.TempDir(), t.TempDir()
	db, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(clone, os.DirFS(dir)); err != nil {
		t.Fatal(err)
	}
	makeCheckpoint := func(dir, key string) (format.Frontier, string) {
		t.Helper()
		db, err := openTestStore(t, dir, cfg, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Append(format.Put, []byte(key), []byte("v"), func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := db.Rotate(); err != nil {
			t.Fatal(err)
		}
		build, err := db.BeginIndexCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		tree, err := btree.NewIn(scratch, btree.MinBudget)
		if err != nil {
			t.Fatal(err)
		}
		if err := build.Write(t.Context(), tree, btree.ExportPacked); err != nil {
			t.Fatal(err)
		}
		if err := build.PublishFile(); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(tree.Close(), build.Install(), build.Release(), db.Close()); err != nil {
			t.Fatal(err)
		}
		return db.Current, filepath.Join(dir, IndexName(db.Current.UUID, db.Catalog.Index.ID))
	}
	a, target := makeCheckpoint(dir, "ours")
	b, other := makeCheckpoint(clone, "peer")
	if a.UUID != b.UUID || a.LastUserSequence != b.LastUserSequence || a.PublicationGeneration != b.PublicationGeneration || a.ActiveID != b.ActiveID ||
		a.ActiveCommittedEndOffset != b.ActiveCommittedEndOffset || a.ManifestID != b.ManifestID || filepath.Base(target) != filepath.Base(other) || a.ManifestDigest == b.ManifestDigest {
		t.Fatal("copies should differ only in authenticated history at the same physical cut")
	}
	own, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := os.ReadFile(other)
	if err != nil {
		t.Fatal(err)
	}
	fresh := func(*Store) (index.Backend, error) { return memory.New(), nil }
	load := func(ctx context.Context, _ *Store, r io.Reader, n int64, visit func(string, index.Address) error) (index.Backend, error) {
		return btree.ImportPacked(ctx, r, n, scratch, btree.MinBudget, visit)
	}
	checkMiss := func(importer IndexImporter) {
		t.Helper()
		db, keys, err := OpenIndexed(t.Context(), dir, cfg, true, nil, fresh, importer)
		if err != nil {
			t.Fatal(err)
		}
		got, found, err := keys.Lookup("ours")
		if db.IndexCheckpointValidated || !found || err != nil || keys.Count() != 1 || db.Segments[got.FileID].LiveKeys.Load() != 1 {
			t.Fatalf("retry recovered checkpoint=%t, key=%+v, live=%d, err=%v", db.IndexCheckpointValidated, got, keys.Count(), err)
		}
		if err := errors.Join(keys.Close(), db.Close()); err != nil {
			t.Fatal(err)
		}
	}

	if err := os.WriteFile(target, foreign, 0600); err != nil {
		t.Fatal(err)
	}
	checkMiss(load)
	if err := os.WriteFile(target, own, 0600); err != nil {
		t.Fatal(err)
	}
	// A page with the correct key but an address outside its covered files is
	// unusable. Partial counts from the visitor must be discarded on retry.
	checkMiss(func(ctx context.Context, s *Store, r io.Reader, n int64, visit func(string, index.Address) error) (index.Backend, error) {
		return load(ctx, s, r, n, func(key string, addr index.Address) error {
			if err := visit(key, addr); err != nil {
				return err
			}
			addr.FileID = s.Current.ActiveID
			return visit(key, addr)
		})
	})
	for _, operational := range []error{syscall.EIO, context.Canceled, os.ErrClosed} {
		// A short read and failed cleanup can arrive together. Preserve the
		// operational cause even if the failed import also qualifies as a miss.
		checkIO := func(context.Context, *Store, io.Reader, int64, func(string, index.Address) error) (index.Backend, error) {
			return nil, fmt.Errorf("read/close: %w", errors.Join(ErrIndexCheckpointUnusable, io.ErrUnexpectedEOF, operational))
		}
		if db, keys, err := OpenIndexed(t.Context(), dir, cfg, true, nil, fresh, checkIO); db != nil || keys != nil || !errors.Is(err, operational) {
			t.Fatalf("joined index and operational failures should preserve %v: %v", operational, err)
		}
	}
	image, err := readImage(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{image.Current.ActiveID, image.Catalog.Entries[0].ID} {
		path := image.Segments[id].Path
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		var called bool
		failingImporter := func(context.Context, *Store, io.Reader, int64, func(string, index.Address) error) (index.Backend, error) {
			called = true
			return nil, syscall.ENOSPC
		}
		db, keys, err := OpenIndexed(t.Context(), dir, cfg, true, nil, fresh, failingImporter)
		if called || db != nil || keys != nil || !errors.Is(err, ErrCorrupt) || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("missing selected DATA %d, importer ran %t, err %v", id, called, err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}

}
