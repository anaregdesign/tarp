package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	src, _, err := Open(dir, Config{Handles: 1, TargetSegmentBytes: 1 << 20}, nil)
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
	err = image.replayIndexed(t.Context(), m)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := Validate(t.Context(), dir)
	if err != nil || image.Required != raw.Required || image.LiveKeys != uint64(len(puts)) || m.Count() != len(puts) {
		t.Fatalf("replayed %d, limits %+v, err=%v", image.LiveKeys, image.Required, err)
	}
	var live int64
	for _, f := range image.Files {
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
		err = image.replayIndexed(t.Context(), failing)
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
	cfg := Config{Handles: 1, ValueCacheBudgetBytes: 2048, RecordBytes: 1024, TargetSegmentBytes: 4096}
	s, _, err := Open(dir, cfg, nil)
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
	build, err := s.BeginIndex()
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
	coveredPath := s.Files[s.Catalog.Files[0].ID].Path
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	want, err := readImage(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	oracle := memory.New()
	defer oracle.Close()
	if err := want.replayIndexed(t.Context(), oracle); err != nil {
		t.Fatal(err)
	}
	cfg.RecordBytes, cfg.ValueCacheBudgetBytes = want.Required.RecordBytes, want.Required.ValueCacheBudgetBytes
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
		coveredPayload := db.Catalog.Files[0].Bytes - format.SegmentHeaderBytes
		if indexed {
			coveredPayload = 0
		}
		tailPayload := db.Catalog.Files[1].Bytes + db.Current.ActiveEnd - 2*format.SegmentHeaderBytes
		if reads["covered-payload"] != coveredPayload || reads["tail-payload"] != tailPayload ||
			reads["covered-header"] < format.SegmentHeaderBytes || reads["tail-header"] < 2*format.SegmentHeaderBytes ||
			reads["replay-header"] != 0 || reads["replay-payload"] != 0 {
			t.Fatalf("indexed=%t: Open DATA reads %v, want covered %d and tail %d payload bytes", indexed, reads, coveredPayload, tailPayload)
		}
		if !db.IndexCheckpointHit || keys.Count() != oracle.Count() || db.MaxValueBytes != want.MaxValueBytes {
			t.Fatalf("indexed=%t: hit=%t, live=%d, max value=%d", indexed, db.IndexCheckpointHit, keys.Count(), db.MaxValueBytes)
		}
		for _, key := range []string{"largest", "survives", "gone", "new"} {
			got, found, err := keys.Lookup(key)
			addr, live, oracleErr := oracle.Lookup(key)
			if err != nil || oracleErr != nil || found != live || got != addr {
				t.Fatalf("%s: checkpoint (%+v, %t, %v), oracle (%+v, %t, %v)", key, got, found, err, addr, live, oracleErr)
			}
		}
		for id, f := range db.Files {
			expect := want.Files[id]
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
	if !db.IndexCheckpointHit {
		t.Fatal("Indexed open did not reuse the checkpoint")
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
