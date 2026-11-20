package storage

import (
	"bufio"
	"context"
	"errors"
	"io"
	"maps"
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
)

func TestSortedReplayClosesRemainingRunsAfterOneCloseFails(t *testing.T) {
	dir := t.TempDir()
	closed, err := newReplayRun(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.fd.Close(); err != nil {
		t.Fatal(err)
	}
	other, err := newReplayRun(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := sortedReplay{pending: closed}
	s.levels[0] = other
	if err := s.close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close: %v", err)
	}
	if _, err := other.fd.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("second run: %v", err)
	}
	if err := s.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestSortedReplayKeepsBinaryKeysAndStableHistoryAcrossRuns(t *testing.T) {
	dir := t.TempDir()
	keys := []string{"a", "a\x00", "a\xff", "\x00", "\xff", strings.Repeat("large key", 1024)}
	sorter := sortedReplay{ctx: t.Context(), dir: dir, limit: 2 * replayKeyCharge}
	defer sorter.close()
	latest := make(map[string]index.Address)
	rng := rand.New(rand.NewSource(42))
	for i := range 240 {
		key := keys[rng.Intn(len(keys))]
		r := format.Record{Kind: format.Put, KeyBytes: int64(len(key)), Sequence: uint64(1000 - i), ValueBytes: 13}
		if _, ok := latest[key]; ok && rng.Intn(3) == 0 {
			r.Kind = format.Delete
			r.ValueBytes = 0
		}
		a := index.Address{FileID: 2, Offset: int64(i * 200), Bytes: r.Bytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence}
		if r.Kind == format.Delete {
			delete(latest, key)
		} else {
			latest[key] = a
		}
		if err := sorter.add(replayEvent{key: key, addr: a, fileKind: format.Log, recordKind: r.Kind}); err != nil {
			t.Fatal(err)
		}
		if sorter.used > sorter.limit && len(sorter.chunk) > 1 {
			t.Fatalf("chunk exceeded budget, %d records", len(sorter.chunk))
		}
	}
	if files, err := os.ReadDir(dir); err != nil || len(files) != 0 {
		t.Fatalf("visible work files %v, %v", files, err)
	}
	got := make(map[string]index.Address)
	prev := ""
	if err := sorter.reduce(0, func(key string, a index.Address) error {
		if key <= prev {
			t.Fatalf("keys out of order %q, %q", prev, key)
		}
		prev = key
		got[key] = a
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(latest, got) {
		t.Fatal("reduced addresses differ from chronological replay")
	}
}

func TestBulkReplayIdentifiesInvalidSourceAndAllowsPrunedDeletes(t *testing.T) {
	for _, test := range []struct {
		name        string
		seq, pruned uint64
		kind        uint32
		ops         []uint8
		wantErr     bool
	}{
		{"ordinary absent delete", 5, 4, format.Log, []uint8{format.Delete, format.Put}, true},
		{"pruned absent delete", 4, 4, format.Log, []uint8{format.Delete, format.Put}, false},
		{"repeated snapshot key", 1, 0, format.Snapshot, []uint8{format.Put, format.Put}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, err := btree.NewIn(t.TempDir(), btree.MinBudget)
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			builder := newReplayBuilder(t.Context(), backend, test.pruned)
			defer builder.abort()
			for i, kind := range test.ops {
				r := format.Record{Sequence: test.seq, KeyBytes: 1, Kind: kind}
				if err := builder.apply(&File{ID: 1, Kind: test.kind}, int64(i*100), r, "k"); err != nil {
					t.Fatal(err)
				}
			}
			err = builder.finish()
			if test.wantErr {
				var src *SourceError
				var work *IndexWorkError
				if !errors.Is(err, ErrCorrupt) || !errors.As(err, &src) || errors.As(err, &work) || backend.Count() != 0 {
					t.Fatalf("count=%d, invalid source %v", backend.Count(), err)
				}
			} else if err != nil || backend.Count() != 1 {
				t.Fatalf("count=%d, err=%v", backend.Count(), err)
			}
		})
	}
}

func TestReplayRunCorruptionAndCancellationRemainPrivate(t *testing.T) {
	run, err := newReplayRun(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer run.fd.Close()
	w := bufio.NewWriter(run.fd)
	if err := run.write(w, replayEvent{key: "hello", addr: index.Address{Bytes: format.RecordHeaderBytes + 5}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := run.fd.WriteAt([]byte("x"), replayHeaderBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := run.cursor().next(); err == nil {
		t.Fatal("accepted invalid checksum")
	}
	if err := run.fd.Truncate(replayHeaderBytes + 1); err != nil {
		t.Fatal(err)
	}
	if _, err := run.cursor().next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated run: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	s := sortedReplay{ctx: ctx, dir: t.TempDir(), limit: 1}
	defer s.close()
	if err := s.each(func(replayEvent) error { t.Fatal("called after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}

	backend, err := btree.NewIn(t.TempDir(), btree.MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	builder := newReplayBuilder(t.Context(), backend, 0)
	defer builder.abort()
	builder.sort.dir = t.TempDir() + "/missing"
	builder.sort.limit = 1
	for _, key := range []string{"first", "last"} {
		err = builder.apply(&File{ID: 1, Kind: format.Log}, 64, format.Record{Kind: format.Put, Sequence: 1, KeyBytes: int64(len(key))}, key)
	}
	var work *IndexWorkError
	var source *SourceError
	if !errors.As(err, &work) || errors.As(err, &source) || backend.Count() != 0 {
		t.Fatalf("workspace error: %v", err)
	}
}
