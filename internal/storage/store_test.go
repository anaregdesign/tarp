package storage

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
	"github.com/anaregdesign/tarp/internal/publication"
)

func newMemoryIndex(*Store) (index.Backend, error) { return memory.New(), nil }

// Most storage tests use the recovered store without its key index.
func openTestStore(t *testing.T, dir string, cfg Config, fs *IO) (*Store, error) {
	t.Helper()
	store, keys, err := OpenIndexed(t.Context(), dir, cfg, false, fs, newMemoryIndex, nil)
	if err != nil {
		return store, err
	}
	if err := keys.Close(); err != nil {
		t.Fatal(err)
	}
	return store, nil
}

func TestReadMissingSegmentPreservesPathErrorAndRecordOffset(t *testing.T) {
	filename := DataName([16]byte{}, 1)
	path := filepath.Join(t.TempDir(), filename)
	s := &Store{readers: newHandles(1)}
	loc := Location{File: &DataSegment{Path: path}, Offset: format.SegmentHeaderBytes}
	_, err := s.Read(loc, "k")
	var pathErr *os.PathError
	want := fmt.Sprintf("%s: record at offset %d:", filename, loc.Offset)
	if !errors.Is(err, os.ErrNotExist) || !errors.As(err, &pathErr) || pathErr.Path != path || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected missing segment and location %q, got %v", want, err)
	}
}

func TestOpenPreservesUnrecognizedFilesBeforeCleanup(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Handles: 1}
	store, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	uuid := store.Current.UUID
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf(".tarp-current-%x-", uuid)
	owned, err := os.CreateTemp(dir, prefix+"*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	otherUUID := uuid
	otherUUID[0] ^= 1
	for _, name := range []string{
		prefix + ".tmp", prefix + "123", fmt.Sprintf(".tarp-current-%x-123.tmp", otherUUID),
		DataName(otherUUID, 99),
		fmt.Sprintf("INDEX-%x-000000000000000A", uuid),
		fmt.Sprintf("PARITY-%x-000000000000000", uuid),
		ManifestName(uuid, 0),
	} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("preserve me"), 0600); err != nil {
			t.Fatal(err)
		}
		got, err := openTestStore(t, dir, cfg, nil)
		if got != nil {
			got.Close()
		}
		if got != nil || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open with unrecognized name %q returned %v", name, err)
		}
		if b, err := os.ReadFile(path); err != nil || string(b) != "preserve me" {
			t.Fatalf("file %q after failed Open: %q, %v", name, b, err)
		}
		if _, err := os.Stat(owned.Name()); err != nil {
			t.Fatalf("cleanup ran before validation for %q: %v", name, err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	reopened, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Stat(owned.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recognized temporary file after valid Open: %v", err)
	}
}

func TestOpenPreservesWorkspaceAndTailUntilValidationSucceeds(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 256, TargetSegmentBytes: 1024, Handles: 1}
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(format.Put, []byte("k"), []byte("saved"), func() error { return nil }); err != nil {
		s.Close()
		t.Fatal(err)
	}
	data := s.Files[s.Current.ActiveID].Path
	end := s.Current.ActiveCommittedEndOffset
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(data, end+1); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(dir, workspaceName)
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	owned := filepath.Join(workspace, "tarp-index-123")
	if err := os.WriteFile(owned, []byte("scratch"), 0600); err != nil {
		t.Fatal(err)
	}
	newBTree := func(s *Store) (index.Backend, error) {
		dir, err := s.WorkspaceDir()
		if err != nil {
			return nil, err
		}
		return btree.NewIn(dir, btree.MinBudget)
	}
	checkUnrecovered := func() {
		t.Helper()
		if got, err := os.ReadFile(owned); err != nil || string(got) != "scratch" {
			t.Fatalf("scratch after failed Open: %q, %v", got, err)
		}
		if st, err := os.Stat(data); err != nil || st.Size() != end+1 {
			t.Fatalf("DATA tail after failed Open: %v, %v", st, err)
		}
	}

	tooSmall := cfg
	tooSmall.MaxRecordBytes = 1
	rejected, keys, err := OpenIndexed(t.Context(), dir, tooSmall, true, nil, newBTree, nil)
	var limits *CompatibilityError
	if rejected != nil || keys != nil || !errors.As(err, &limits) {
		t.Fatalf("Open with incompatible limits: %v, %v, %v", rejected, keys, err)
	}
	checkUnrecovered()

	unknown := filepath.Join(workspace, "unknown")
	if err := os.WriteFile(unknown, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	rejected, keys, err = OpenIndexed(t.Context(), dir, cfg, true, nil, newBTree, nil)
	if rejected != nil || keys != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with unknown workspace file: %v, %v, %v", rejected, keys, err)
	}
	checkUnrecovered()
	if got, err := os.ReadFile(unknown); err != nil || string(got) != "keep" {
		t.Fatalf("unknown file after failed Open: %q, %v", got, err)
	}

	if err := os.Remove(unknown); err != nil {
		t.Fatal(err)
	}
	reopened, live, err := OpenIndexed(t.Context(), dir, cfg, false, nil, newMemoryIndex, nil)
	if err != nil {
		t.Fatalf("retry Open: %v", err)
	}
	defer reopened.Close()
	defer live.Close()
	addr, ok, err := live.Lookup("k")
	if err != nil || !ok {
		t.Fatalf("committed key after retry: found=%t, err=%v", ok, err)
	}
	if got, err := reopened.Read(At(reopened.Files[addr.FileID], addr), "k"); err != nil || string(got) != "saved" {
		t.Fatalf("retry read: %q, %v", got, err)
	}
	if _, err := os.Lstat(owned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch after retry: %v", err)
	}
	if st, err := os.Stat(data); err != nil || st.Size() != end {
		t.Fatalf("DATA tail after retry: %v, %v", st, err)
	}
}

func TestOpenReservesFileIDsBeforeRemovingUnselectedFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 128, TargetSegmentBytes: 1024, Handles: 1}
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	frontier := s.Current
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(dir, DataName(frontier.UUID, 100))
	manifest := filepath.Join(dir, ManifestName(frontier.UUID, 101))
	if err := os.WriteFile(data, []byte("orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("CURRENT", manifest); err != nil {
		t.Fatal(err)
	}
	// The unselected DATA sorts before the invalid MANIFEST. Neither is
	// eligible for cleanup until every entry has passed type and name checks.
	rejected, err := openTestStore(t, dir, cfg, nil)
	if rejected != nil {
		rejected.Close()
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with symlink returned %v", err)
	}
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(data); err != nil || string(b) != "orphan" {
		t.Fatalf("orphan after rejected Open: %q, %v", b, err)
	}
	unselected := []string{data, manifest, filepath.Join(dir, IndexName(frontier.UUID, 102)), filepath.Join(dir, parityName(frontier.UUID, 103))}
	for _, path := range unselected[1:] {
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err = openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, path := range unselected {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unselected file %s after Open: %v", path, err)
		}
	}
	if _, err := s.Append(format.Put, []byte("k"), nil, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	if s.Current.ActiveID <= 103 {
		t.Fatalf("rotation reused a previously observed ID: %d", s.Current.ActiveID)
	}
}

func TestAppendBatchRecoversOnlyCompleteFrontiers(t *testing.T) {
	// The first value exceeds the encoding buffer. The second record shares
	// the same commit, even when a write fails after the whole first record.
	entries := []Entry{{[]byte("a"), bytes.Repeat([]byte("1"), 96<<10)}, {[]byte("b"), []byte("2")}}
	firstBytes := int64(format.RecordHeaderBytes + len(entries[0].Key) + len(entries[0].Value))
	batchBytes := firstBytes + int64(format.RecordHeaderBytes+len(entries[1].Key)+len(entries[1].Value))
	for _, rotate := range []bool{false, true} {
		for _, tc := range []struct {
			point     string
			outcome   publication.Outcome
			committed bool
		}{
			{"partial-data", publication.NotApplied, false},
			{"current.sync:after", publication.NotApplied, false},
			{"current.rename:before", publication.Unknown, false},
			{"current.rename:after", publication.Unknown, true},
			{"current.dirsync:after", publication.Unknown, true},
			{"", "", true},
		} {
			t.Run(fmt.Sprintf("rotate=%v/%s", rotate, tc.point), func(t *testing.T) {
				dir := t.TempDir()
				cfg := Config{ValueCacheBudgetBytes: 1 << 20, MaxRecordBytes: 128 << 10, TargetSegmentBytes: 1 << 20, Handles: 1}
				if rotate {
					cfg.TargetSegmentBytes = format.SegmentHeaderBytes + batchBytes
				}
				s, err := openTestStore(t, dir, cfg, nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				healthy := func() error { return nil }
				if _, err := s.Append(format.Put, []byte("seed"), nil, healthy); err != nil {
					t.Fatal(err)
				}
				before := s.Current
				injected := errors.New("injected batch failure")
				publications := 0
				s.IO = &IO{Hook: func(point string) error {
					if point == tc.point {
						return injected
					}
					if point == "current.rename:after" {
						publications++
					}
					return nil
				}}
				if tc.point == "partial-data" {
					start := before.ActiveCommittedEndOffset
					if rotate {
						start = format.SegmentHeaderBytes
					}
					cut := start + firstBytes + 1
					s.IO.WriteAt = func(f *os.File, b []byte, off int64) (int, error) {
						if off < cut && cut-off <= int64(len(b)) {
							n, err := f.WriteAt(b[:cut-off], off)
							return n, errors.Join(err, injected)
						}
						return f.WriteAt(b, off)
					}
				}
				locations, err := s.AppendBatch(format.Put, entries, healthy)
				if tc.point == "" {
					if err != nil || publications != 1 || len(locations) != 2 {
						t.Fatalf("append returned %v, %v with %d publications", locations, err, publications)
					}
					for i, loc := range locations {
						v, err := s.Read(loc, string(entries[i].Key))
						if err != nil || !bytes.Equal(v, entries[i].Value) {
							t.Fatalf("read returned %q, %v", v, err)
						}
					}
				} else {
					var fault *Fault
					if len(locations) != 0 || !errors.Is(err, injected) || !errors.As(err, &fault) || fault.Outcome != tc.outcome {
						t.Fatalf("append returned %v, %v, want %s", locations, err, tc.outcome)
					}
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, keys, err := OpenIndexed(t.Context(), dir, cfg, false, nil, newMemoryIndex, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				defer keys.Close()
				after := reopened.Current
				if tc.committed {
					if after.LastUserSequence != before.LastUserSequence+2 || after.PublicationGeneration != before.PublicationGeneration+1 || (after.ActiveID != before.ActiveID) != rotate {
						t.Fatalf("frontier after batch: %+v -> %+v", before, after)
					}
					for _, e := range entries {
						addr, ok, err := keys.Lookup(string(e.Key))
						if err != nil || !ok {
							t.Fatalf("lookup %q after reopen: found=%t, err=%v", e.Key, ok, err)
						}
						v, err := reopened.Read(At(reopened.Files[addr.FileID], addr), string(e.Key))
						if err != nil || !bytes.Equal(v, e.Value) {
							t.Fatalf("reopened %q: %q, %v", e.Key, v, err)
						}
					}
				} else if after != before || keys.Count() != 1 {
					t.Fatalf("unpublished batch appeared: %+v -> %+v, keys %d", before, after, keys.Count())
				}
			})
		}
	}
}

func TestAppendBatchRejectsExhaustedSequenceBeforeIO(t *testing.T) {
	s := &Store{Current: format.Frontier{LastUserSequence: math.MaxUint64 - 1}}
	_, err := s.AppendBatch(format.Put, []Entry{{Key: []byte("a")}, {Key: []byte("b")}}, nil)
	var f *Fault
	if !errors.As(err, &f) || f.Unsafe || f.Outcome != publication.NotApplied {
		t.Fatalf("sequence overflow returned %v", err)
	}
}

func TestRejectedRotatingPreparationKeepsIdentityForRetry(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 4096, MaxRecordBytes: 256, TargetSegmentBytes: format.SegmentHeaderBytes + 256, Handles: 1}
	s, err := openTestStore(t, t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	healthy := func() error { return nil }
	value := bytes.Repeat([]byte{'v'}, 175)
	if _, err := s.Append(format.Put, []byte("before"), value, healthy); err != nil {
		t.Fatal(err)
	}
	before, lastID, files := s.Current, s.nextID, len(s.Files)
	rejected := errors.New("reject staged index")
	var candidate uint64
	_, err = s.AppendPrepared(format.Put, []byte("after"), value, healthy, func(loc Location) error {
		candidate = loc.File.ID
		return rejected
	})
	if !errors.Is(err, rejected) || s.Current != before || s.nextID != lastID || len(s.Files) != files {
		t.Fatalf("rejected rotation: error %v, frontier %+v, last ID %d (was %d), files %d (was %d)", err, s.Current, s.nextID, lastID, len(s.Files), files)
	}
	loc, err := s.Append(format.Put, []byte("after"), value, healthy)
	if err != nil || candidate == before.ActiveID || loc.File.ID != candidate || loc.Sequence != before.LastUserSequence+1 {
		t.Fatalf("retry used location %+v, wanted file %d, error %v", loc, candidate, err)
	}
}

func TestOpenRetiresOnlyParityBelongingToThisDatabase(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Handles: 1}
	store, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	uuid, activeID := store.Current.UUID, store.Current.ActiveID
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	foreignUUID := uuid
	foreignUUID[0] ^= 0xff
	foreign := filepath.Join(dir, parityName(foreignUUID, activeID))
	stray := filepath.Join(dir, parityName(uuid, activeID))
	for _, path := range []string{foreign, stray} {
		if err := os.WriteFile(path, []byte("unchanged"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if s, err := openTestStore(t, dir, cfg, nil); !errors.Is(err, ErrCorrupt) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("foreign object: %v", err)
	}
	for _, path := range []string{foreign, stray} {
		if b, err := os.ReadFile(path); err != nil || string(b) != "unchanged" {
			t.Fatalf("validation modified %s: %v", path, err)
		}
	}
	if err := os.Remove(foreign); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Lstat(stray); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("parity for active segment was not retired: %v", err)
	}
}
