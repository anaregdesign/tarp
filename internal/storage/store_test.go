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
	"github.com/anaregdesign/tarp/internal/publication"
)

func TestReadMissingSegmentPreservesPathErrorAndRecordOffset(t *testing.T) {
	filename := DataName([16]byte{}, 1)
	path := filepath.Join(t.TempDir(), filename)
	s := &Store{readers: newHandles(1)}
	loc := Location{File: &File{Path: path}, Offset: format.SegmentHeaderBytes}
	_, err := s.Read(loc, "k")
	var pathErr *os.PathError
	want := fmt.Sprintf("%s: record at offset %d:", filename, loc.Offset)
	if !errors.Is(err, os.ErrNotExist) || !errors.As(err, &pathErr) || pathErr.Path != path || !strings.Contains(err.Error(), want) {
		t.Fatalf("expected missing segment and location %q, got %v", want, err)
	}
}

func TestOpenPreservesUnrecognizedTemporaryFilesBeforeCleanup(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Handles: 1}
	store, _, err := Open(dir, cfg, nil)
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
	for _, name := range []string{prefix + ".tmp", prefix + "123", fmt.Sprintf(".tarp-current-%x-123.tmp", otherUUID)} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("preserve me"), 0600); err != nil {
			t.Fatal(err)
		}
		got, _, err := Open(dir, cfg, nil)
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
	reopened, _, err := Open(dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Stat(owned.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recognized temporary file after valid Open: %v", err)
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
				cfg := Config{CacheBytes: 1 << 20, RecordBytes: 128 << 10, TargetSegmentBytes: 1 << 20, Handles: 1}
				if rotate {
					cfg.TargetSegmentBytes = format.SegmentHeaderBytes + batchBytes
				}
				s, _, err := Open(dir, cfg, nil)
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
					start := before.ActiveEnd
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
				reopened, index, err := Open(dir, cfg, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				after := reopened.Current
				if tc.committed {
					if after.Sequence != before.Sequence+2 || after.Generation != before.Generation+1 || (after.ActiveID != before.ActiveID) != rotate {
						t.Fatalf("frontier after batch: %+v -> %+v", before, after)
					}
					for _, e := range entries {
						loc, ok := index[string(e.Key)]
						if !ok {
							t.Fatalf("missing %q", e.Key)
						}
						v, err := reopened.Read(loc, string(e.Key))
						if err != nil || !bytes.Equal(v, e.Value) {
							t.Fatalf("reopened %q: %q, %v", e.Key, v, err)
						}
					}
				} else if after != before || len(index) != 1 {
					t.Fatalf("unpublished batch appeared: %+v -> %+v, keys %v", before, after, index)
				}
			})
		}
	}
}

func TestAppendBatchRejectsExhaustedSequenceBeforeIO(t *testing.T) {
	s := &Store{Current: format.Frontier{Sequence: math.MaxUint64 - 1}}
	_, err := s.AppendBatch(format.Put, []Entry{{Key: []byte("a")}, {Key: []byte("b")}}, nil)
	var f *Fault
	if !errors.As(err, &f) || f.Unsafe || f.Outcome != publication.NotApplied {
		t.Fatalf("sequence overflow returned %v", err)
	}
}

func TestRejectedRotatingPreparationKeepsIdentityForRetry(t *testing.T) {
	cfg := Config{CacheBytes: 4096, RecordBytes: 256, TargetSegmentBytes: format.SegmentHeaderBytes + 256, Handles: 1}
	s, _, err := Open(t.TempDir(), cfg, nil)
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
	if err != nil || candidate == before.ActiveID || loc.File.ID != candidate || loc.Sequence != before.Sequence+1 {
		t.Fatalf("retry used location %+v, wanted file %d, error %v", loc, candidate, err)
	}
}
