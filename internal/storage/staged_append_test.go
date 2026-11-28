package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
)

func TestStagedAppendSelectsEverySegmentAtOneFrontier(t *testing.T) {
	for _, prior := range []bool{false, true} {
		for _, count := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("prior=%t/count=%d", prior, count), func(t *testing.T) {
				dir := t.TempDir()
				cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 128, TargetSegmentBytes: 192, Handles: 1}
				store, err := openTestStore(t, dir, cfg, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer store.Close()
				healthy := func() error { return nil }
				value := bytes.Repeat([]byte("v"), 70)
				want := make(map[string][]byte)
				if prior {
					if _, err := store.Append(format.Put, []byte("before"), value, healthy); err != nil {
						t.Fatal(err)
					}
					want["before"] = value
				}
				before := store.Current
				stage, err := store.BeginStagedAppend()
				if err != nil {
					t.Fatal(err)
				}
				defer stage.Discard()
				locations := make([]Location, 0, count)
				for i := range count {
					key := fmt.Sprintf("staged-%d", i)
					loc, err := stage.Append(format.Put, []byte(key), value)
					if err != nil {
						t.Fatal(err)
					}
					if store.Current != before || store.Segments[loc.Segment.ID] != nil || loc.Sequence != before.LastUserSequence+uint64(i)+1 {
						t.Fatalf("stage exposed or used a wrong location: %+v", loc)
					}
					locations = append(locations, loc)
					want[key] = value
				}
				adjustment := count
				if !prior {
					adjustment--
				}
				delta, err := stage.Publish(t.Context(), healthy)
				if err != nil || delta != adjustment {
					t.Fatalf("Publish got %d, %v; want %d", delta, err, adjustment)
				}
				if after := store.Current; after.PublicationGeneration != before.PublicationGeneration+1 ||
					after.LastUserSequence != before.LastUserSequence+uint64(count) || after.ActiveID != locations[count-1].Segment.ID {
					t.Fatalf("stage did not publish an exact frontier: %+v -> %+v", before, after)
				}
				if err := stage.Discard(); err != nil {
					t.Fatal(err)
				}
				for i, loc := range locations {
					key := fmt.Sprintf("staged-%d", i)
					got, err := store.Read(loc, key)
					if store.Segments[loc.Segment.ID] != loc.Segment || err != nil || !bytes.Equal(got, value) {
						t.Fatalf("published location %q: %q, %v", key, got, err)
					}
				}
				// With one record per segment the next normal append seals the
				// staged active segment using the digest kept by Publish.
				if _, err := store.Append(format.Put, []byte("after"), value, healthy); err != nil {
					t.Fatal(err)
				}
				want["after"] = value
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, keys, err := OpenIndexed(t.Context(), dir, cfg, false, nil, newMemoryIndex, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				defer keys.Close()
				if keys.Count() != len(want) {
					t.Fatalf("found %d keys after Open, want %d", keys.Count(), len(want))
				}
				for key, value := range want {
					addr, ok, err := keys.Lookup(key)
					if err != nil || !ok {
						t.Fatalf("lookup %q after Open: found %t, %v", key, ok, err)
					}
					got, err := reopened.Read(At(reopened.Segments[addr.FileID], addr), key)
					if err != nil || !bytes.Equal(got, value) {
						t.Fatalf("read %q after Open: %q, %v", key, got, err)
					}
				}
			})
		}
	}
}

func TestStagedAppendDiscardsPartialPrivateWrite(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 128, TargetSegmentBytes: 192, Handles: 1}
	store, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	before := store.Current
	healthy := func() error { return nil }
	var candidate string
	store.IO = &IO{WriteAt: func(f *os.File, b []byte, off int64) (int, error) {
		if f.Name() == candidate {
			n, err := f.WriteAt(b[:1], off)
			return n, errors.Join(err, syscall.ENOSPC)
		}
		return f.WriteAt(b, off)
	}}
	stage, err := store.BeginStagedAppend()
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Discard()
	loc, err := stage.Append(format.Put, []byte("never-published"), nil)
	if err != nil {
		t.Fatal(err)
	}
	candidate = loc.Segment.Path
	delta, err := stage.Publish(t.Context(), healthy)
	var failure *Fault
	if delta != 0 || !errors.Is(err, syscall.ENOSPC) || !errors.As(err, &failure) || failure.Outcome != publication.NotApplied || failure.Unsafe {
		t.Fatalf("failed stage returned %d, %v", delta, err)
	}
	if store.Current != before {
		t.Fatal("private DATA write changed CURRENT")
	}
	if err := stage.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(loc.Segment.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discard left private DATA: %v", err)
	}
	if _, err := store.Append(format.Put, []byte("retry"), nil, healthy); err != nil {
		t.Fatal(err)
	}
	if store.Current.LastUserSequence != before.LastUserSequence+1 {
		t.Fatal("unpublished records advanced the sequence")
	}
}
