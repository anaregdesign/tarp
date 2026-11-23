package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestParityBuildCoalescesWritesAndPreservesUnsafeDestination(t *testing.T) {
	for _, valueBytes := range []int{5, 32 * format.DataBlockBytes * format.DataBlocksPerStripe} {
		name := "tiny"
		if valueBytes > 5 {
			name = "multiple stripes"
		}
		t.Run(name, func(t *testing.T) {
			var writes int
			io := &IO{Wrote: func(point string, n int) {
				if point == "parity.write" {
					writes++
				}
			}}
			cfg := Config{ValueCacheBudgetBytes: 4 << 20, MaxRecordBytes: 3 << 20, TargetSegmentBytes: 4 << 20, Handles: 1}
			s, err := openTestStore(t, t.TempDir(), cfg, io)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			value := bytes.Repeat([]byte("v"), valueBytes)
			_, err = s.Append(format.Put, []byte("k"), value, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Rotate(); err != nil {
				t.Fatal(err)
			}
			entry := s.Catalog.Entries[0]
			source := CapturedSegment{Segment: s.Segments[entry.ID], Entry: entry}
			source.Segment.Pin()
			defer source.Segment.Unpin()
			ensure := func() error { return s.EnsureParitySidecar(t.Context(), s.Current.UUID, source, true, nil) }
			if err := ensure(); err != nil {
				t.Fatal(err)
			}
			// At 32 KiB per write, 32 full DATA stripes take five sidecar writes.
			if name == "tiny" && writes != 1 || name != "tiny" && (writes < 2 || writes > 5) {
				t.Fatalf("wrote parity for %d DATA value bytes in %d writes", valueBytes, writes)
			}
			writes = 0
			if err := ensure(); err != nil || writes != 0 {
				t.Fatalf("healthy sidecar revalidation rewrote %d times: %v", writes, err)
			}
			path := filepath.Join(s.Dir, parityName(s.Current.UUID, entry.ID))
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			unrelated := filepath.Join(t.TempDir(), "unrelated")
			if err := os.WriteFile(unrelated, []byte("unrelated"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(unrelated, path); err != nil {
				t.Fatal(err)
			}
			var sourceErr *SourceError
			if err := ensure(); !errors.Is(err, ErrCorrupt) || errors.As(err, &sourceErr) || source.Segment.ParityReady() {
				t.Fatalf("unsafe sidecar misclassified: %v, ready %t", err, source.Segment.ParityReady())
			}
			if target, err := os.Readlink(path); err != nil || target != unrelated {
				t.Fatalf("changed symlink: %q, %v", target, err)
			}
			if got, err := os.ReadFile(unrelated); err != nil || string(got) != "unrelated" {
				t.Fatalf("changed symlink destination: %q, %v", got, err)
			}
		})
	}
}
