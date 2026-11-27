package storage

import (
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func snapshotForBoundaries(t *testing.T, files ...[]string) *DataCheckpoint {
	t.Helper()
	s := &Store{Dir: t.TempDir()}
	cap := &Capture{}
	cap.Current.UUID[0] = 1
	var sequence uint64
	for i, keys := range files {
		id := uint64(i + 1)
		path := filepath.Join(s.Dir, DataName(cap.Current.UUID, id))
		wire := format.EncodeSegmentHeader(format.SegmentHeader{UUID: cap.Current.UUID, ID: id, Kind: format.SnapshotKind})
		for _, key := range keys {
			sequence++
			wire = append(wire, format.EncodeRecord(format.Put, sequence, []byte(key), []byte("value"))...)
		}
		if err := os.WriteFile(path, wire, 0600); err != nil {
			t.Fatal(err)
		}
		e := format.CatalogEntry{ID: id, Kind: format.SnapshotKind, Bytes: int64(len(wire)), DataFileDigest: sha256.Sum256(wire)}
		cap.Catalog.Entries = append(cap.Catalog.Entries, e)
		cap.Segments = append(cap.Segments, CapturedSegment{Segment: &DataSegment{ID: id, Kind: format.SnapshotKind, Path: path}, Entry: e})
	}
	cap.Catalog.CheckpointCutoffSequence = sequence
	cap.Current.LastUserSequence = sequence
	return s.NewDataCheckpoint(cap)
}

func TestOrderedBaseRejectsOverlappingSnapshotFiles(t *testing.T) {
	c := snapshotForBoundaries(t, []string{"a", "c"}, []string{"b", "d"})
	if ordered, err := c.certifyCapturedBase(t.Context(), nil); err != nil || ordered || c.HasOrderedBase() {
		t.Fatalf("overlapping snapshot base: ordered=%t err=%v", ordered, err)
	}
}

func TestOrderedBaseProbesAuthenticatedBoundariesOutsideCacheBudget(t *testing.T) {
	longKey := strings.Repeat("a", snapshotFenceBudget+1)
	c := snapshotForBoundaries(t, []string{longKey, "z"}, []string{"zz"})
	if ok, err := c.certifyCapturedBase(t.Context(), nil); err != nil || !ok {
		t.Fatalf("certify: %t %v", ok, err)
	}
	src := c.capture.Segments[0]
	if _, _, cached := c.store.fences.get(src.Entry.ID, len(longKey)); cached {
		t.Fatal("oversized fence pair was cached")
	}
	for _, src := range c.capture.Segments {
		f, l, err := c.snapshotBounds(t.Context(), src.Entry)
		if err != nil || src.Entry.ID == 1 && (f != longKey || l != "z") || src.Entry.ID == 2 && (f != "zz" || l != "zz") {
			t.Fatalf("wrong fences for file %d, lengths %d and %d: %v", src.Entry.ID, len(f), len(l), err)
		}
	}
	fd, err := os.OpenFile(src.Segment.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Alter the last boundary without changing the segment or record headers.
	_, err = fd.WriteAt([]byte("y"), src.Segment.snapshotRange.lastOffset+format.RecordHeaderBytes)
	if err = errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	_, _, err = c.snapshotBounds(t.Context(), src.Entry)
	read := failedDataRead(err)
	if !errors.Is(err, ErrCorrupt) || read == nil || read.id != src.Entry.ID {
		t.Fatalf("changed boundary: %v", err)
	}
}

func TestOrderedBaseAdoptsOnlyIdenticalSelectedSnapshot(t *testing.T) {
	c := snapshotForBoundaries(t, []string{"a", "c"})
	if ok, err := c.certifyCapturedBase(t.Context(), nil); err != nil || !ok {
		t.Fatalf("certify: %t %v", ok, err)
	}
	c.ordered = true
	c.entries = append(c.entries, c.capture.Catalog.Entries...)
	c.store.Catalog = c.capture.Catalog
	// A concurrent rotation may add tail files after the captured prefix.
	entries := append(append([]format.CatalogEntry{}, c.entries...), format.CatalogEntry{ID: 6, Kind: format.RawLogKind})
	c.store.Catalog.Entries = nil
	c.store.catalogEntries = CatalogEntries{catalogFromSlice(entries)}
	if !c.AdoptUnchangedOrdered() || !c.store.orderedSnapshot {
		t.Fatal("did not adopt stable base with concurrent tail")
	}
	entries[0].ID++
	c.store.catalogEntries = CatalogEntries{catalogFromSlice(entries)}
	if c.AdoptUnchangedOrdered() {
		t.Fatal("adopted a different selected base")
	}
}
