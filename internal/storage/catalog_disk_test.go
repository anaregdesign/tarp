package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestCatalogRotationEditsSurviveReopenAndRebase(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Handles: 1, ValueCacheBudgetBytes: 1 << 20, MaxRecordBytes: 1 << 20, TargetSegmentBytes: 1 << 20}
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range rebaseCatalogAt + 1 {
		if _, err := s.Append(format.Put, []byte(fmt.Sprint(i)), []byte("data"), func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := s.Rotate(); err != nil {
			t.Fatal(err)
		}
	}
	tip := s.Current
	view := s.CatalogEntries()
	tipBytes, err := os.ReadFile(filepath.Join(dir, ManifestName(tip.UUID, tip.ManifestID)))
	if err != nil || !format.IsCatalogEdit(tipBytes) || len(tipBytes) != format.MaxCatalogEditBytes {
		t.Fatalf("expected a bounded incremental manifest, got %d bytes, %v", len(tipBytes), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.Current != tip || !slices.Equal(slices.Collect(view.All()), s.CatalogSnapshot().Entries) || !s.NeedsCatalogRebase() {
		t.Fatal("reopen did not reconstruct the selected chain")
	}
	if err := s.RebaseCatalog(func(prepare func() error) error {
		if err := prepare(); err != nil {
			return err
		}
		if _, err := s.Append(format.Put, []byte("racing rotation"), []byte("data"), func() error { return nil }); err != nil {
			return err
		}
		return s.Rotate()
	}); err != nil {
		t.Fatal(err)
	}
	if s.Current.LastUserSequence != tip.LastUserSequence+1 || s.CatalogEntries().Len() != view.Len()+1 {
		t.Fatal("a prepared base superseded a newer rotation")
	}
	tip = s.Current
	before := slices.Clone(s.catalogChain)
	if err := s.RebaseCatalog(func(prepare func() error) error { return prepare() }); err != nil {
		t.Fatal(err)
	}
	if len(s.catalogChain) != 1 || s.Current.LastUserSequence != tip.LastUserSequence || s.Current.ManifestID == tip.ManifestID {
		t.Fatal("rebase failed to select a standalone base at the same user sequence")
	}
	if _, _, err := s.Cleanup(); err != nil {
		t.Fatal(err)
	}
	for _, id := range before {
		if _, err := os.Stat(filepath.Join(dir, ManifestName(tip.UUID, id))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("retired manifest %d remains: %v", id, err)
		}
	}
	if _, err := readImage(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogRejectsCorruptSelectedPredecessor(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Handles: 1, ValueCacheBudgetBytes: 1 << 20, MaxRecordBytes: 1 << 20, TargetSegmentBytes: 1 << 20}
	s, err := openTestStore(t, dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, ManifestName(s.Current.UUID, s.Current.ManifestID))
	if _, err := s.Append(format.Put, []byte("key"), []byte("value"), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(base, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := openTestStore(t, dir, cfg, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt selected predecessor did not fail reopen: %v", err)
	}
}
