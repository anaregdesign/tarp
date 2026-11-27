package btree

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
)

func captureOverlay(t *testing.T, backend index.Backend, previous uint64) (OverlayLayout, []byte) {
	t.Helper()
	var layout OverlayLayout
	var data bytes.Buffer
	next := previous + 1
	if err := ExportOverlay(t.Context(), backend, func(got OverlayLayout, baseTop uint64) error {
		if baseTop != previous {
			t.Fatalf("inherited page top = %d, want %d", baseTop, previous)
		}
		layout = got
		return nil
	}, func(id uint64, p []byte) error {
		if id != next || len(p) != pageSize {
			t.Fatalf("page %d of size %d, want %d and %d bytes", id, len(p), next, pageSize)
		}
		next++
		_, err := data.Write(p)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if next != layout.Top+1 {
		t.Fatalf("page emission ended at %d, want %d", next, layout.Top+1)
	}
	return layout, data.Bytes()
}

type countedOverlaySource struct {
	io.ReaderAt
	reads int
}

func (s *countedOverlaySource) ReadAt(p []byte, offset int64) (int, error) {
	s.reads++
	return s.ReaderAt.ReadAt(p, offset)
}

func TestOverlayWritesOnlyNewPagesAndReopensAcrossLayers(t *testing.T) {
	tr := openTest(t)
	want := make(map[string]index.Address)
	firstLong := strings.Repeat("overflow-a", 1_000)
	keys := []string{firstLong}
	for i := range 1800 {
		keys = append(keys, fmt.Sprintf("p%08d", i))
	}
	slices.Sort(keys)
	if err := tr.BuildSorted(t.Context(), func(put func(string, index.Address) error) error {
		for i, key := range keys {
			address := packedAddress(key, uint64(i+1))
			want[key] = address
			if err := put(key, address); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var packed bytes.Buffer
	if err := ExportPacked(t.Context(), tr, &packed); err != nil {
		t.Fatal(err)
	}
	base, err := PackedOverlayLayout(packed.Bytes()[:packedHeaderBytes], int64(packed.Len()))
	if err != nil {
		t.Fatal(err)
	}
	data := packed.Bytes()[packedHeaderBytes:]
	source := &countedOverlaySource{ReaderAt: bytes.NewReader(data)}
	work, err := OpenOverlay(t.Context(), source, nil, base, t.TempDir(), MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()
	old := work.View()
	defer old.Close()
	change := work.Begin()
	secondLong := strings.Repeat("overflow-b", 1_000)
	winner := packedAddress(secondLong, 9_998)
	if err := change.Put(secondLong, winner); err != nil {
		t.Fatal(err)
	}
	if err := change.Delete(firstLong); err != nil {
		t.Fatal(err)
	}
	change.Commit()
	readBeforeExport := source.reads
	first, diff := captureOverlay(t, work, base.Top)
	if got := source.reads - readBeforeExport; uint64(got) >= base.Top {
		t.Fatalf("overlay read %d immutable pages from a %d-page base", got, base.Top)
	}
	if first.Top <= base.Top || first.Top-base.Top >= base.Top {
		t.Fatalf("overlay size grew from %d to %d pages", base.Top, first.Top)
	}
	if got, ok, err := old.Lookup(firstLong); !ok || err != nil || got != want[firstLong] {
		t.Fatalf("old pinned root lost key: %+v %t %v", got, ok, err)
	}
	delete(want, firstLong)
	want[secondLong] = winner
	data = append(append([]byte(nil), data...), diff...)
	reopened, err := OpenOverlay(t.Context(), bytes.NewReader(data), nil, first, t.TempDir(), MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	view := reopened.View()
	verify(t, view, want)
	view.Close()

	next := reopened.Begin()
	changedKey := keys[20]
	address := packedAddress(changedKey, 9_999)
	if err := next.Put(changedKey, address); err != nil {
		t.Fatal(err)
	}
	next.Commit()
	want[changedKey] = address
	second, diff := captureOverlay(t, reopened, first.Top)
	if second.Top <= first.Top {
		t.Fatal("the second overlay did not reserve new pages")
	}
	data = append(data, diff...)
	reopenedAgain, err := OpenOverlay(t.Context(), bytes.NewReader(data), nil, second, t.TempDir(), MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedAgain.Close()
	view = reopenedAgain.View()
	defer view.Close()
	verify(t, view, want)
}

func TestOverlayEmptyRootPreservesHolesAndAllocatorTop(t *testing.T) {
	work, err := OpenOverlay(t.Context(), nil, nil, OverlayLayout{}, t.TempDir(), MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer work.Close()
	change := work.Begin()
	if err := change.Put("temporary", packedAddress("temporary", 1)); err != nil {
		t.Fatal(err)
	}
	if err := change.Delete("temporary"); err != nil {
		t.Fatal(err)
	}
	change.Commit()
	layout, diff := captureOverlay(t, work, 0)
	if layout.Root != 0 || layout.Keys != 0 || layout.MaxKey != 0 || layout.Top == 0 ||
		!bytes.Equal(diff, make([]byte, len(diff))) {
		t.Fatalf("empty overlay = %+v, expected zero-filled retained page IDs", layout)
	}
	reopened, err := OpenOverlay(t.Context(), bytes.NewReader(diff), nil, layout, t.TempDir(), MinBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, found, err := reopened.Lookup("temporary"); found || err != nil {
		t.Fatalf("deleted key reappeared: %t, %v", found, err)
	}
	change = reopened.Begin()
	if err := change.Put("after", packedAddress("after", 2)); err != nil {
		t.Fatal(err)
	}
	change.Commit()
	added, _ := captureOverlay(t, reopened, layout.Top)
	if added.Root <= layout.Top || added.Keys != 1 {
		t.Fatalf("new root reused a reserved page: previous=%+v, current=%+v", layout, added)
	}
}

func TestOverlayRejectsInvalidRootsAndBounds(t *testing.T) {
	for _, layout := range []OverlayLayout{
		{Root: 2, Top: 1, Keys: 1, MaxKey: 1},
		{Top: 1, Keys: 1, MaxKey: 1},
		{Top: math.MaxInt64/pageSize + 1},
		{Root: 1, Top: 1, MaxKey: 1},
		{Root: 1, Top: 1, Keys: pageSize, MaxKey: 1},
		{Root: 1, Top: 1, Keys: 1, MaxKey: pageSize + 1},
		{Root: 1, Top: 1, Keys: 1, MaxKey: 1}, // Zero-filled root page.
	} {
		if got, err := OpenOverlay(t.Context(), bytes.NewReader(make([]byte, pageSize)), nil, layout, t.TempDir(), MinBudget); got != nil || !errors.Is(err, ErrInvalidPacked) {
			t.Fatalf("invalid overlay %+v = %v, %v", layout, got, err)
		}
	}
	if _, err := PackedOverlayLayout([]byte("short"), packedHeaderBytes); !errors.Is(err, ErrInvalidPacked) {
		t.Fatalf("short packed header = %v", err)
	}
}
