package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
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
