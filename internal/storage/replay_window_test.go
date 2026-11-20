package storage

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestReadOnlyReplayWindowsMatchFullValidation(t *testing.T) {
	dir := t.TempDir()
	s, _, err := Open(dir, Config{Handles: 1, TargetSegmentBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var puts []Entry
	for i := 63; i >= 0; i-- {
		key := fmt.Sprintf("prefix\x00%02d", i)
		if i == 31 {
			key = strings.Repeat("prefix", 400)
		}
		puts = append(puts, Entry{Key: []byte(key), Value: []byte(strings.Repeat("v", i))})
	}
	if _, err := s.AppendBatch(format.Put, puts, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	var deletes []Entry
	for i := 0; i < len(puts); i += 4 {
		deletes = append(deletes, Entry{Key: puts[i].Key})
	}
	if _, err := s.AppendBatch(format.Delete, deletes, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendBatch(format.Put, puts[:3], func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	full, err := Validate(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, budget := range []int64{1, 512, 4096, verifyWindowBytes} {
		img, err := validateReadOnly(t.Context(), dir, budget)
		if err != nil {
			t.Fatal(err)
		}
		if img.Index != nil || img.LiveKeys != uint64(len(full.Index)) || img.Required != full.Required || img.MaxValueBytes != full.MaxValueBytes {
			t.Fatalf("budget %d: live=%d required=%+v maxValue=%d", budget, img.LiveKeys, img.Required, img.MaxValueBytes)
		}
	}
	// This tombstone sorts after every key retained by the first small window.
	if _, err := s.Append(format.Delete, []byte("zzmissing"), nil, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := validateReadOnly(t.Context(), dir, 256); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("invalid tombstone in late window: %v", err)
	}
}

func TestReplayWindowsAdvancePastBinaryPrefixesAndOneOversizedKey(t *testing.T) {
	keys := []string{"\xff", "a", "a\x00", "\x00", "\x00\x00", strings.Repeat("a", 1000), "\xff\xff"}
	var visited []string
	for after := ""; ; {
		w := newKeyWindow(after, 2*replayKeyCharge+4)
		for _, key := range keys {
			r := format.Record{Kind: format.Put, KeyBytes: int64(len(key)), Sequence: 1}
			if err := w.apply(format.Log, 0, r, key); err != nil {
				t.Fatal(err)
			}
			if w.used > w.limit && len(w.live) != 1 {
				t.Fatalf("used=%d keys=%d", w.used, len(w.live))
			}
		}
		for key := range w.live {
			visited = append(visited, key)
		}
		if !w.more {
			break
		}
		if w.keys[0] <= after {
			t.Fatal("no progress")
		}
		after = w.keys[0]
	}
	slices.Sort(keys)
	slices.Sort(visited)
	if !slices.Equal(keys, visited) {
		t.Fatalf("partition %q, want %q", visited, keys)
	}
}
