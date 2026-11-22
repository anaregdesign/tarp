package storage

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func adjacentReadFixture(t *testing.T) (*Store, []AdjacentRead) {
	t.Helper()
	s, err := openTestStore(t, t.TempDir(), Config{Handles: 1, MaxRecordBytes: 256, TargetSegmentBytes: 1024, ValueCacheBudgetBytes: 1024}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	entries := []Entry{{[]byte("a"), []byte("one")}, {[]byte("b"), []byte("two")}, {[]byte("c"), []byte("three")}}
	locs, err := s.AppendBatch(format.Put, entries, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	group := make([]AdjacentRead, len(locs))
	for i, loc := range locs {
		group[i] = AdjacentRead{Location: loc, Key: entries[i].Key}
	}
	locs[0].File.Pin()
	t.Cleanup(locs[0].File.Unpin)
	return s, group
}

func TestReadAdjacentReportsSecondRecordAfterTruncationOrCorruption(t *testing.T) {
	for _, damage := range []string{"missing second record", "short second record", "bad second checksum"} {
		t.Run(damage, func(t *testing.T) {
			s, group := adjacentReadFixture(t)
			second := group[1].Location
			lastByte := second.Offset + second.Bytes - 1
			wantErr := io.EOF
			var err error
			switch damage {
			case "missing second record":
				err = s.active.Truncate(second.Offset)
			case "short second record":
				err = s.active.Truncate(lastByte)
			case "bad second checksum":
				_, err = s.active.WriteAt([]byte{'!'}, lastByte)
				wantErr = format.ErrCorrupt
			}
			if err != nil {
				t.Fatal(err)
			}
			err = s.ReadAdjacent(group)
			location := fmt.Sprintf("%s: record at offset %d:", filepath.Base(second.File.Path), second.Offset)
			if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), location) {
				t.Fatalf("read error %v, want %v at %s", err, wantErr, location)
			}
		})
	}
}

func TestReadAdjacentReturnsIndependentValues(t *testing.T) {
	s, group := adjacentReadFixture(t)
	if err := s.ReadAdjacent(group); err != nil {
		t.Fatal(err)
	}
	first := group[0].Value
	first[0] = '!'
	if err := s.ReadAdjacent(group); err != nil {
		t.Fatal(err)
	}
	if string(first) != "!ne" || string(group[0].Value) != "one" || string(group[1].Value) != "two" || string(group[2].Value) != "three" {
		t.Fatalf("prior result %q, next group %q %q %q", first, group[0].Value, group[1].Value, group[2].Value)
	}
}
