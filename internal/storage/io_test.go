package storage

import (
	"errors"
	"io"
	"os"
	"slices"
	"syscall"
	"testing"
)

func TestWriteRetriesOnlyUnwrittenBytes(t *testing.T) {
	const start = 17
	for _, tc := range []struct {
		name       string
		data       string
		counts     []int
		lastError  error
		wantError  error
		wantCounts []int
	}{
		{"empty", "", nil, nil, nil, nil},
		{"several short writes", "abcdef", []int{1, 2, 3}, nil, nil, []int{1, 2, 3}},
		{"no progress", "abcdef", []int{0}, nil, io.ErrShortWrite, nil},
		{"no progress after short write", "abcdef", []int{2, 0}, nil, io.ErrShortWrite, []int{2}},
		{"failed without progress", "abcdef", []int{0}, syscall.EIO, syscall.EIO, nil},
		{"partial write with error", "abcdef", []int{2, 1}, syscall.EIO, syscall.EIO, []int{2, 1}},
		{"full count with error", "abcdef", []int{6}, syscall.EIO, syscall.EIO, []int{6}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls, accepted int
			var counts []int
			var hooks []string
			fs := &IO{
				WriteAt: func(_ *os.File, b []byte, offset int64) (int, error) {
					if calls >= len(tc.counts) {
						t.Fatalf("unexpected extra write at %d", offset)
					}
					if offset != int64(start+accepted) || string(b) != tc.data[accepted:] {
						t.Fatalf("WriteAt(%q, %d), want (%q, %d)", b, offset, tc.data[accepted:], start+accepted)
					}
					n := tc.counts[calls]
					calls++
					accepted += n
					if calls == len(tc.counts) {
						return n, tc.lastError
					}
					return n, nil
				},
				Wrote: func(point string, n int) {
					if point != "record" {
						t.Fatalf("Wrote point %q, want record", point)
					}
					counts = append(counts, n)
				},
				Hook: func(point string) error {
					hooks = append(hooks, point)
					return nil
				},
			}
			err := fs.Write(nil, []byte(tc.data), start, "record")
			if !errors.Is(err, tc.wantError) || calls != len(tc.counts) || !slices.Equal(counts, tc.wantCounts) {
				t.Fatalf("Write returned %v after %d calls, observed %v bytes; want %v, %d calls, %v", err, calls, counts, tc.wantError, len(tc.counts), tc.wantCounts)
			}
			wantHooks := []string{"record:before"}
			if tc.wantError == nil {
				wantHooks = append(wantHooks, "record:after")
			}
			if !slices.Equal(hooks, wantHooks) {
				t.Fatalf("hooks %v, want %v", hooks, wantHooks)
			}
		})
	}
}

func TestWriteRejectsImpossibleCountsBeforeAccounting(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
	}{{"negative", -1}, {"greater than request", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			fs := &IO{
				WriteAt: func(*os.File, []byte, int64) (int, error) {
					calls++
					if calls > 1 {
						t.Fatal("retried an invalid count")
					}
					return tc.n, nil
				},
				Wrote: func(point string, n int) {
					t.Fatalf("recorded invalid count %d at %s", n, point)
				},
			}
			if err := fs.Write(nil, []byte{1}, 0, "record"); err == nil || calls != 1 {
				t.Fatalf("Write returned %v after %d calls, want error after one call", err, calls)
			}
		})
	}
}
