package tarp

import (
	"errors"
	"math"
	"testing"
)

func TestOptionsAdmission(t *testing.T) {
	o := Options{CacheBytes: 130, MaxRecordBytes: 50, MaxDataFileBytes: 114, ReadFileHandles: 1}
	if _, err := o.normalized(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		k, v uint64
		want error
	}{{0, 0, ErrEmptyKey}, {1, 1, nil}, {2, 0, nil}, {1, 2, ErrTooLarge}, {math.MaxUint64, 1, ErrTooLarge}} {
		if err := o.checkSize(tc.k, tc.v); !errors.Is(err, tc.want) {
			t.Fatalf("size %d/%d: %v", tc.k, tc.v, err)
		}
	}
	for _, invalid := range []Options{{CacheBytes: -1}, {CacheBytes: 128}, {MaxRecordBytes: 48}, {MaxDataFileBytes: 112}, {ReadFileHandles: -1}, {MaxRecordBytes: 100, MaxDataFileBytes: 150}} {
		if _, err := invalid.normalized(); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("options %#v: %v", invalid, err)
		}
	}
}

func TestTargetSegmentBytesNormalization(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
		want int64
	}{
		{"default", Options{}, 4 << 20},
		{"override", Options{TargetSegmentBytes: 5 << 20}, 5 << 20},
		{"hard maximum", Options{TargetSegmentBytes: math.MaxInt64}, 128 << 20},
		{"fit largest record", Options{MaxRecordBytes: 6 << 20, TargetSegmentBytes: 400}, (6 << 20) + 64},
		{"near integer limit", Options{MaxDataFileBytes: math.MaxInt64, MaxRecordBytes: math.MaxInt64 - 64}, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.opts.normalized()
			if err != nil || got.TargetSegmentBytes != tc.want {
				t.Fatalf("target %d, want %d: %v", got.TargetSegmentBytes, tc.want, err)
			}
		})
	}
	if _, err := (Options{TargetSegmentBytes: -1}).normalized(); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("negative target: %v", err)
	}
}
