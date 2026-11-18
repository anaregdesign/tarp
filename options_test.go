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
