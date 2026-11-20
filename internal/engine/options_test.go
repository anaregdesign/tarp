package engine

import (
	"errors"
	"math"
	"testing"
)

func TestOptionsAdmission(t *testing.T) {
	o := Options{ValueCacheBudgetBytes: 130, MaxRecordBytes: 50, ReadFileHandles: 1}
	if _, err := o.normalized(); err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(100, func() {
		if err := o.checkSize(1, 1); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("successful entry admission allocated %v times", allocs)
	}
	for _, tc := range []struct {
		k, v uint64
		want error
	}{{0, 0, ErrEmptyKey}, {1, 1, nil}, {2, 0, nil}, {1, 2, ErrTooLarge}, {math.MaxUint64, 1, ErrTooLarge}} {
		if err := o.checkSize(tc.k, tc.v); !errors.Is(err, tc.want) {
			t.Fatalf("size %d/%d: %v", tc.k, tc.v, err)
		}
	}
	for _, invalid := range []Options{{ValueCacheBudgetBytes: -1}, {ValueCacheBudgetBytes: 128}, {MaxRecordBytes: 48}, {ReadFileHandles: -1}, {MaxRecordBytes: math.MaxInt64 - 63}, {MaxBatchGetValueBytes: -1}} {
		if _, err := invalid.normalized(); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("options %#v: %v", invalid, err)
		}
	}
}

func TestMaxBatchGetValueBytesNormalization(t *testing.T) {
	for _, tc := range []struct{ input, want int64 }{{0, 64 << 20}, {1, 1}, {math.MaxInt64, math.MaxInt64}} {
		got, err := (Options{MaxBatchGetValueBytes: tc.input}).normalized()
		if err != nil || got.MaxBatchGetValueBytes != tc.want {
			t.Fatalf("MaxBatchGetValueBytes %d: got %d, %v", tc.input, got.MaxBatchGetValueBytes, err)
		}
	}
}

func TestAdmissionCacheAndRecoveryUseSameEntrySizes(t *testing.T) {
	opts := Options{MaxRecordBytes: 51, ValueCacheBudgetBytes: 131}
	db := newTestDB(t, opts, nil)
	mustPut(t, db, "ab", "x")
	stats := db.Stats()
	if stats.LiveRecordBytes != 51 || stats.ValueCache.UsedBytes != 131 {
		t.Fatalf("record bytes = %d, cache bytes = %d", stats.LiveRecordBytes, stats.ValueCache.UsedBytes)
	}
	var size *SizeError
	if err := db.Put([]byte("ab"), []byte("xx")); !errors.As(err, &size) || size.RecordBytes != 52 || size.ValueCacheChargeBytes != 132 {
		t.Fatalf("size error = %+v, err = %v", size, err)
	}
	dir := db.store.Dir
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, smaller := range []Options{{MaxRecordBytes: 50, ValueCacheBudgetBytes: 131}, {MaxRecordBytes: 51, ValueCacheBudgetBytes: 130}} {
		var required *CompatibilityError
		opened, err := Open(dir, smaller)
		if opened != nil {
			opened.Close()
		}
		if !errors.As(err, &required) || required.RequiredRecordBytes != 51 || required.RequiredValueCacheBudgetBytes != 131 {
			t.Fatalf("reopen with %+v: required = %+v, err = %v", smaller, required, err)
		}
	}
	reopened, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	wantValue(t, reopened, "ab", "x")
}

func TestTargetSegmentBytesNormalization(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
		want int64
	}{
		{"default", Options{}, 4 << 20},
		{"override", Options{TargetSegmentBytes: 5 << 20}, 5 << 20},
		{"largest target", Options{TargetSegmentBytes: math.MaxInt64}, math.MaxInt64},
		{"fit largest record", Options{MaxRecordBytes: 6 << 20, TargetSegmentBytes: 400}, (6 << 20) + 64},
		{"largest record that fits", Options{MaxRecordBytes: math.MaxInt64 - 64}, math.MaxInt64},
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
