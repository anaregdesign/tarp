package tarp_test

import (
	"errors"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestPublicDBReportsSizeAndReopenLimits(t *testing.T) {
	// A one-byte key and a three-byte value occupy a 52-byte record
	// and have a 132-byte logical cache charge.
	for _, tc := range []struct {
		name          string
		cache, record int64
	}{
		{"cache", 132, 64},
		{"record", 256, 52},
		{"both", 132, 52},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := tarp.Options{CacheBytes: tc.cache, MaxRecordBytes: tc.record}
			db := openPublicDB(t, dir, opts)
			key := []byte("k")
			if err := db.Put(key, []byte("old")); err != nil {
				t.Fatal(err)
			}
			err := db.Put(key, []byte("over"))
			var size *tarp.SizeError
			if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &size) {
				t.Fatalf("oversized Put = %v", err)
			}
			if size.KeyBytes != 1 || size.ValueBytes != 4 || size.RecordBytes != 53 || size.CacheCharge != 133 ||
				size.Limits.CacheBytes != tc.cache || size.Limits.MaxRecordBytes != tc.record || size.Overflow {
				t.Fatalf("incorrect admission costs or limits: %+v", size)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			opts.CacheBytes--
			opts.MaxRecordBytes--
			failed, err := tarp.Open(dir, opts)
			var required *tarp.CompatibilityError
			if failed != nil || !errors.Is(err, tarp.ErrIncompatibleOptions) || !errors.As(err, &required) {
				t.Fatalf("Open with smaller limits = %v, %v", failed, err)
			}
			if required.RequiredCacheBytes != 132 || required.RequiredRecordBytes != 52 {
				t.Fatalf("wrong reopen minima: %+v", required)
			}
			exact := tarp.Options{CacheBytes: required.RequiredCacheBytes, MaxRecordBytes: required.RequiredRecordBytes}
			if value, err := openPublicDB(t, dir, exact).Get(key); string(value) != "old" || err != nil {
				t.Fatalf("Get after reopen at reported minima = %q, %v", value, err)
			}
		})
	}
}
