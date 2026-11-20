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
				size.CacheBytes != tc.cache || size.MaxRecordBytes != tc.record || size.Overflow {
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

func TestBatchLimitsReportOriginalInputAndPreserveSizeCause(t *testing.T) {
	opts := tarp.Options{CacheBytes: 132, MaxRecordBytes: 52, TargetSegmentBytes: 164}
	db := openPublicDB(t, t.TempDir(), opts) // 100-byte segment payload
	view, err := tarp.Bind(db, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Put("a", "old"); err != nil {
		t.Fatal(err)
	}
	initial := db.Stats()
	count := tarp.MaxBatchEntries + 1
	for _, tc := range []struct {
		name   string
		run    func() error
		kind   tarp.BatchLimitKind
		index  int
		actual uint64
		limit  uint64
	}{
		{"raw get count", func() error { _, err := db.BatchGet(make([][]byte, count)); return err }, tarp.BatchInputCountLimit, -1, uint64(count), tarp.MaxBatchEntries},
		{"typed put count", func() error { return view.BatchPut(make([]tarp.TypedEntry[string, string], count)) }, tarp.BatchInputCountLimit, -1, uint64(count), tarp.MaxBatchEntries},
		{"raw put entry", func() error {
			return db.BatchPut([]tarp.Entry{{Key: []byte("a")}, {Key: []byte("a"), Value: []byte("long")}})
		}, tarp.BatchEntrySizeLimit, 1, 0, 0},
		{"typed put entry", func() error {
			return view.BatchPut([]tarp.TypedEntry[string, string]{{Key: "a"}, {Key: "a", Value: "long"}})
		}, tarp.BatchEntrySizeLimit, 1, 0, 0},
		{"repeated put payload", func() error {
			return view.BatchPut([]tarp.TypedEntry[string, string]{{Key: "a"}, {Key: "a"}, {Key: "a"}})
		}, tarp.BatchPayloadLimit, 2, 147, 100},
		{"distinct delete payload", func() error { return view.BatchDelete([]string{"a", "a", "b", "c", "d"}) }, tarp.BatchPayloadLimit, 3, 147, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var limit *tarp.BatchLimitError
			if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &limit) || limit.Kind != tc.kind ||
				limit.Index != tc.index || limit.Actual != tc.actual || limit.Limit != tc.limit {
				t.Fatalf("incorrect batch limit: %v (%+v)", err, limit)
			}
			var entry *tarp.SizeError
			if tc.kind == tarp.BatchEntrySizeLimit {
				if !errors.As(err, &entry) || entry != limit.Entry || entry.KeyBytes != 1 || entry.ValueBytes != 4 ||
					entry.RecordBytes != 53 || entry.CacheCharge != 133 || entry.MaxRecordBytes != 52 || entry.CacheBytes != 132 {
					t.Fatalf("lost entry size details: %+v", entry)
				}
			} else if errors.As(err, &entry) || limit.Entry != nil {
				t.Fatalf("unexpected size error: %+v", entry)
			}
			if got := db.Stats(); got != initial {
				t.Fatalf("rejected batch changed Stats: %+v", got)
			}
		})
	}
	if value, err := view.Get("a"); value != "old" || err != nil {
		t.Fatalf("rejected batch changed value: %q, %v", value, err)
	}
}
