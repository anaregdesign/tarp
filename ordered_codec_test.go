package tarp_test

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

func TestOrderedScalarCodecs(t *testing.T) {
	checkOrderedCodec(t, tarp.OrderedStringCodec{}, []string{"", "\x00", "\x00\xff", "a", "a\x00", "ab", "\xff"}, func(a, b string) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedBytesCodec{}, [][]byte{{}, {0}, {0, 0xff}, {'a'}, {'a', 0}, {'b'}}, bytes.Equal)
	checkOrderedCodec(t, tarp.OrderedInt8Codec{}, []int8{math.MinInt8, -1, 0, 1, math.MaxInt8}, func(a, b int8) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedInt16Codec{}, []int16{math.MinInt16, -1, 0, 1, math.MaxInt16}, func(a, b int16) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedInt32Codec{}, []int32{math.MinInt32, -1, 0, 1, math.MaxInt32}, func(a, b int32) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedInt64Codec{}, []int64{math.MinInt64, -1, 0, 1, math.MaxInt64}, func(a, b int64) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedIntCodec{}, []int{math.MinInt, -1, 0, 1, math.MaxInt}, func(a, b int) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedFloat32Codec{}, []float32{float32(math.Inf(-1)), -1.5, 0, 1.5, float32(math.Inf(1))}, func(a, b float32) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedFloat64Codec{}, []float64{math.Inf(-1), -1.5, 0, 1.5, math.Inf(1)}, func(a, b float64) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedDurationCodec{}, []time.Duration{-time.Hour, -1, 0, 1, time.Hour}, func(a, b time.Duration) bool { return a == b })
	checkOrderedCodec(t, tarp.OrderedTimeCodec{}, []time.Time{
		time.Unix(-100, 0).UTC(),
		time.Unix(-1, 999_999_999).UTC(),
		time.Unix(0, 0).UTC(),
		time.Unix(100, 500).UTC(),
	}, func(a, b time.Time) bool { return a.Equal(b) && a.Location() == time.UTC })

	// Nil and empty []byte share the canonical empty encoding and decode to non-nil empty slice.
	nilEnc, err := (tarp.OrderedBytesCodec{}).Encode(nil)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := (tarp.OrderedBytesCodec{}).Decode(nilEnc)
	if err != nil {
		t.Fatal(err)
	}
	if dec == nil || len(dec) != 0 {
		t.Fatalf("OrderedBytesCodec nil round-trip: got %#v, want empty non-nil slice", dec)
	}

	// Trailing bytes rejected on variable-width codecs; wrong size rejected on fixed-width codecs.
	if _, err := (tarp.OrderedStringCodec{}).Decode([]byte{'a', 0, 0, 'b'}); err == nil {
		t.Fatal("expected trailing byte error on OrderedStringCodec")
	}
	if _, err := (tarp.OrderedBytesCodec{}).Decode([]byte{'a', 0, 0, 'b'}); err == nil {
		t.Fatal("expected trailing byte error on OrderedBytesCodec")
	}
	if _, err := (tarp.OrderedInt64Codec{}).Decode([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected size error on OrderedInt64Codec")
	}
}

func checkOrderedCodec[T any](t *testing.T, c tarp.Codec[T], sorted []T, eq func(a, b T) bool) {
	t.Helper()
	var prev []byte
	for i, v := range sorted {
		enc, err := c.Encode(v)
		if err != nil {
			t.Fatalf("%T Encode(%v): %v", c, v, err)
		}
		got, err := c.Decode(enc)
		if err != nil {
			t.Fatalf("%T Decode(%x): %v", c, enc, err)
		}
		if !eq(got, v) {
			t.Fatalf("%T round-trip %d: got %#v, want %#v", c, i, got, v)
		}
		if i > 0 && bytes.Compare(prev, enc) >= 0 {
			t.Fatalf("%T order violation at %d: %x >= %x", c, i, prev, enc)
		}
		prev = enc
	}
}
