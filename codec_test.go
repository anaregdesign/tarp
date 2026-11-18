package tarp_test

import (
	"bytes"
	"encoding/hex"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

func fixedCodecCase[T comparable](codec tarp.Codec[T], value T, wire string) func(*testing.T) {
	return func(t *testing.T) {
		want, err := hex.DecodeString(wire)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range []tarp.Codec[T]{codec, mustDerive[T](t)} {
			got, err := c.Encode(value)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%T.Encode(%v) = %x, %v; want %x", c, value, got, err, want)
			}
			decoded, err := c.Decode(want)
			if err != nil || decoded != value {
				t.Fatalf("%T.Decode(%x) = %v, %v; want %v", c, want, decoded, err, value)
			}
			for _, malformed := range [][]byte{want[:len(want)-1], append(bytes.Clone(want), 0)} {
				if _, err := c.Decode(malformed); err == nil {
					t.Errorf("%T.Decode(%x) accepted %d bytes; want %d", c, malformed, len(malformed), len(want))
				}
			}
		}
	}
}

func TestBuiltinCodecsUseFixedEncodings(t *testing.T) {
	cases := []struct {
		name string
		run  func(*testing.T)
	}{
		{"false", fixedCodecCase(tarp.BoolCodec{}, false, "00")},
		{"true", fixedCodecCase(tarp.BoolCodec{}, true, "01")},
		{"int", fixedCodecCase(tarp.IntCodec{}, -2, "fffffffffffffffe")},
		{"uint", fixedCodecCase(tarp.UintCodec{}, uint(258), "0000000000000102")},
		{"uintptr", fixedCodecCase(tarp.UintptrCodec{}, uintptr(259), "0000000000000103")},
		{"int8", fixedCodecCase(tarp.Int8Codec{}, int8(-128), "80")},
		{"uint8/byte", fixedCodecCase(tarp.Uint8Codec{}, byte(255), "ff")},
		{"int16", fixedCodecCase(tarp.Int16Codec{}, int16(-32767), "8001")},
		{"uint16", fixedCodecCase(tarp.Uint16Codec{}, uint16(0xff01), "ff01")},
		{"int32/rune", fixedCodecCase(tarp.Int32Codec{}, rune(-2147483647), "80000001")},
		{"uint32", fixedCodecCase(tarp.Uint32Codec{}, uint32(0xff010203), "ff010203")},
		{"int64", fixedCodecCase(tarp.Int64Codec{}, int64(math.MinInt64+1), "8000000000000001")},
		{"uint64", fixedCodecCase(tarp.Uint64Codec{}, uint64(0xff01020304050607), "ff01020304050607")},
		{"float32", fixedCodecCase(tarp.Float32Codec{}, float32(-2.5), "c0200000")},
		{"float64", fixedCodecCase(tarp.Float64Codec{}, -2.5, "c004000000000000")},
		{"complex64", fixedCodecCase(tarp.Complex64Codec{}, complex64(-2.5+1i), "c02000003f800000")},
		{"complex128", fixedCodecCase(tarp.Complex128Codec{}, -2.5+1i, "c0040000000000003ff0000000000000")},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.run)
	}
}

func TestBoolCodecRejectsNoncanonicalBytes(t *testing.T) {
	for _, b := range []byte{2, 255} {
		if _, err := (tarp.BoolCodec{}).Decode([]byte{b}); err == nil {
			t.Errorf("Decode(%x) accepted an invalid boolean", b)
		}
	}
}

func TestStringCodecPreservesArbitraryBytes(t *testing.T) {
	codec := tarp.StringCodec{}
	for _, value := range []string{"", "日本語", "\xff\x00\x80"} {
		encoded, err := codec.Encode(value)
		if err != nil || string(encoded) != value {
			t.Fatalf("Encode(%q) = %x, %v", value, encoded, err)
		}
		decoded, err := codec.Decode(encoded)
		if err != nil || decoded != value {
			t.Fatalf("Decode(%x) = %q, %v; want %q", encoded, decoded, err, value)
		}
	}
}

func TestBytesCodecReturnsIndependentBytes(t *testing.T) {
	codec := tarp.BytesCodec{}
	for _, convert := range []func([]byte) ([]byte, error){codec.Encode, codec.Decode} {
		for _, in := range [][]byte{nil, {}, {0xff, 0x00, 0x80}} {
			out, err := convert(in)
			if err != nil || !bytes.Equal(out, in) {
				t.Fatalf("conversion of %x = %x, %v", in, out, err)
			}
			if len(out) > 0 {
				out[0] = 1
				if in[0] != 0xff {
					t.Fatal("changing the output modified the input")
				}
			}
		}
	}
}

func checkBitRoundTrip[T any](t *testing.T, codec tarp.Codec[T], data []byte) {
	t.Helper()
	value, err := codec.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Encode(value)
	if err != nil || !bytes.Equal(got, data) {
		t.Errorf("round trip of %x = %x, %v", data, got, err)
	}
}

func TestFloatAndComplexCodecsPreserveSpecialBits(t *testing.T) {
	negativeZero32 := []byte{0x80, 0, 0, 0}
	nan32 := []byte{0x7f, 0x80, 0, 0x12} // Signaling NaN, which a float conversion can quiet.
	negativeZero64 := []byte{0x80, 0, 0, 0, 0, 0, 0, 0}
	nan64 := []byte{0x7f, 0xf0, 0, 0, 0, 0, 0, 0x12}
	checkBitRoundTrip(t, tarp.Float32Codec{}, negativeZero32)
	checkBitRoundTrip(t, tarp.Float32Codec{}, nan32)
	checkBitRoundTrip(t, tarp.Float64Codec{}, negativeZero64)
	checkBitRoundTrip(t, tarp.Float64Codec{}, nan64)
	checkBitRoundTrip(t, tarp.Complex64Codec{}, append(negativeZero32, nan32...))
	checkBitRoundTrip(t, tarp.Complex128Codec{}, append(negativeZero64, nan64...))
}

func TestNativeIntCodecsCheckHostWidth(t *testing.T) {
	wide := []byte{0, 0, 0, 1, 0, 0, 0, 0}
	negative := []byte{0xff, 0xff, 0xff, 0xfe, 0xff, 0xff, 0xff, 0xff}
	wantErr := strconv.IntSize == 32
	i, err := (tarp.IntCodec{}).Decode(wide)
	if (err != nil) != wantErr || (err == nil && int64(i) != 1<<32) {
		t.Errorf("int decoded 2^32 as %d, %v", i, err)
	}
	i, err = (tarp.IntCodec{}).Decode(negative)
	if (err != nil) != wantErr || (err == nil && int64(i) != -(1<<32)-1) {
		t.Errorf("int decoded -2^32-1 as %d, %v", i, err)
	}
	u, err := (tarp.UintCodec{}).Decode(wide)
	if (err != nil) != wantErr || (err == nil && uint64(u) != 1<<32) {
		t.Errorf("uint decoded 2^32 as %d, %v", u, err)
	}
	p, err := (tarp.UintptrCodec{}).Decode(wide)
	if (err != nil) != wantErr || (err == nil && uint64(p) != 1<<32) {
		t.Errorf("uintptr decoded 2^32 as %d, %v", p, err)
	}
}

func TestTimeCodecUsesWallClockInstant(t *testing.T) {
	codec := tarp.TimeCodec{}
	jst := time.FixedZone("JST", 9*60*60)
	for _, instant := range []time.Time{
		{},
		time.Date(2500, 1, 2, 3, 4, 5, 999999999, jst),
		time.Now(), // Includes a process-local monotonic reading.
	} {
		encoded, err := codec.Encode(instant)
		if err != nil {
			t.Fatal(err)
		}
		otherZone, err := codec.Encode(instant.In(jst))
		if err != nil || !bytes.Equal(encoded, otherZone) {
			t.Fatalf("same instant encoded differently: %x and %x, %v", encoded, otherZone, err)
		}
		got, err := codec.Decode(encoded)
		if err != nil || got != instant.UTC() {
			t.Fatalf("Decode(%x) = %v, %v; want %v", encoded, got, err, instant.UTC())
		}
	}
}

func TestTimeCodecRequiresCanonicalSecondsAndNanos(t *testing.T) {
	codec := tarp.TimeCodec{}
	t.Run("before epoch", fixedCodecCase(codec, time.Unix(-1, 123456789).UTC(), "ffffffffffffffff075bcd15"))
	data := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0x3b, 0x9a, 0xca, 0x00} // 1e9 nanos
	if got, err := codec.Decode(data); err == nil || !got.IsZero() {
		t.Fatalf("Decode(%x) = %v, %v; want zero time and an error", data, got, err)
	}
}

func TestDurationCodecUsesSignedNanoseconds(t *testing.T) {
	codec := tarp.DurationCodec{}
	t.Run("zero", fixedCodecCase(codec, time.Duration(0), "0000000000000000"))
	t.Run("negative", fixedCodecCase(codec, -2*time.Nanosecond, "fffffffffffffffe"))
	t.Run("minimum", fixedCodecCase(codec, time.Duration(math.MinInt64), "8000000000000000"))
	t.Run("maximum", fixedCodecCase(codec, time.Duration(math.MaxInt64), "7fffffffffffffff"))
}
