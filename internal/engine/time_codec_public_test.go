package engine_test

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

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
