package orderkey_test

import (
	"bytes"
	"cmp"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/internal/orderkey"
)

func assertOrderPreserved[T any](t *testing.T, codec orderkey.Codec[T], sorted []T, equal func(a, b T) bool) {
	t.Helper()
	encoded := make([][]byte, len(sorted))
	for i, v := range sorted {
		b, err := codec.Encode(v)
		if err != nil {
			t.Fatalf("Encode(%d: %v): %v", i, v, err)
		}
		got, err := codec.Decode(b)
		if err != nil {
			t.Fatalf("Decode(%d: %x): %v", i, b, err)
		}
		if !equal(got, v) {
			t.Fatalf("round-trip %d: got %#v, want %#v", i, got, v)
		}
		encoded[i] = b
	}
	for i := 0; i < len(encoded); i++ {
		for j := i + 1; j < len(encoded); j++ {
			if bytes.Compare(encoded[i], encoded[j]) >= 0 {
				t.Fatalf("order violation between %d (%#v -> %x) and %d (%#v -> %x)",
					i, sorted[i], encoded[i], j, sorted[j], encoded[j])
			}
		}
	}
}

func TestScalarOrderPreservation(t *testing.T) {
	boolC, err := orderkey.New[bool]()
	if err != nil {
		t.Fatal(err)
	}
	assertOrderPreserved(t, boolC, []bool{false, true}, func(a, b bool) bool { return a == b })

	i8C, _ := orderkey.New[int8]()
	assertOrderPreserved(t, i8C, []int8{math.MinInt8, -100, -1, 0, 1, 100, math.MaxInt8}, func(a, b int8) bool { return a == b })

	i16C, _ := orderkey.New[int16]()
	assertOrderPreserved(t, i16C, []int16{math.MinInt16, -256, -1, 0, 1, 256, math.MaxInt16}, func(a, b int16) bool { return a == b })

	i32C, _ := orderkey.New[int32]()
	assertOrderPreserved(t, i32C, []int32{math.MinInt32, -65536, -1, 0, 1, 65536, math.MaxInt32}, func(a, b int32) bool { return a == b })

	i64C, _ := orderkey.New[int64]()
	assertOrderPreserved(t, i64C, []int64{math.MinInt64, -1 << 40, -1, 0, 1, 1 << 40, math.MaxInt64}, func(a, b int64) bool { return a == b })

	intC, _ := orderkey.New[int]()
	assertOrderPreserved(t, intC, []int{math.MinInt, -999999, -1, 0, 1, 999999, math.MaxInt}, func(a, b int) bool { return a == b })

	u8C, _ := orderkey.New[uint8]()
	assertOrderPreserved(t, u8C, []uint8{0, 1, 127, 128, 255}, func(a, b uint8) bool { return a == b })

	u16C, _ := orderkey.New[uint16]()
	assertOrderPreserved(t, u16C, []uint16{0, 1, 255, 256, math.MaxUint16}, func(a, b uint16) bool { return a == b })

	u32C, _ := orderkey.New[uint32]()
	assertOrderPreserved(t, u32C, []uint32{0, 1, 65535, 65536, math.MaxUint32}, func(a, b uint32) bool { return a == b })

	u64C, _ := orderkey.New[uint64]()
	assertOrderPreserved(t, u64C, []uint64{0, 1, 1 << 32, math.MaxUint64}, func(a, b uint64) bool { return a == b })

	uintC, _ := orderkey.New[uint]()
	assertOrderPreserved(t, uintC, []uint{0, 1, 1024, math.MaxUint}, func(a, b uint) bool { return a == b })

	uptrC, _ := orderkey.New[uintptr]()
	assertOrderPreserved(t, uptrC, []uintptr{0, 1, 4096, ^uintptr(0)}, func(a, b uintptr) bool { return a == b })

	durC, _ := orderkey.New[time.Duration]()
	assertOrderPreserved(t, durC, []time.Duration{
		time.Duration(math.MinInt64),
		-24 * time.Hour,
		-time.Nanosecond,
		0,
		time.Nanosecond,
		24 * time.Hour,
		time.Duration(math.MaxInt64),
	}, func(a, b time.Duration) bool { return a == b })

	f32C, _ := orderkey.New[float32]()
	assertOrderPreserved(t, f32C, []float32{
		float32(math.Inf(-1)),
		-math.MaxFloat32,
		-1e10,
		-1.0,
		-math.SmallestNonzeroFloat32,
		0.0,
		math.SmallestNonzeroFloat32,
		1.0,
		1e10,
		math.MaxFloat32,
		float32(math.Inf(1)),
	}, func(a, b float32) bool { return a == b })

	f64C, _ := orderkey.New[float64]()
	assertOrderPreserved(t, f64C, []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1e100,
		-1.0,
		-math.SmallestNonzeroFloat64,
		0.0,
		math.SmallestNonzeroFloat64,
		1.0,
		1e100,
		math.MaxFloat64,
		math.Inf(1),
	}, func(a, b float64) bool { return a == b })

	timeC, _ := orderkey.New[time.Time]()
	zone := time.FixedZone("UTC+9", 9*3600)
	times := []time.Time{
		time.Unix(-1_000_000_000, 0).UTC(),
		time.Unix(-1, 0).UTC(),
		time.Unix(-1, 100_000_000).In(zone),
		time.Unix(-1, 999_999_999).UTC(),
		time.Unix(0, 0).UTC(),
		time.Unix(0, 1).UTC(),
		time.Unix(1_700_000_000, 500_000_000).In(zone),
		time.Unix(4_000_000_000, 999_999_999).UTC(),
	}
	assertOrderPreserved(t, timeC, times, func(a, b time.Time) bool {
		return a.Location() == time.UTC && a.Equal(b)
	})

	arrC, _ := orderkey.New[[4]byte]()
	assertOrderPreserved(t, arrC, [][4]byte{
		{0, 0, 0, 0},
		{0, 0, 0, 1},
		{0, 255, 0, 0},
		{255, 255, 255, 255},
	}, func(a, b [4]byte) bool { return a == b })
}

func TestStringAndBytesEscapingOrder(t *testing.T) {
	samples := []string{
		"",
		"\x00",
		"\x00\x00",
		"\x00\x00\x00",
		"\x00\x01",
		"\x00a",
		"\x00\xff",
		"\x00\xff\x00",
		"\x01",
		"a",
		"a\x00",
		"a\x00\x00",
		"a\x00b",
		"a\x01",
		"ab",
		"a\xff",
		"\xff",
		"\xff\x00",
		"\xff\xff",
	}
	slices.Sort(samples)
	samples = slices.Compact(samples)

	strC, err := orderkey.New[string]()
	if err != nil {
		t.Fatal(err)
	}
	assertOrderPreserved(t, strC, samples, func(a, b string) bool { return a == b })

	byteSamples := make([][]byte, len(samples))
	for i, s := range samples {
		byteSamples[i] = []byte(s)
	}
	bytesC, err := orderkey.New[[]byte]()
	if err != nil {
		t.Fatal(err)
	}
	assertOrderPreserved(t, bytesC, byteSamples, bytes.Equal)
}

func TestCompositeStructOrderAndPrefixes(t *testing.T) {
	type Inner struct {
		Scope string
		Rank  int32
	}
	type Composite struct {
		Header Inner
		Score  float64
		Stamp  time.Time
		Raw    []byte
		Tag    [2]byte
	}

	codec, err := orderkey.New[Composite]()
	if err != nil {
		t.Fatal(err)
	}

	t0 := time.Unix(-10, 500).UTC()
	t1 := time.Unix(0, 0).UTC()
	t2 := time.Unix(10, 999_999_999).UTC()

	items := []Composite{
		{Header: Inner{Scope: "", Rank: -5}, Score: math.Inf(-1), Stamp: t0, Raw: nil, Tag: [2]byte{0, 0}},
		{Header: Inner{Scope: "\x00", Rank: -5}, Score: -1.5, Stamp: t0, Raw: []byte{0}, Tag: [2]byte{0, 1}},
		{Header: Inner{Scope: "alpha", Rank: -10}, Score: 0, Stamp: t1, Raw: []byte("x\x00y"), Tag: [2]byte{1, 0}},
		{Header: Inner{Scope: "alpha", Rank: -10}, Score: 0, Stamp: t1, Raw: []byte("x\x00z"), Tag: [2]byte{0, 0}},
		{Header: Inner{Scope: "alpha", Rank: 0}, Score: -0.25, Stamp: t1, Raw: []byte{}, Tag: [2]byte{2, 3}},
		{Header: Inner{Scope: "alpha", Rank: 0}, Score: 0.0, Stamp: t0, Raw: []byte{}, Tag: [2]byte{2, 3}},
		{Header: Inner{Scope: "alpha", Rank: 0}, Score: 0.0, Stamp: t2, Raw: []byte{}, Tag: [2]byte{2, 3}},
		{Header: Inner{Scope: "alpha\x00beta", Rank: -100}, Score: 100, Stamp: t0, Raw: []byte{0xff}, Tag: [2]byte{9, 9}},
		{Header: Inner{Scope: "alphabet", Rank: -100}, Score: -100, Stamp: t0, Raw: []byte{1}, Tag: [2]byte{0, 0}},
	}
	slices.SortFunc(items, func(a, b Composite) int {
		if c := cmp.Compare(a.Header.Scope, b.Header.Scope); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Header.Rank, b.Header.Rank); c != 0 {
			return c
		}
		if c := cmp.Compare(a.Score, b.Score); c != 0 {
			return c
		}
		if c := a.Stamp.Compare(b.Stamp); c != 0 {
			return c
		}
		if c := bytes.Compare(a.Raw, b.Raw); c != 0 {
			return c
		}
		return bytes.Compare(a.Tag[:], b.Tag[:])
	})

	assertOrderPreserved(t, codec, items, func(a, b Composite) bool {
		return a.Header == b.Header && a.Score == b.Score && a.Stamp.Equal(b.Stamp) &&
			bytes.Equal(a.Raw, b.Raw) && a.Tag == b.Tag
	})

	// Verify string prefix matching on leading string field.
	strPfx := orderkey.EncodeStringPrefix("alpha")
	for _, item := range items {
		enc, err := codec.Encode(item)
		if err != nil {
			t.Fatal(err)
		}
		want := strings.HasPrefix(item.Header.Scope, "alpha")
		if got := bytes.HasPrefix(enc, strPfx); got != want {
			t.Fatalf("string prefix match for %q: got %v, want %v", item.Header.Scope, got, want)
		}
	}

	// Verify exact tuple prefix matching on ("alpha", int32(0)).
	tuplePfx, err := orderkey.EncodeTuplePrefix("alpha", int32(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		enc, err := codec.Encode(item)
		if err != nil {
			t.Fatal(err)
		}
		want := item.Header.Scope == "alpha" && item.Header.Rank == 0
		if got := bytes.HasPrefix(enc, tuplePfx); got != want {
			t.Fatalf("tuple prefix match for %+v: got %v, want %v", item.Header, got, want)
		}
	}
}

func TestFloatCanonicalZeroAndMalformedRejections(t *testing.T) {
	f64C, _ := orderkey.New[float64]()
	negZero := math.Copysign(0, -1)
	posEnc, err := f64C.Encode(0.0)
	if err != nil {
		t.Fatal(err)
	}
	negEnc, err := f64C.Encode(negZero)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(posEnc, negEnc) {
		t.Fatalf("-0.0 and +0.0 encodings differ: %x vs %x", negEnc, posEnc)
	}
	decoded, err := f64C.Decode(negEnc)
	if err != nil {
		t.Fatal(err)
	}
	if math.Signbit(decoded) {
		t.Fatalf("decoded zero retained negative signbit")
	}

	if _, err := f64C.Encode(math.NaN()); err == nil {
		t.Fatal("expected float64 NaN encode error")
	}
	// Non-canonical -0.0 encoding: 0x7fff_ffff_ffff_ffff.
	if _, err := f64C.Decode([]byte{0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("expected float64 non-canonical -0.0 decode error")
	}
	// Positive and negative NaN bit encodings.
	if _, err := f64C.Decode([]byte{0xff, 0xf8, 0, 0, 0, 0, 0, 1}); err == nil {
		t.Fatal("expected positive NaN decode error")
	}
	if _, err := f64C.Decode([]byte{0x00, 0x07, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe}); err == nil {
		t.Fatal("expected negative NaN decode error")
	}

	f32C, _ := orderkey.New[float32]()
	if _, err := f32C.Encode(float32(math.NaN())); err == nil {
		t.Fatal("expected float32 NaN encode error")
	}
	if _, err := f32C.Decode([]byte{0x7f, 0xff, 0xff, 0xff}); err == nil {
		t.Fatal("expected float32 non-canonical -0.0 decode error")
	}
	if _, err := f32C.Decode([]byte{0xff, 0xc0, 0, 1}); err == nil {
		t.Fatal("expected float32 NaN decode error")
	}

	strC, _ := orderkey.New[string]()
	for _, bad := range [][]byte{
		{},
		{0x00},
		{'a', 'b'},
		{'a', 0x00},
		{'a', 0x00, 0x01, 0x00, 0x00},
		{'a', 0x00, 0x00, 'x'},
	} {
		if _, err := strC.Decode(bad); err == nil {
			t.Fatalf("expected error decoding malformed string %x", bad)
		}
	}

	timeC, _ := orderkey.New[time.Time]()
	badTime := []byte{0x80, 0, 0, 0, 0, 0, 0, 0, 0x3b, 0x9a, 0xca, 0x00} // nanos == 1_000_000_000
	if _, err := timeC.Decode(badTime); err == nil {
		t.Fatal("expected invalid nanosecond decode error")
	}
}

func TestUnsupportedTypesRejected(t *testing.T) {
	type unexportedStruct struct {
		Public string
		hidden int
	}
	type recursiveStruct struct {
		Next *recursiveStruct
	}

	if _, err := orderkey.New[[]int](); err == nil {
		t.Fatal("expected []int to be rejected")
	}
	if _, err := orderkey.New[[2]int](); err == nil {
		t.Fatal("expected [2]int to be rejected")
	}
	if _, err := orderkey.New[map[string]int](); err == nil {
		t.Fatal("expected map to be rejected")
	}
	if _, err := orderkey.New[complex128](); err == nil {
		t.Fatal("expected complex128 to be rejected")
	}
	if _, err := orderkey.New[unexportedStruct](); err == nil {
		t.Fatal("expected struct with unexported field to be rejected")
	}
	if _, err := orderkey.New[recursiveStruct](); err == nil {
		t.Fatal("expected pointer field to be rejected")
	}
	if _, err := orderkey.EncodeTuplePrefix(nil); err == nil {
		t.Fatal("expected nil tuple field to be rejected")
	}
	if _, err := orderkey.EncodeTuplePrefix("ok", []int{1}); err == nil {
		t.Fatal("expected unsupported tuple field to be rejected")
	}
}
