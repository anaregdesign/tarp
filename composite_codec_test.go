package tarp_test

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
)

func mustDerive[T any](t testing.TB) tarp.Codec[T] {
	t.Helper()
	codec, err := tarp.NewCodec[T]()
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func TestCompositeCodecReopensNestedValues(t *testing.T) {
	type label string
	type key struct {
		Name   string
		Labels map[label]uint
	}
	type octet uint8
	type blob []octet
	type record struct {
		Start    time.Time
		Windows  map[label][][2]time.Duration
		Payloads []blob
		Labels   []map[label]int
	}
	dir := t.TempDir()
	open := func() *tarp.TypedDB[key, record] {
		db, err := tarp.OpenTyped(dir, tarp.Options{}, mustDerive[key](t), mustDerive[record](t))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
		})
		return db
	}
	k := key{"job", map[label]uint{"first": 1, "second": 2}}
	want := record{
		Start:    time.Date(1960, 1, 2, 3, 4, 5, 123, time.FixedZone("east", 3600)),
		Windows:  map[label][][2]time.Duration{"active": {{-time.Minute, time.Second}}, "unset": nil, "empty": {}},
		Payloads: []blob{nil, {}, {0xff, 0}},
		Labels:   []map[label]int{nil, {}, {"x": 1}},
	}
	db := open()
	if err := db.Put(k, want); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open()
	got, err := reopened.Get(key{"job", map[label]uint{"second": 2, "first": 1}})
	want.Start = want.Start.UTC()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("Get after reopen = %#v, %v; want %#v", got, err, want)
	}
}

func TestCompositeCodecMapWireOrder(t *testing.T) {
	codec := mustDerive[map[string]uint16](t)
	input := map[string]uint16{"z": 1, "aa": 0x1234, "": 0}
	want := []byte{
		1,
		0, 2, 0, 0,
		2, 'a', 'a', 2, 0x12, 0x34,
		1, 'z', 2, 0, 1,
	}
	for range 12 {
		got, err := codec.Encode(input)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("Encode = %x, %v; want %x", got, err, want)
		}
	}
	decoded, err := codec.Decode(want)
	if err != nil || !reflect.DeepEqual(decoded, input) {
		t.Fatalf("Decode = %#v, %v; want %#v", decoded, err, input)
	}
	for _, wire := range [][]byte{
		nil, {2}, {0, 0}, {1, 0}, {1, 0x80}, {1, 0x80, 0, 2, 0, 0},
		{1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 1},
		{1, 0, 1, 0},                          // Wrong uint16 width.
		{1, 0, 2, 0, 0, 0, 2, 0, 1},           // Duplicate empty key.
		{1, 1, 'z', 2, 0, 0, 1, 'a', 2, 0, 1}, // Out of order.
	} {
		if _, err := codec.Decode(wire); err == nil {
			t.Errorf("Decode accepted malformed map %x", wire)
		}
	}
}

func TestCompositeCodecMapKeyCollisions(t *testing.T) {
	times := mustDerive[map[time.Time]uint8](t)
	now := time.Unix(0, 1).UTC()
	keys := map[time.Time]uint8{now: 1, now.In(time.FixedZone("east", 3600)): 2}
	if _, err := times.Encode(keys); err == nil {
		t.Fatal("Encode accepted two keys for the same instant")
	}
	// Normalizing the time can also merge Go keys whose float encodings differ.
	type compositeKey struct {
		When time.Time
		Sign float64
	}
	composite := mustDerive[map[compositeKey]uint8](t)
	merged := map[compositeKey]uint8{
		{now, 0}: 1,
		{now.In(time.FixedZone("east", 3600)), math.Copysign(0, -1)}: 2,
	}
	if _, err := composite.Encode(merged); err == nil {
		t.Fatal("Encode accepted keys that collide on decode")
	}
	floats := mustDerive[map[float32]uint8](t)
	bothZeros := []byte{1, 4, 0, 0, 0, 0, 1, 1, 4, 0x80, 0, 0, 0, 1, 2}
	if _, err := floats.Decode(bothZeros); err == nil {
		t.Fatal("Decode accepted two Go-equal map keys")
	}
	nan := math.Float32frombits(0x7f800012)
	want := []byte{1, 4, 0x7f, 0x80, 0, 0x12, 1, 9}
	got, err := floats.Encode(map[float32]uint8{nan: 9})
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Encode of NaN key = %x, %v; want %x", got, err, want)
	}
	checkBitRoundTrip(t, floats, want)
}

func TestCompositeCodecRejectsNestedDuplicateNaNKeysBeforePut(t *testing.T) {
	type Reading struct {
		Series []map[float32]bool
	}
	key := math.Float32frombits(0x7fc00001)
	another := math.Float32frombits(0x7fc00002)
	codec := mustDerive[Reading](t)
	raw := openPublicDB(t, t.TempDir(), tarp.Options{})
	db, err := tarp.Bind(raw, tarp.StringCodec{}, codec)
	if err != nil {
		t.Fatal(err)
	}
	values := map[float32]bool{key: true, another: false}
	reading := Reading{Series: []map[float32]bool{values}}
	if err := db.Put("sensor", reading); err != nil {
		t.Fatal(err)
	}
	before, err := raw.Get([]byte("sensor"))
	if err != nil {
		t.Fatal(err)
	}
	// NaN is unequal to itself. This inserts a third Go key whose
	// encoded bytes match those of an existing key.
	values[key] = false
	if err := db.Put("sensor", reading); err == nil {
		t.Fatal("Put accepted two identical encoded map keys")
	}
	after, err := raw.Get([]byte("sensor"))
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("failed Put changed the stored reading: %x, %v; want %x", after, err, before)
	}
}

func TestCompositeCodecKeepsNilAndEmptyByteKeysDistinct(t *testing.T) {
	db, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, mustDerive[[]byte](t), tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cases := []struct {
		key   []byte
		value string
	}{{nil, "nil"}, {[]byte{}, "empty"}, {[]byte{0}, "zero"}}
	for _, tc := range cases {
		if err := db.Put(tc.key, tc.value); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range cases {
		if got, err := db.Get(tc.key); got != tc.value || err != nil {
			t.Errorf("Get(%#v) = %q, %v; want %q", tc.key, got, err, tc.value)
		}
	}
}

func TestCompositeCodecPreservesNamedComplex64Bits(t *testing.T) {
	type sample complex64
	// A conversion through complex128 would quiet the signaling NaN.
	wire := []byte{0x7f, 0x80, 0, 0x12, 0x80, 0, 0, 0}
	checkBitRoundTrip(t, mustDerive[sample](t), wire)
}

func TestCompositeCodecFramesStructFieldsAndArrayElements(t *testing.T) {
	type pair struct {
		Name    string
		Enabled [2]bool
	}
	codec := mustDerive[pair](t)
	value := pair{"ok", [2]bool{true, false}}
	want := []byte{2, 'o', 'k', 4, 1, 1, 1, 0}
	got, err := codec.Encode(value)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("Encode = %x, %v; want %x", got, err, want)
	}
	for _, wire := range [][]byte{
		want[:3], {0, 2, 1, 1}, {0, 4, 1, 1, 1, 2},
		{0, 5, 1, 1, 1, 0, 0}, append(bytes.Clone(want), 0),
	} {
		if _, err := codec.Decode(wire); err == nil {
			t.Errorf("Decode accepted malformed struct %x", wire)
		}
	}
}

func checkUnsupportedCodec[T any](t *testing.T) {
	t.Helper()
	if c, err := tarp.NewCodec[T](); err == nil || c != nil {
		t.Errorf("NewCodec[%v]() = %v, %v; want nil codec and error", reflect.TypeFor[T](), c, err)
	}
}

func TestCompositeCodecRejectsUnsupportedSchemas(t *testing.T) {
	type recursive map[string]recursive
	checkUnsupportedCodec[recursive](t)
	checkUnsupportedCodec[map[string][]*int](t)
	checkUnsupportedCodec[struct{ hidden int }](t)
	checkUnsupportedCodec[any](t)
}

func FuzzCompositeCodec(f *testing.F) {
	codec := mustDerive[map[string][]uint16](f)
	f.Add([]byte{0})
	f.Add([]byte{1})
	f.Add([]byte{1, 1, 'x', 4, 1, 2, 0x12, 0x34})
	f.Add([]byte{1, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		value, err := codec.Decode(data)
		if err != nil {
			return
		}
		got, err := codec.Encode(value)
		if err != nil || !bytes.Equal(data, got) {
			t.Fatalf("accepted %x, encoded back as %x, %v", data, got, err)
		}
	})
}

func ExampleNewCodec() {
	type Run struct {
		Started  time.Time
		Attempts []time.Duration
		Labels   map[string]string
	}
	codec, err := tarp.NewCodec[Run]()
	if err != nil {
		panic(err)
	}
	data, err := codec.Encode(Run{
		Started:  time.Date(2026, 11, 19, 9, 30, 0, 0, time.UTC),
		Attempts: []time.Duration{200 * time.Millisecond, 150 * time.Millisecond},
		Labels:   map[string]string{"job": "backup"},
	})
	if err != nil {
		panic(err)
	}
	run, err := codec.Decode(data)
	if err != nil {
		panic(err)
	}
	fmt.Println(run.Labels["job"], run.Started.Format(time.RFC3339), run.Attempts)
	// Output: backup 2026-11-19T09:30:00Z [200ms 150ms]
}
