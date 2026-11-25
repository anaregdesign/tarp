package tarp

import (
	"fmt"
	"time"

	"github.com/anaregdesign/tarp/internal/orderkey"
)

// OrderedStringCodec stores a string with 0x00 escaped as 0x00 0xFF and
// terminated by 0x00 0x00, preserving Go's lexicographic string order both
// standalone and inside composite keys.
type OrderedStringCodec struct{}

func (OrderedStringCodec) Encode(value string) ([]byte, error) {
	return orderkey.AppendString(nil, value), nil
}

func (OrderedStringCodec) Decode(data []byte) (string, error) {
	value, rest, err := orderkey.ConsumeString(data)
	if err != nil {
		return "", err
	}
	if len(rest) != 0 {
		return "", fmt.Errorf("tarp: trailing data in ordered string (%d extra bytes)", len(rest))
	}
	return value, nil
}

// OrderedBytesCodec stores a byte slice with 0x00 escaped as 0x00 0xFF and
// terminated by 0x00 0x00, preserving bytes.Compare order. Both nil and empty
// slices encode to 0x00 0x00 and decode to a non-nil empty slice.
type OrderedBytesCodec struct{}

func (OrderedBytesCodec) Encode(value []byte) ([]byte, error) {
	return orderkey.AppendBytes(nil, value), nil
}

func (OrderedBytesCodec) Decode(data []byte) ([]byte, error) {
	value, rest, err := orderkey.ConsumeBytes(data)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("tarp: trailing data in ordered bytes (%d extra bytes)", len(rest))
	}
	return value, nil
}

// OrderedInt8Codec stores an int8 as one sign-flipped byte so bytewise order
// matches signed numeric order.
type OrderedInt8Codec struct{}

func (OrderedInt8Codec) Encode(value int8) ([]byte, error) {
	return orderkey.AppendInt8(nil, value), nil
}

func (OrderedInt8Codec) Decode(data []byte) (int8, error) {
	if err := checkCodecSize[int8](data, 1); err != nil {
		return 0, err
	}
	v, _, err := orderkey.ConsumeInt8(data)
	return v, err
}

// OrderedInt16Codec stores an int16 as two sign-flipped big-endian bytes so
// bytewise order matches signed numeric order.
type OrderedInt16Codec struct{}

func (OrderedInt16Codec) Encode(value int16) ([]byte, error) {
	return orderkey.AppendInt16(nil, value), nil
}

func (OrderedInt16Codec) Decode(data []byte) (int16, error) {
	if err := checkCodecSize[int16](data, 2); err != nil {
		return 0, err
	}
	v, _, err := orderkey.ConsumeInt16(data)
	return v, err
}

// OrderedInt32Codec stores an int32 as four sign-flipped big-endian bytes so
// bytewise order matches signed numeric order.
type OrderedInt32Codec struct{}

func (OrderedInt32Codec) Encode(value int32) ([]byte, error) {
	return orderkey.AppendInt32(nil, value), nil
}

func (OrderedInt32Codec) Decode(data []byte) (int32, error) {
	if err := checkCodecSize[int32](data, 4); err != nil {
		return 0, err
	}
	v, _, err := orderkey.ConsumeInt32(data)
	return v, err
}

// OrderedInt64Codec stores an int64 as eight sign-flipped big-endian bytes so
// bytewise order matches signed numeric order.
type OrderedInt64Codec struct{}

func (OrderedInt64Codec) Encode(value int64) ([]byte, error) {
	return orderkey.AppendInt64(nil, value), nil
}

func (OrderedInt64Codec) Decode(data []byte) (int64, error) {
	if err := checkCodecSize[int64](data, 8); err != nil {
		return 0, err
	}
	v, _, err := orderkey.ConsumeInt64(data)
	return v, err
}

// OrderedIntCodec stores an int as eight sign-flipped big-endian bytes so
// bytewise order matches signed numeric order. Decode rejects values outside
// the platform's int range.
type OrderedIntCodec struct{}

func (OrderedIntCodec) Encode(value int) ([]byte, error) {
	return orderkey.AppendInt(nil, value), nil
}

func (OrderedIntCodec) Decode(data []byte) (int, error) {
	if err := checkCodecSize[int](data, 8); err != nil {
		return 0, err
	}
	v, _, err := orderkey.ConsumeInt(data)
	return v, err
}

// OrderedFloat32Codec stores a float32 in four order-preserving bytes,
// ordering -Inf < negative < 0.0 < positive < +Inf. Encode canonicalizes -0.0
// to +0.0 and rejects NaN; Decode rejects NaN and non-canonical -0.0 bytes.
type OrderedFloat32Codec struct{}

func (OrderedFloat32Codec) Encode(value float32) ([]byte, error) {
	return orderkey.AppendFloat32(nil, value)
}

func (OrderedFloat32Codec) Decode(data []byte) (float32, error) {
	if err := checkCodecSize[float32](data, 4); err != nil {
		return 0, err
	}
	v, _, err := orderkey.ConsumeFloat32(data)
	return v, err
}

// OrderedFloat64Codec stores a float64 in eight order-preserving bytes,
// ordering -Inf < negative < 0.0 < positive < +Inf. Encode canonicalizes -0.0
// to +0.0 and rejects NaN; Decode rejects NaN and non-canonical -0.0 bytes.
type OrderedFloat64Codec struct{}

func (OrderedFloat64Codec) Encode(value float64) ([]byte, error) {
	return orderkey.AppendFloat64(nil, value)
}

func (OrderedFloat64Codec) Decode(data []byte) (float64, error) {
	if err := checkCodecSize[float64](data, 8); err != nil {
		return 0, err
	}
	v, _, err := orderkey.ConsumeFloat64(data)
	return v, err
}

// OrderedDurationCodec stores a time.Duration as eight sign-flipped big-endian
// nanosecond bytes so bytewise order matches duration order.
type OrderedDurationCodec struct{}

func (OrderedDurationCodec) Encode(value time.Duration) ([]byte, error) {
	return orderkey.AppendInt64(nil, int64(value)), nil
}

func (OrderedDurationCodec) Decode(data []byte) (time.Duration, error) {
	if err := checkCodecSize[time.Duration](data, 8); err != nil {
		return 0, err
	}
	nanos, _, err := orderkey.ConsumeInt64(data)
	return time.Duration(nanos), err
}

// OrderedTimeCodec stores a time.Time as eight sign-flipped big-endian Unix
// seconds followed by four unsigned big-endian nanosecond bytes, preserving
// chronological order across negative, zero, and positive Unix instants.
// Decode returns UTC.
type OrderedTimeCodec struct{}

func (OrderedTimeCodec) Encode(value time.Time) ([]byte, error) {
	return orderkey.AppendTime(nil, value), nil
}

func (OrderedTimeCodec) Decode(data []byte) (time.Time, error) {
	if err := checkCodecSize[time.Time](data, 12); err != nil {
		return time.Time{}, err
	}
	tm, _, err := orderkey.ConsumeTime(data)
	return tm, err
}

// NewOrderedCodec derives a byte-comparable Codec from T's Go type so that
// bytes.Compare(enc(a), enc(b)) matches T's natural order.
//
// Supported types are bool, string, []byte, [N]byte, int, int8, int16, int32,
// int64, uint, uint8, uint16, uint32, uint64, uintptr, float32, float64,
// time.Duration, time.Time, and non-recursive structs (including nested
// structs) of exported orderable fields in declaration order.
func NewOrderedCodec[T any]() (Codec[T], error) {
	return orderkey.New[T]()
}

// EncodeOrderedStringPrefix escapes 0x00 bytes in prefix as 0x00 0xFF without
// appending the 0x00 0x00 terminator, producing a byte prefix suitable for
// PrefixRange over OrderedStringCodec keys or the leading string field of a
// NewOrderedCodec struct key.
func EncodeOrderedStringPrefix(prefix string) []byte {
	return orderkey.EncodeStringPrefix(prefix)
}

// EncodeOrderedBytesPrefix escapes 0x00 bytes in prefix as 0x00 0xFF without
// appending the 0x00 0x00 terminator, producing a byte prefix suitable for
// PrefixRange over OrderedBytesCodec keys or the leading []byte field of a
// NewOrderedCodec struct key.
func EncodeOrderedBytesPrefix(prefix []byte) []byte {
	return orderkey.EncodeBytesPrefix(prefix)
}

// EncodeOrderedTuplePrefix encodes one or more complete leading struct fields
// using NewOrderedCodec's field encodings (including terminators for string
// and []byte fields), producing a byte prefix or range bound for composite
// struct keys.
func EncodeOrderedTuplePrefix(fields ...any) ([]byte, error) {
	return orderkey.EncodeTuplePrefix(fields...)
}
