// Package orderkey encodes Go scalars and flat or nested structs into
// lexicographically byte-comparable keys.
package orderkey

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"
)

// Codec converts typed values to and from byte-comparable keys.
type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

const (
	signBit8  = uint8(1) << 7
	signBit16 = uint16(1) << 15
	signBit32 = uint32(1) << 31
	signBit64 = uint64(1) << 63
)

var (
	timeType     = reflect.TypeFor[time.Time]()
	durationType = reflect.TypeFor[time.Duration]()
)

// AppendBool appends an order-preserving 1-byte boolean encoding.
func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// ConsumeBool decodes one boolean byte and returns the remaining slice.
func ConsumeBool(data []byte) (bool, []byte, error) {
	if len(data) < 1 {
		return false, nil, fmt.Errorf("tarp: truncated ordered bool: got %d bytes, need 1", len(data))
	}
	if data[0] > 1 {
		return false, nil, fmt.Errorf("tarp: invalid bool byte %#x", data[0])
	}
	return data[0] == 1, data[1:], nil
}

// AppendInt8 appends a sign-flipped 1-byte signed integer.
func AppendInt8(dst []byte, v int8) []byte {
	return append(dst, uint8(v)^signBit8)
}

// ConsumeInt8 decodes one sign-flipped int8 byte and returns the remaining slice.
func ConsumeInt8(data []byte) (int8, []byte, error) {
	if len(data) < 1 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered int8: got %d bytes, need 1", len(data))
	}
	return int8(data[0] ^ signBit8), data[1:], nil
}

// AppendInt16 appends a sign-flipped big-endian int16.
func AppendInt16(dst []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(dst, uint16(v)^signBit16)
}

// ConsumeInt16 decodes a sign-flipped big-endian int16 and returns the remaining slice.
func ConsumeInt16(data []byte) (int16, []byte, error) {
	if len(data) < 2 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered int16: got %d bytes, need 2", len(data))
	}
	return int16(binary.BigEndian.Uint16(data[:2]) ^ signBit16), data[2:], nil
}

// AppendInt32 appends a sign-flipped big-endian int32.
func AppendInt32(dst []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(dst, uint32(v)^signBit32)
}

// ConsumeInt32 decodes a sign-flipped big-endian int32 and returns the remaining slice.
func ConsumeInt32(data []byte) (int32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered int32: got %d bytes, need 4", len(data))
	}
	return int32(binary.BigEndian.Uint32(data[:4]) ^ signBit32), data[4:], nil
}

// AppendInt64 appends a sign-flipped big-endian int64.
func AppendInt64(dst []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(v)^signBit64)
}

// ConsumeInt64 decodes a sign-flipped big-endian int64 and returns the remaining slice.
func ConsumeInt64(data []byte) (int64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered int64: got %d bytes, need 8", len(data))
	}
	return int64(binary.BigEndian.Uint64(data[:8]) ^ signBit64), data[8:], nil
}

// AppendInt appends an int as an 8-byte sign-flipped big-endian integer.
func AppendInt(dst []byte, v int) []byte {
	return AppendInt64(dst, int64(v))
}

// ConsumeInt decodes an 8-byte sign-flipped big-endian int, rejecting values
// outside the platform int range.
func ConsumeInt(data []byte) (int, []byte, error) {
	v64, rest, err := ConsumeInt64(data)
	if err != nil {
		return 0, nil, err
	}
	v := int(v64)
	if int64(v) != v64 {
		return 0, nil, fmt.Errorf("tarp: encoded ordered integer %d overflows int", v64)
	}
	return v, rest, nil
}

// AppendUint8 appends a 1-byte uint8.
func AppendUint8(dst []byte, v uint8) []byte {
	return append(dst, v)
}

// ConsumeUint8 decodes a 1-byte uint8 and returns the remaining slice.
func ConsumeUint8(data []byte) (uint8, []byte, error) {
	if len(data) < 1 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered uint8: got %d bytes, need 1", len(data))
	}
	return data[0], data[1:], nil
}

// AppendUint16 appends a big-endian uint16.
func AppendUint16(dst []byte, v uint16) []byte {
	return binary.BigEndian.AppendUint16(dst, v)
}

// ConsumeUint16 decodes a big-endian uint16 and returns the remaining slice.
func ConsumeUint16(data []byte) (uint16, []byte, error) {
	if len(data) < 2 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered uint16: got %d bytes, need 2", len(data))
	}
	return binary.BigEndian.Uint16(data[:2]), data[2:], nil
}

// AppendUint32 appends a big-endian uint32.
func AppendUint32(dst []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, v)
}

// ConsumeUint32 decodes a big-endian uint32 and returns the remaining slice.
func ConsumeUint32(data []byte) (uint32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered uint32: got %d bytes, need 4", len(data))
	}
	return binary.BigEndian.Uint32(data[:4]), data[4:], nil
}

// AppendUint64 appends a big-endian uint64.
func AppendUint64(dst []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(dst, v)
}

// ConsumeUint64 decodes a big-endian uint64 and returns the remaining slice.
func ConsumeUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered uint64: got %d bytes, need 8", len(data))
	}
	return binary.BigEndian.Uint64(data[:8]), data[8:], nil
}

// AppendUint appends a uint as an 8-byte big-endian unsigned integer.
func AppendUint(dst []byte, v uint) []byte {
	return AppendUint64(dst, uint64(v))
}

// ConsumeUint decodes an 8-byte big-endian uint, rejecting values outside the
// platform uint range.
func ConsumeUint(data []byte) (uint, []byte, error) {
	u64, rest, err := ConsumeUint64(data)
	if err != nil {
		return 0, nil, err
	}
	v := uint(u64)
	if uint64(v) != u64 {
		return 0, nil, fmt.Errorf("tarp: encoded ordered integer %#x overflows uint", u64)
	}
	return v, rest, nil
}

// AppendUintptr appends a uintptr as an 8-byte big-endian unsigned integer.
func AppendUintptr(dst []byte, v uintptr) []byte {
	return AppendUint64(dst, uint64(v))
}

// ConsumeUintptr decodes an 8-byte big-endian uintptr, rejecting values
// outside the platform uintptr range.
func ConsumeUintptr(data []byte) (uintptr, []byte, error) {
	u64, rest, err := ConsumeUint64(data)
	if err != nil {
		return 0, nil, err
	}
	v := uintptr(u64)
	if uint64(v) != u64 {
		return 0, nil, fmt.Errorf("tarp: encoded ordered integer %#x overflows uintptr", u64)
	}
	return v, rest, nil
}

// AppendFloat32 appends an order-preserving 4-byte float32 encoding,
// canonicalizing -0.0 to +0.0 and rejecting NaN.
func AppendFloat32(dst []byte, v float32) ([]byte, error) {
	if math.IsNaN(float64(v)) {
		return nil, fmt.Errorf("tarp: ordered float32 codec rejects NaN")
	}
	var encoded uint32
	if v == 0 {
		encoded = signBit32
	} else if bits := math.Float32bits(v); bits&signBit32 != 0 {
		encoded = ^bits
	} else {
		encoded = bits | signBit32
	}
	return binary.BigEndian.AppendUint32(dst, encoded), nil
}

// ConsumeFloat32 decodes an order-preserving 4-byte float32, rejecting NaN
// and non-canonical negative zero encodings.
func ConsumeFloat32(data []byte) (float32, []byte, error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered float32: got %d bytes, need 4", len(data))
	}
	encoded := binary.BigEndian.Uint32(data[:4])
	if encoded == ^signBit32 {
		return 0, nil, fmt.Errorf("tarp: non-canonical negative zero in ordered float32")
	}
	var bits uint32
	if encoded&signBit32 != 0 {
		bits = encoded &^ signBit32
	} else {
		bits = ^encoded
	}
	v := math.Float32frombits(bits)
	if math.IsNaN(float64(v)) {
		return 0, nil, fmt.Errorf("tarp: ordered float32 codec rejects NaN")
	}
	return v, data[4:], nil
}

// AppendFloat64 appends an order-preserving 8-byte float64 encoding,
// canonicalizing -0.0 to +0.0 and rejecting NaN.
func AppendFloat64(dst []byte, v float64) ([]byte, error) {
	if math.IsNaN(v) {
		return nil, fmt.Errorf("tarp: ordered float64 codec rejects NaN")
	}
	var encoded uint64
	if v == 0 {
		encoded = signBit64
	} else if bits := math.Float64bits(v); bits&signBit64 != 0 {
		encoded = ^bits
	} else {
		encoded = bits | signBit64
	}
	return binary.BigEndian.AppendUint64(dst, encoded), nil
}

// ConsumeFloat64 decodes an order-preserving 8-byte float64, rejecting NaN
// and non-canonical negative zero encodings.
func ConsumeFloat64(data []byte) (float64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, fmt.Errorf("tarp: truncated ordered float64: got %d bytes, need 8", len(data))
	}
	encoded := binary.BigEndian.Uint64(data[:8])
	if encoded == ^signBit64 {
		return 0, nil, fmt.Errorf("tarp: non-canonical negative zero in ordered float64")
	}
	var bits uint64
	if encoded&signBit64 != 0 {
		bits = encoded &^ signBit64
	} else {
		bits = ^encoded
	}
	v := math.Float64frombits(bits)
	if math.IsNaN(v) {
		return 0, nil, fmt.Errorf("tarp: ordered float64 codec rejects NaN")
	}
	return v, data[8:], nil
}

// AppendTime appends a 12-byte order-preserving instant (8-byte sign-flipped
// big-endian Unix seconds followed by 4-byte unsigned nanoseconds).
func AppendTime(dst []byte, v time.Time) []byte {
	dst = AppendInt64(dst, v.Unix())
	return binary.BigEndian.AppendUint32(dst, uint32(v.Nanosecond()))
}

// ConsumeTime decodes a 12-byte order-preserving instant in UTC.
func ConsumeTime(data []byte) (time.Time, []byte, error) {
	if len(data) < 12 {
		return time.Time{}, nil, fmt.Errorf("tarp: truncated ordered time.Time: got %d bytes, need 12", len(data))
	}
	sec, _, _ := ConsumeInt64(data[:8])
	nanos := binary.BigEndian.Uint32(data[8:12])
	if nanos >= 1_000_000_000 {
		return time.Time{}, nil, fmt.Errorf("tarp: invalid nanosecond within second: %d", nanos)
	}
	return time.Unix(sec, int64(nanos)).UTC(), data[12:], nil
}

// AppendString appends s with 0x00 escaped as 0x00 0xFF and terminated by 0x00 0x00.
func AppendString(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == 0 {
			dst = append(dst, 0x00, 0xff)
		} else {
			dst = append(dst, b)
		}
	}
	return append(dst, 0x00, 0x00)
}

// AppendBytes appends src with 0x00 escaped as 0x00 0xFF and terminated by 0x00 0x00.
func AppendBytes(dst, src []byte) []byte {
	for _, b := range src {
		if b == 0 {
			dst = append(dst, 0x00, 0xff)
		} else {
			dst = append(dst, b)
		}
	}
	return append(dst, 0x00, 0x00)
}

// ConsumeBytes decodes one escaped, 0x00 0x00-terminated byte sequence and
// returns a newly allocated slice alongside the unconsumed tail.
func ConsumeBytes(data []byte) ([]byte, []byte, error) {
	end := bytes.Index(data, []byte{0, 0})
	if end < 0 {
		return nil, nil, fmt.Errorf("tarp: unterminated ordered byte sequence")
	}
	field := data[:end]
	out := make([]byte, 0, len(field))
	for {
		i := bytes.IndexByte(field, 0)
		if i < 0 {
			return append(out, field...), data[end+2:], nil
		}
		// A zero at the end of field would have started the terminator.
		if field[i+1] != 0xff {
			return nil, nil, fmt.Errorf("tarp: invalid ordered escape sequence 0x00 %#02x", field[i+1])
		}
		out = append(out, field[:i+1]...)
		field = field[i+2:]
	}
}

// ConsumeString decodes one escaped, 0x00 0x00-terminated string and returns
// the unconsumed tail.
func ConsumeString(data []byte) (string, []byte, error) {
	if len(data) < 2 {
		return "", nil, fmt.Errorf("tarp: unterminated ordered byte sequence")
	}
	if idx := bytes.IndexByte(data, 0x00); idx >= 0 && idx+1 < len(data) && data[idx+1] == 0x00 {
		return string(data[:idx]), data[idx+2:], nil
	}
	raw, rest, err := ConsumeBytes(data)
	if err != nil {
		return "", nil, err
	}
	return string(raw), rest, nil
}

// EncodeStringPrefix escapes 0x00 bytes in prefix as 0x00 0xFF without
// appending the 0x00 0x00 terminator.
func EncodeStringPrefix(prefix string) []byte {
	if len(prefix) == 0 {
		return nil
	}
	zeros := 0
	for i := 0; i < len(prefix); i++ {
		if prefix[i] == 0 {
			zeros++
		}
	}
	out := make([]byte, 0, len(prefix)+zeros)
	for i := 0; i < len(prefix); i++ {
		b := prefix[i]
		if b == 0 {
			out = append(out, 0x00, 0xff)
		} else {
			out = append(out, b)
		}
	}
	return out
}

// EncodeBytesPrefix escapes 0x00 bytes in prefix as 0x00 0xFF without
// appending the 0x00 0x00 terminator.
func EncodeBytesPrefix(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	zeros := bytes.Count(prefix, []byte{0})
	out := make([]byte, 0, len(prefix)+zeros)
	for _, b := range prefix {
		if b == 0 {
			out = append(out, 0x00, 0xff)
		} else {
			out = append(out, b)
		}
	}
	return out
}

// EncodeTuplePrefix encodes one or more complete leading tuple/struct fields
// using their full ordered field representations (including string/[]byte
// terminators).
func EncodeTuplePrefix(fields ...any) ([]byte, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	b := builder{schema: make(map[reflect.Type]*valueCodec)}
	var out []byte
	for i, field := range fields {
		if field == nil {
			return nil, fmt.Errorf("tarp: ordered tuple field %d is nil", i)
		}
		v := reflect.ValueOf(field)
		c, err := b.build(v.Type())
		if err != nil {
			return nil, fmt.Errorf("field %d: %w", i, err)
		}
		out, err = c.appendVal(out, v)
		if err != nil {
			return nil, fmt.Errorf("field %d: %w", i, err)
		}
	}
	return out, nil
}

// New derives an order-preserving Codec for T.
func New[T any]() (Codec[T], error) {
	t := reflect.TypeFor[T]()
	b := builder{schema: make(map[reflect.Type]*valueCodec)}
	c, err := b.build(t)
	if err != nil {
		return nil, err
	}
	return derivedCodec[T]{typ: t, codec: c}, nil
}

type valueCodec struct {
	appendVal  func([]byte, reflect.Value) ([]byte, error)
	consumeVal func([]byte, reflect.Value) ([]byte, error)
}

type derivedCodec[T any] struct {
	typ   reflect.Type
	codec valueCodec
}

func (c derivedCodec[T]) Encode(value T) ([]byte, error) {
	out, err := c.codec.appendVal(nil, reflect.ValueOf(value))
	if err != nil {
		return nil, err
	}
	if out == nil {
		return []byte{}, nil
	}
	return out, nil
}

func (c derivedCodec[T]) Decode(data []byte) (T, error) {
	var value T
	rest, err := c.codec.consumeVal(data, reflect.ValueOf(&value).Elem())
	if err != nil {
		var zero T
		return zero, err
	}
	if len(rest) != 0 {
		var zero T
		return zero, fmt.Errorf("tarp: trailing data for ordered %s (%d extra bytes)", c.typ, len(rest))
	}
	return value, nil
}

type builder struct {
	schema map[reflect.Type]*valueCodec
}

func (b builder) build(t reflect.Type) (valueCodec, error) {
	if existing, ok := b.schema[t]; ok {
		if existing == nil {
			return valueCodec{}, fmt.Errorf("tarp: recursive ordered codec type %s", t)
		}
		return *existing, nil
	}
	b.schema[t] = nil
	c, err := b.buildType(t)
	if err == nil {
		b.schema[t] = &c
	} else {
		delete(b.schema, t)
	}
	return c, err
}

func (b builder) buildType(t reflect.Type) (valueCodec, error) {
	if t == timeType {
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendTime(dst, v.Interface().(time.Time)), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				tm, rest, err := ConsumeTime(data)
				if err != nil {
					return nil, err
				}
				dst.Set(reflect.ValueOf(tm))
				return rest, nil
			},
		}, nil
	}
	if t == durationType {
		return int64ValueCodec(), nil
	}

	switch t.Kind() {
	case reflect.Bool:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendBool(dst, v.Bool()), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeBool(data)
				if err != nil {
					return nil, err
				}
				dst.SetBool(val)
				return rest, nil
			},
		}, nil

	case reflect.String:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendString(dst, v.String()), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeString(data)
				if err != nil {
					return nil, err
				}
				dst.SetString(val)
				return rest, nil
			},
		}, nil

	case reflect.Int:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendInt(dst, int(v.Int())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeInt(data)
				if err != nil {
					return nil, err
				}
				dst.SetInt(int64(val))
				return rest, nil
			},
		}, nil

	case reflect.Int8:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendInt8(dst, int8(v.Int())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeInt8(data)
				if err != nil {
					return nil, err
				}
				dst.SetInt(int64(val))
				return rest, nil
			},
		}, nil

	case reflect.Int16:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendInt16(dst, int16(v.Int())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeInt16(data)
				if err != nil {
					return nil, err
				}
				dst.SetInt(int64(val))
				return rest, nil
			},
		}, nil

	case reflect.Int32:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendInt32(dst, int32(v.Int())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeInt32(data)
				if err != nil {
					return nil, err
				}
				dst.SetInt(int64(val))
				return rest, nil
			},
		}, nil

	case reflect.Int64:
		return int64ValueCodec(), nil

	case reflect.Uint:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendUint(dst, uint(v.Uint())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeUint(data)
				if err != nil {
					return nil, err
				}
				dst.SetUint(uint64(val))
				return rest, nil
			},
		}, nil

	case reflect.Uint8:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendUint8(dst, uint8(v.Uint())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeUint8(data)
				if err != nil {
					return nil, err
				}
				dst.SetUint(uint64(val))
				return rest, nil
			},
		}, nil

	case reflect.Uint16:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendUint16(dst, uint16(v.Uint())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeUint16(data)
				if err != nil {
					return nil, err
				}
				dst.SetUint(uint64(val))
				return rest, nil
			},
		}, nil

	case reflect.Uint32:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendUint32(dst, uint32(v.Uint())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeUint32(data)
				if err != nil {
					return nil, err
				}
				dst.SetUint(uint64(val))
				return rest, nil
			},
		}, nil

	case reflect.Uint64:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendUint64(dst, v.Uint()), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeUint64(data)
				if err != nil {
					return nil, err
				}
				dst.SetUint(val)
				return rest, nil
			},
		}, nil

	case reflect.Uintptr:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendUintptr(dst, uintptr(v.Uint())), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeUintptr(data)
				if err != nil {
					return nil, err
				}
				dst.SetUint(uint64(val))
				return rest, nil
			},
		}, nil

	case reflect.Float32:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendFloat32(dst, float32(v.Float()))
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeFloat32(data)
				if err != nil {
					return nil, err
				}
				dst.SetFloat(float64(val))
				return rest, nil
			},
		}, nil

	case reflect.Float64:
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendFloat64(dst, v.Float())
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				val, rest, err := ConsumeFloat64(data)
				if err != nil {
					return nil, err
				}
				dst.SetFloat(val)
				return rest, nil
			},
		}, nil

	case reflect.Slice:
		if t.Elem().Kind() != reflect.Uint8 {
			return valueCodec{}, fmt.Errorf("tarp: unsupported ordered codec type %s", t)
		}
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				return AppendBytes(dst, v.Bytes()), nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				raw, rest, err := ConsumeBytes(data)
				if err != nil {
					return nil, err
				}
				dst.SetBytes(raw)
				return rest, nil
			},
		}, nil

	case reflect.Array:
		if t.Elem().Kind() != reflect.Uint8 {
			return valueCodec{}, fmt.Errorf("tarp: unsupported ordered codec type %s", t)
		}
		n := t.Len()
		return valueCodec{
			appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
				dst = slices.Grow(dst, n)
				for i := range n {
					dst = append(dst, byte(v.Index(i).Uint()))
				}
				return dst, nil
			},
			consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
				if len(data) < n {
					return nil, fmt.Errorf("tarp: truncated ordered %s: got %d bytes, need %d", t, len(data), n)
				}
				for i := range n {
					dst.Index(i).SetUint(uint64(data[i]))
				}
				return data[n:], nil
			},
		}, nil

	case reflect.Struct:
		return b.structCodec(t)

	default:
		return valueCodec{}, fmt.Errorf("tarp: unsupported ordered codec type %s", t)
	}
}

func int64ValueCodec() valueCodec {
	return valueCodec{
		appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
			return AppendInt64(dst, v.Int()), nil
		},
		consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
			val, rest, err := ConsumeInt64(data)
			if err != nil {
				return nil, err
			}
			dst.SetInt(val)
			return rest, nil
		},
	}
}

func (b builder) structCodec(t reflect.Type) (valueCodec, error) {
	fields := make([]valueCodec, t.NumField())
	for i := range fields {
		field := t.Field(i)
		if !field.IsExported() {
			return valueCodec{}, fmt.Errorf("tarp: ordered codec field %s.%s is unexported", t, field.Name)
		}
		c, err := b.build(field.Type)
		if err != nil {
			return valueCodec{}, fmt.Errorf("%s.%s: %w", t, field.Name, err)
		}
		fields[i] = c
	}
	return valueCodec{
		appendVal: func(dst []byte, v reflect.Value) ([]byte, error) {
			var err error
			for i, field := range fields {
				dst, err = field.appendVal(dst, v.Field(i))
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", t, t.Field(i).Name, err)
				}
			}
			return dst, nil
		},
		consumeVal: func(data []byte, dst reflect.Value) ([]byte, error) {
			var err error
			for i, field := range fields {
				data, err = field.consumeVal(data, dst.Field(i))
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", t, t.Field(i).Name, err)
				}
			}
			return data, nil
		},
	}, nil
}
