package tarp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// Codec converts between Go values and the bytes stored in a DB. The DB
// compares encoded keys byte for byte, so key codecs must be deterministic.
// Use compatible codecs each time a database is opened.
//
// A codec used concurrently must support concurrent calls. Its results must
// stay valid across later calls. The built-in codecs have usable zero values
// and give the caller ownership of their results.
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}

// StringCodec stores the bytes of a string verbatim, including invalid UTF-8.
type StringCodec struct{}

func (StringCodec) Encode(value string) ([]byte, error) { return []byte(value), nil }
func (StringCodec) Decode(data []byte) (string, error)  { return string(data), nil }

// BytesCodec stores a byte slice verbatim. Encode and Decode each return a copy.
type BytesCodec struct{}

func (BytesCodec) Encode(value []byte) ([]byte, error) { return bytes.Clone(value), nil }
func (BytesCodec) Decode(data []byte) ([]byte, error)  { return bytes.Clone(data), nil }

// BoolCodec stores false as 0 and true as 1 in a single byte.
type BoolCodec struct{}

func (BoolCodec) Encode(value bool) ([]byte, error) {
	if value {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

func (BoolCodec) Decode(data []byte) (bool, error) {
	if err := checkCodecSize[bool](data, 1); err != nil {
		return false, err
	}
	if data[0] > 1 {
		return false, fmt.Errorf("tarp: invalid bool byte %#x", data[0])
	}
	return data[0] == 1, nil
}

// Int8Codec stores an int8 as one two's-complement byte.
type Int8Codec struct{}

func (Int8Codec) Encode(value int8) ([]byte, error) { return encodeNumber(value) }
func (Int8Codec) Decode(data []byte) (int8, error)  { return decodeNumber[int8](data) }

// Int16Codec stores an int16 as two big-endian two's-complement bytes.
type Int16Codec struct{}

func (Int16Codec) Encode(value int16) ([]byte, error) { return encodeNumber(value) }
func (Int16Codec) Decode(data []byte) (int16, error)  { return decodeNumber[int16](data) }

// Int32Codec stores an int32 (also called rune) as four big-endian two's-complement bytes.
type Int32Codec struct{}

func (Int32Codec) Encode(value int32) ([]byte, error) { return encodeNumber(value) }
func (Int32Codec) Decode(data []byte) (int32, error)  { return decodeNumber[int32](data) }

// Int64Codec stores an int64 as eight big-endian two's-complement bytes.
type Int64Codec struct{}

func (Int64Codec) Encode(value int64) ([]byte, error) { return encodeNumber(value) }
func (Int64Codec) Decode(data []byte) (int64, error)  { return decodeNumber[int64](data) }

// Uint8Codec stores a uint8 (also called byte) as one byte.
type Uint8Codec struct{}

func (Uint8Codec) Encode(value uint8) ([]byte, error) { return encodeNumber(value) }
func (Uint8Codec) Decode(data []byte) (uint8, error)  { return decodeNumber[uint8](data) }

// Uint16Codec stores a uint16 as two big-endian bytes.
type Uint16Codec struct{}

func (Uint16Codec) Encode(value uint16) ([]byte, error) { return encodeNumber(value) }
func (Uint16Codec) Decode(data []byte) (uint16, error)  { return decodeNumber[uint16](data) }

// Uint32Codec stores a uint32 as four big-endian bytes.
type Uint32Codec struct{}

func (Uint32Codec) Encode(value uint32) ([]byte, error) { return encodeNumber(value) }
func (Uint32Codec) Decode(data []byte) (uint32, error)  { return decodeNumber[uint32](data) }

// Uint64Codec stores a uint64 as eight big-endian bytes.
type Uint64Codec struct{}

func (Uint64Codec) Encode(value uint64) ([]byte, error) { return encodeNumber(value) }
func (Uint64Codec) Decode(data []byte) (uint64, error)  { return decodeNumber[uint64](data) }

// Float32Codec stores the IEEE 754 bit pattern of a float32 in four big-endian bytes.
// As keys, signed zeros and distinct NaN bit patterns retain separate identities.
type Float32Codec struct{}

func (Float32Codec) Encode(value float32) ([]byte, error) { return encodeNumber(value) }
func (Float32Codec) Decode(data []byte) (float32, error)  { return decodeNumber[float32](data) }

// Float64Codec stores the IEEE 754 bit pattern of a float64 in eight big-endian bytes.
// As keys, signed zeros and distinct NaN bit patterns retain separate identities.
type Float64Codec struct{}

func (Float64Codec) Encode(value float64) ([]byte, error) { return encodeNumber(value) }
func (Float64Codec) Decode(data []byte) (float64, error)  { return decodeNumber[float64](data) }

type fixedNumber interface {
	int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64 | float32 | float64
}

func encodeNumber[T fixedNumber](value T) ([]byte, error) {
	return binary.Append(nil, binary.BigEndian, value)
}

func decodeNumber[T fixedNumber](data []byte) (T, error) {
	var value T
	if err := checkCodecSize[T](data, binary.Size(value)); err != nil {
		return value, err
	}
	_, err := binary.Decode(data, binary.BigEndian, &value)
	return value, err
}

// IntCodec stores an int as eight big-endian two's-complement bytes.
// Decode rejects values outside the platform's int range.
type IntCodec struct{}

func (IntCodec) Encode(value int) ([]byte, error) { return encodeWord(value) }
func (IntCodec) Decode(data []byte) (int, error)  { return decodeWord[int](data) }

// UintCodec stores a uint as eight big-endian bytes.
// Decode rejects values outside the platform's uint range.
type UintCodec struct{}

func (UintCodec) Encode(value uint) ([]byte, error) { return encodeWord(value) }
func (UintCodec) Decode(data []byte) (uint, error)  { return decodeWord[uint](data) }

// UintptrCodec stores the numeric value of a uintptr as eight big-endian bytes.
// Decode rejects values outside the platform's uintptr range.
type UintptrCodec struct{}

func (UintptrCodec) Encode(value uintptr) ([]byte, error) { return encodeWord(value) }
func (UintptrCodec) Decode(data []byte) (uintptr, error)  { return decodeWord[uintptr](data) }

func encodeWord[T int | uint | uintptr](value T) ([]byte, error) {
	return binary.BigEndian.AppendUint64(nil, uint64(value)), nil
}

func decodeWord[T int | uint | uintptr](data []byte) (T, error) {
	if err := checkCodecSize[T](data, 8); err != nil {
		return 0, err
	}
	bits := binary.BigEndian.Uint64(data)
	value := T(bits)
	if uint64(value) != bits {
		return 0, fmt.Errorf("tarp: encoded integer %#x overflows %T", bits, value)
	}
	return value, nil
}

// Complex64Codec stores the real and imaginary parts of a complex64, in order,
// each in the same bit-preserving encoding as Float32Codec.
type Complex64Codec struct{}

func (Complex64Codec) Encode(value complex64) ([]byte, error) {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data, math.Float32bits(real(value)))
	binary.BigEndian.PutUint32(data[4:], math.Float32bits(imag(value)))
	return data, nil
}

func (Complex64Codec) Decode(data []byte) (complex64, error) {
	if err := checkCodecSize[complex64](data, 8); err != nil {
		return 0, err
	}
	re := math.Float32frombits(binary.BigEndian.Uint32(data))
	im := math.Float32frombits(binary.BigEndian.Uint32(data[4:]))
	return complex(re, im), nil
}

// Complex128Codec stores the real and imaginary parts of a complex128, in order,
// each in the same bit-preserving encoding as Float64Codec.
type Complex128Codec struct{}

func (Complex128Codec) Encode(value complex128) ([]byte, error) {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data, math.Float64bits(real(value)))
	binary.BigEndian.PutUint64(data[8:], math.Float64bits(imag(value)))
	return data, nil
}

func (Complex128Codec) Decode(data []byte) (complex128, error) {
	if err := checkCodecSize[complex128](data, 16); err != nil {
		return 0, err
	}
	re := math.Float64frombits(binary.BigEndian.Uint64(data))
	im := math.Float64frombits(binary.BigEndian.Uint64(data[8:]))
	return complex(re, im), nil
}

func checkCodecSize[T any](data []byte, want int) error {
	if len(data) != want {
		var value T
		return fmt.Errorf("tarp: invalid %T length: got %d bytes, want %d", value, len(data), want)
	}
	return nil
}
