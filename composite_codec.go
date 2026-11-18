package tarp

import (
	"reflect"
	"time"

	"github.com/anaregdesign/tarp/internal/composite"
)

// NewCodec derives a codec from T's Go type. It supports the basic scalar
// types, time.Time, time.Duration, slices, arrays, maps, and structs of
// exported fields. Nested types must be supported and non-recursive.
//
// Struct fields are positional, in declaration order. Map entries are ordered
// by their encoded keys. Distinct map keys must have distinct encodings.
// Slices (including []byte) and maps distinguish nil from empty.
// Reuse the same field order and compatible types when reading stored data.
func NewCodec[T any]() (Codec[T], error) {
	return composite.New[T](scalarForType)
}

func scalarForType(t reflect.Type) (composite.ValueCodec, bool) {
	if c, ok := scalarByType[t]; ok {
		return c, true
	}
	c, ok := scalarByKind[t.Kind()]
	return c, ok
}

var scalarByType = map[reflect.Type]composite.ValueCodec{
	reflect.TypeFor[time.Time]():     composite.Bind(TimeCodec{}).Normalizing(),
	reflect.TypeFor[time.Duration](): composite.Bind(DurationCodec{}),
}

var scalarByKind = map[reflect.Kind]composite.ValueCodec{
	reflect.Bool:       composite.Bind(BoolCodec{}),
	reflect.String:     composite.Bind(StringCodec{}),
	reflect.Int:        composite.Bind(IntCodec{}),
	reflect.Int8:       composite.Bind(Int8Codec{}),
	reflect.Int16:      composite.Bind(Int16Codec{}),
	reflect.Int32:      composite.Bind(Int32Codec{}),
	reflect.Int64:      composite.Bind(Int64Codec{}),
	reflect.Uint:       composite.Bind(UintCodec{}),
	reflect.Uint8:      composite.Bind(Uint8Codec{}),
	reflect.Uint16:     composite.Bind(Uint16Codec{}),
	reflect.Uint32:     composite.Bind(Uint32Codec{}),
	reflect.Uint64:     composite.Bind(Uint64Codec{}),
	reflect.Uintptr:    composite.Bind(UintptrCodec{}),
	reflect.Float32:    composite.Bind(Float32Codec{}),
	reflect.Float64:    composite.Bind(Float64Codec{}),
	reflect.Complex64:  composite.Bind(Complex64Codec{}),
	reflect.Complex128: composite.Bind(Complex128Codec{}),
}
