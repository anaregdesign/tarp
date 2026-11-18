// Package composite derives codecs for types composed of registered scalar values.
package composite

import (
	"fmt"
	"reflect"
)

// Codec converts typed values to and from bytes.
type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

// New builds a codec for T. Its scalar lookup is only used during construction.
func New[T any](scalar func(reflect.Type) (ValueCodec, bool)) (Codec[T], error) {
	b := codecBuilder{schema: make(map[reflect.Type]*ValueCodec), scalar: scalar}
	c, err := b.build(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return derivedCodec[T]{c}, nil
}

// ValueCodec encodes and decodes a value of a schema at runtime.
type ValueCodec struct {
	encode func(reflect.Value) ([]byte, error)
	decode func([]byte, reflect.Value) error
	// Canonicalizing time.Time can merge distinct Go map keys on decode.
	normalizes bool
}

type derivedCodec[T any] struct{ codec ValueCodec }

func (c derivedCodec[T]) Encode(value T) ([]byte, error) {
	return c.codec.encode(reflect.ValueOf(value))
}

func (c derivedCodec[T]) Decode(data []byte) (T, error) {
	var value T
	if err := c.codec.decode(data, reflect.ValueOf(&value).Elem()); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}

// Bind adapts a typed scalar codec for use by New. Named types use the same
// bits as their underlying types, including signaling NaNs in complex64.
func Bind[T any](c Codec[T]) ValueCodec {
	ptr := reflect.TypeFor[*T]()
	return ValueCodec{
		encode: func(v reflect.Value) ([]byte, error) {
			if x, ok := v.Interface().(T); ok {
				return c.Encode(x)
			}
			// Pointer conversion preserves bits even for named numeric types.
			// MapRange keys and values need an addressable copy.
			src := reflect.New(v.Type())
			src.Elem().Set(v)
			return c.Encode(*src.Convert(ptr).Interface().(*T))
		},
		decode: func(data []byte, dst reflect.Value) error {
			x, err := c.Decode(data)
			if err != nil {
				return err
			}
			*dst.Addr().Convert(ptr).Interface().(*T) = x
			return nil
		},
	}
}

// Normalizing marks a scalar whose round trip can map distinct Go keys to equal values.
func (c ValueCodec) Normalizing() ValueCodec {
	c.normalizes = true
	return c
}

// A nil entry marks a type on the current recursion path. Completed codecs
// are shared between subtrees of this schema.
type codecBuilder struct {
	schema map[reflect.Type]*ValueCodec
	scalar func(reflect.Type) (ValueCodec, bool)
}

func (b codecBuilder) build(t reflect.Type) (ValueCodec, error) {
	if existing, ok := b.schema[t]; ok {
		if existing == nil {
			return ValueCodec{}, fmt.Errorf("tarp: recursive codec type %s", t)
		}
		return *existing, nil
	}
	b.schema[t] = nil
	c, err := b.buildType(t)
	if err == nil {
		b.schema[t] = &c
	}
	return c, err
}

func (b codecBuilder) buildType(t reflect.Type) (ValueCodec, error) {
	if c, ok := b.scalar(t); ok {
		return c, nil
	}
	switch t.Kind() {
	case reflect.Array, reflect.Slice:
		elem, err := b.build(t.Elem())
		if err != nil {
			return ValueCodec{}, err
		}
		return sequenceCodec(t, elem), nil
	case reflect.Map:
		key, err := b.build(t.Key())
		if err != nil {
			return ValueCodec{}, err
		}
		elem, err := b.build(t.Elem())
		if err != nil {
			return ValueCodec{}, err
		}
		return mapCodec(t, key, elem), nil
	case reflect.Struct:
		return b.structCodec(t)
	default:
		return ValueCodec{}, fmt.Errorf("tarp: unsupported codec type %s", t)
	}
}

func (b codecBuilder) structCodec(t reflect.Type) (ValueCodec, error) {
	fields := make([]ValueCodec, t.NumField())
	normalizes := false
	for i := range fields {
		field := t.Field(i)
		if !field.IsExported() {
			return ValueCodec{}, fmt.Errorf("tarp: codec field %s.%s is unexported", t, field.Name)
		}
		c, err := b.build(field.Type)
		if err != nil {
			return ValueCodec{}, fmt.Errorf("%s.%s: %w", t, field.Name, err)
		}
		fields[i] = c
		normalizes = normalizes || c.normalizes
	}
	return ValueCodec{
		normalizes: normalizes,
		encode: func(v reflect.Value) ([]byte, error) {
			var data []byte
			for i, field := range fields {
				payload, err := field.encode(v.Field(i))
				if err != nil {
					return nil, fmt.Errorf("%s.%s: %w", t, t.Field(i).Name, err)
				}
				data = appendFrame(data, payload)
			}
			return data, nil
		},
		decode: func(data []byte, dst reflect.Value) error {
			for i, field := range fields {
				payload, rest, err := takeFrame(data)
				if err == nil {
					err = field.decode(payload, dst.Field(i))
				}
				if err != nil {
					return fmt.Errorf("%s.%s: %w", t, t.Field(i).Name, err)
				}
				data = rest
			}
			if len(data) != 0 {
				return fmt.Errorf("tarp: trailing data for %s", t)
			}
			return nil
		},
	}, nil
}
