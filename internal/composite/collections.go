package composite

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
)

func appendFrame(dst, payload []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(payload)))
	return append(dst, payload...)
}

func takeFrame(data []byte) (payload, rest []byte, err error) {
	size, n := binary.Uvarint(data)
	if n <= 0 || (n > 1 && data[n-1] == 0) {
		return nil, nil, errors.New("tarp: invalid frame length")
	}
	if size > uint64(len(data)-n) {
		return nil, nil, errors.New("tarp: truncated frame")
	}
	end := n + int(size)
	return data[n:end:end], data[end:], nil
}

// A nil payload denotes a nil slice or map. A present, empty collection has a
// non-nil, zero-length payload following its presence byte.
func collectionPayload(data []byte) ([]byte, error) {
	switch {
	case len(data) == 1 && data[0] == 0:
		return nil, nil
	case len(data) > 0 && data[0] == 1:
		return data[1:], nil
	default:
		return nil, errors.New("tarp: invalid collection presence byte or trailing data")
	}
}

func sequenceCodec(t reflect.Type, elem ValueCodec) ValueCodec {
	isSlice := t.Kind() == reflect.Slice
	isBytes := isSlice && t.Elem().Kind() == reflect.Uint8
	return ValueCodec{
		normalizes: !isSlice && elem.normalizes,
		encode: func(v reflect.Value) ([]byte, error) {
			var data []byte
			if isSlice {
				if v.IsNil() {
					return []byte{0}, nil
				}
				data = []byte{1}
				if isBytes {
					return append(data, v.Bytes()...), nil
				}
			}
			for i := 0; i < v.Len(); i++ {
				payload, err := elem.encode(v.Index(i))
				if err != nil {
					return nil, fmt.Errorf("element %d: %w", i, err)
				}
				data = appendFrame(data, payload)
			}
			return data, nil
		},
		decode: func(data []byte, dst reflect.Value) error {
			if isSlice {
				body, err := collectionPayload(data)
				if err != nil {
					return err
				}
				if body == nil {
					dst.SetZero()
					return nil
				}
				if isBytes {
					dst.SetBytes(bytes.Clone(body))
					return nil
				}
				return decodeSlice(body, dst, elem)
			}
			for i := 0; i < dst.Len(); i++ {
				payload, rest, err := takeFrame(data)
				if err != nil {
					return fmt.Errorf("element %d: %w", i, err)
				}
				if err := elem.decode(payload, dst.Index(i)); err != nil {
					return fmt.Errorf("element %d: %w", i, err)
				}
				data = rest
			}
			if len(data) != 0 {
				return errors.New("tarp: trailing array data")
			}
			return nil
		},
	}
}

func decodeSlice(data []byte, dst reflect.Value, elem ValueCodec) error {
	t := dst.Type()
	out := reflect.MakeSlice(t, 0, 0)
	for len(data) > 0 {
		payload, rest, err := takeFrame(data)
		if err != nil {
			return fmt.Errorf("element %d: %w", out.Len(), err)
		}
		value := reflect.New(t.Elem()).Elem()
		if err := elem.decode(payload, value); err != nil {
			return fmt.Errorf("element %d: %w", out.Len(), err)
		}
		out = reflect.Append(out, value)
		data = rest
	}
	dst.Set(out)
	return nil
}

func mapCodec(t reflect.Type, key, elem ValueCodec) ValueCodec {
	return ValueCodec{
		encode: func(v reflect.Value) ([]byte, error) {
			if v.IsNil() {
				return []byte{0}, nil
			}
			type encodedPair struct{ key, value []byte }
			pairs := make([]encodedPair, 0, v.Len())
			// Normalizing time fields can make distinct keys Go-equal, even
			// when other fields such as signed zeros keep their bytes distinct.
			var decodedKeys map[any]struct{}
			if key.normalizes && v.Len() > 1 {
				decodedKeys = make(map[any]struct{}, v.Len())
			}
			it := v.MapRange()
			for it.Next() {
				k, err := key.encode(it.Key())
				if err != nil {
					return nil, fmt.Errorf("map key: %w", err)
				}
				if decodedKeys != nil {
					decoded := reflect.New(t.Key()).Elem()
					if err := key.decode(k, decoded); err != nil {
						return nil, fmt.Errorf("map key: %w", err)
					}
					canonical := decoded.Interface()
					if _, ok := decodedKeys[canonical]; ok {
						return nil, errors.New("tarp: duplicate decoded map key")
					}
					decodedKeys[canonical] = struct{}{}
				}
				entry, err := elem.encode(it.Value())
				if err != nil {
					return nil, fmt.Errorf("map value: %w", err)
				}
				pairs = append(pairs, encodedPair{k, entry})
			}
			slices.SortFunc(pairs, func(a, b encodedPair) int { return bytes.Compare(a.key, b.key) })
			data := []byte{1}
			for i, p := range pairs {
				if i > 0 && bytes.Equal(p.key, pairs[i-1].key) {
					return nil, errors.New("tarp: duplicate encoded map key")
				}
				data = appendFrame(data, p.key)
				data = appendFrame(data, p.value)
			}
			return data, nil
		},
		decode: func(data []byte, dst reflect.Value) error {
			body, err := collectionPayload(data)
			if err != nil {
				return err
			}
			if body == nil {
				dst.SetZero()
				return nil
			}
			out := reflect.MakeMap(t)
			var previous []byte
			for len(body) > 0 {
				k, rest, err := takeFrame(body)
				if err != nil {
					return fmt.Errorf("map key: %w", err)
				}
				if previous != nil && bytes.Compare(previous, k) >= 0 {
					return errors.New("tarp: encoded map keys must increase")
				}
				entry, rest, err := takeFrame(rest)
				if err != nil {
					return fmt.Errorf("map value: %w", err)
				}
				mapKey := reflect.New(t.Key()).Elem()
				if err := key.decode(k, mapKey); err != nil {
					return fmt.Errorf("map key: %w", err)
				}
				if out.MapIndex(mapKey).IsValid() {
					return errors.New("tarp: duplicate decoded map key")
				}
				value := reflect.New(t.Elem()).Elem()
				if err := elem.decode(entry, value); err != nil {
					return fmt.Errorf("map value: %w", err)
				}
				out.SetMapIndex(mapKey, value)
				previous, body = k, rest
			}
			dst.Set(out)
			return nil
		},
	}
}
