package composite

import (
	"errors"
	"reflect"
	"testing"
)

func TestNewReusesScalarCodecAndPreservesNestedErrors(t *testing.T) {
	type record struct {
		First byte
		Last  [1]byte
	}
	rejected := errors.New("reserved byte")
	scalar := ValueCodec{
		encode: func(v reflect.Value) ([]byte, error) {
			if v.Uint() == 0xff {
				return nil, rejected
			}
			return []byte{byte(v.Uint())}, nil
		},
		decode: func(data []byte, dst reflect.Value) error {
			if len(data) != 1 || data[0] == 0xff {
				return rejected
			}
			dst.SetUint(uint64(data[0]))
			return nil
		},
	}
	lookups := 0
	codec, err := New[record](func(typ reflect.Type) (ValueCodec, bool) {
		if typ != reflect.TypeFor[byte]() {
			return ValueCodec{}, false
		}
		lookups++
		return scalar, true
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := codec.Encode(record{7, [1]byte{0xff}}); got != nil || !errors.Is(err, rejected) {
		t.Errorf("Encode with invalid second field = %x, %v", got, err)
	}
	// The first field decodes successfully before the nested element fails.
	if got, err := codec.Decode([]byte{1, 7, 2, 1, 0xff}); got != (record{}) || !errors.Is(err, rejected) {
		t.Errorf("Decode with invalid second field = %+v, %v", got, err)
	}
	if lookups != 1 {
		t.Errorf("scalar used in two fields was looked up %d times, want 1", lookups)
	}
}
