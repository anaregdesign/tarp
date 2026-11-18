package tarp

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
)

// TypedDB reads and writes a DB using application-selected key and value codecs.
// The encoded bytes determine key identity. Reuse compatible codecs on reopen
// and after Restore. The database does not record which codecs wrote an entry.
type TypedDB[K, V any] struct {
	core       *DB
	keyCodec   Codec[K]
	valueCodec Codec[V]
}

// OpenTyped opens a DB with the given options and non-nil codecs. Close releases it.
func OpenTyped[K, V any](dir string, opts Options, keyCodec Codec[K], valueCodec Codec[V]) (*TypedDB[K, V], error) {
	if err := checkCodecs(keyCodec, valueCodec); err != nil {
		return nil, err
	}
	core, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &TypedDB[K, V]{core: core, keyCodec: keyCodec, valueCodec: valueCodec}, nil
}

// NewTypedDB wraps an open DB with non-nil codecs. Raw and typed operations
// share the same keys and values. Closing either handle closes both.
func NewTypedDB[K, V any](core *DB, keyCodec Codec[K], valueCodec Codec[V]) (*TypedDB[K, V], error) {
	if core == nil {
		return nil, errors.New("tarp: nil DB")
	}
	if err := checkCodecs(keyCodec, valueCodec); err != nil {
		return nil, err
	}
	return &TypedDB[K, V]{core: core, keyCodec: keyCodec, valueCodec: valueCodec}, nil
}

func checkCodecs[K, V any](keyCodec Codec[K], valueCodec Codec[V]) error {
	if nilCodec(keyCodec) {
		return errors.New("tarp: nil key codec")
	}
	if nilCodec(valueCodec) {
		return errors.New("tarp: nil value codec")
	}
	return nil
}

// A nil pointer assigned to a Codec still has a dynamic type.
func nilCodec[T any](codec Codec[T]) bool {
	v := reflect.ValueOf(codec)
	switch v.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (db *TypedDB[K, V]) encodeKey(key K) ([]byte, error) {
	data, err := db.keyCodec.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("tarp: encode key: %w", err)
	}
	return data, nil
}

func (db *TypedDB[K, V]) decodeValue(data []byte) (V, error) {
	value, err := db.valueCodec.Decode(data)
	if err != nil {
		var zero V
		return zero, fmt.Errorf("tarp: decode value: %w", err)
	}
	return value, nil
}

// Get returns the decoded value or the zero value of V and an error.
func (db *TypedDB[K, V]) Get(key K) (V, error) {
	var zero V
	k, err := db.encodeKey(key)
	if err != nil {
		return zero, err
	}
	data, err := db.core.Get(k)
	if err != nil {
		return zero, err
	}
	return db.decodeValue(data)
}

// Put durably inserts or replaces a value. Both encodings must succeed before
// the underlying DB receives the write.
func (db *TypedDB[K, V]) Put(key K, value V) error {
	k, err := db.encodeKey(key)
	if err != nil {
		return err
	}
	v, err := db.valueCodec.Encode(value)
	if err != nil {
		return fmt.Errorf("tarp: encode value: %w", err)
	}
	return db.core.Put(k, v)
}

// Delete durably removes a key. A missing key returns ErrNotFound.
func (db *TypedDB[K, V]) Delete(key K) error {
	k, err := db.encodeKey(key)
	if err != nil {
		return err
	}
	return db.core.Delete(k)
}

// TypedEntry contains the decoded key and value from a snapshot.
type TypedEntry[K, V any] struct {
	Key   K
	Value V
}

// Scan decodes each entry in a DB.Scan snapshot. A DB or codec error yields
// one zero TypedEntry and ends iteration. Breaking early releases the snapshot.
func (db *TypedDB[K, V]) Scan(ctx context.Context) iter.Seq2[TypedEntry[K, V], error] {
	return func(yield func(TypedEntry[K, V], error) bool) {
		for entry, err := range db.core.Scan(ctx) {
			if err != nil {
				yield(TypedEntry[K, V]{}, err)
				return
			}
			key, err := db.keyCodec.Decode(entry.Key)
			if err != nil {
				yield(TypedEntry[K, V]{}, fmt.Errorf("tarp: decode key: %w", err))
				return
			}
			value, err := db.decodeValue(entry.Value)
			if err != nil {
				yield(TypedEntry[K, V]{}, err)
				return
			}
			if !yield(TypedEntry[K, V]{Key: key, Value: value}, nil) {
				return
			}
		}
	}
}

// Backup publishes a point-in-time copy of the underlying DB.
func (db *TypedDB[K, V]) Backup(ctx context.Context, destination string) error {
	return db.core.Backup(ctx, destination)
}

// Compact builds an online checkpoint of the underlying DB.
func (db *TypedDB[K, V]) Compact(ctx context.Context) (CompactResult, error) {
	return db.core.Compact(ctx)
}

// MaintenanceStatus returns the underlying DB's automatic maintenance status.
func (db *TypedDB[K, V]) MaintenanceStatus() MaintenanceStatus {
	return db.core.MaintenanceStatus()
}

// Close closes the underlying DB, including any other views of it.
func (db *TypedDB[K, V]) Close() error {
	return db.core.Close()
}
