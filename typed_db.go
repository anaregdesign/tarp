package tarp

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
)

// TypedDB owns a DB opened by [OpenTyped] and provides the key, value, scan
// and batch operations of [View]. Call Close to release the database.
type TypedDB[K, V any] struct {
	boundView[K, V]
}

// Embed the view privately so the caller cannot replace an owner's DB.
type boundView[K, V any] = View[K, V]

// View provides live, typed access to the DB owned by its caller. Each read
// sees the committed state at that operation. Closing the owner ends access.
type View[K, V any] struct {
	core       *DB
	keyCodec   Codec[K]
	valueCodec Codec[V]
}

// OpenTyped opens a DB with the given options and non-nil codecs. The encoded
// bytes determine key identity. Use compatible codecs on reopen and after Restore.
func OpenTyped[K, V any](dir string, opts Options, keyCodec Codec[K], valueCodec Codec[V]) (*TypedDB[K, V], error) {
	if err := checkCodecs(keyCodec, valueCodec); err != nil {
		return nil, err
	}
	core, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &TypedDB[K, V]{View[K, V]{core, keyCodec, valueCodec}}, nil
}

// Bind associates non-nil codecs with an open DB. The typed view shares
// the live key and value state with the owner. The DB does not store the codecs with its data.
func Bind[K, V any](core *DB, keyCodec Codec[K], valueCodec Codec[V]) (*View[K, V], error) {
	if core == nil {
		return nil, errors.New("tarp: nil DB")
	}
	if err := checkCodecs(keyCodec, valueCodec); err != nil {
		return nil, err
	}
	return &View[K, V]{core, keyCodec, valueCodec}, nil
}

// View returns a non-owning handle that shares this database and its codecs.
// Closing the owner also ends access through the view.
func (db *TypedDB[K, V]) View() *View[K, V] {
	view := db.boundView
	return &view
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

func (db *View[K, V]) encodeKey(key K) ([]byte, error) {
	data, err := db.keyCodec.Encode(key)
	if err != nil {
		return nil, fmt.Errorf("tarp: encode key: %w", err)
	}
	return data, nil
}

func (db *View[K, V]) decodeValue(data []byte) (V, error) {
	value, err := db.valueCodec.Decode(data)
	if err != nil {
		var zero V
		return zero, fmt.Errorf("tarp: decode value: %w", err)
	}
	return value, nil
}

// Get returns the decoded value or the zero value of V and an error.
func (db *View[K, V]) Get(key K) (V, error) {
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
func (db *View[K, V]) Put(key K, value V) error {
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
func (db *View[K, V]) Delete(key K) error {
	k, err := db.encodeKey(key)
	if err != nil {
		return err
	}
	return db.core.Delete(k)
}

// TypedEntry pairs a key and value for BatchPut and Scan.
type TypedEntry[K, V any] struct {
	Key   K
	Value V
}

// Scan decodes each entry in a DB.Scan snapshot. The iterator retains the DB
// and codecs from this call. A DB or codec error yields one zero TypedEntry and
// ends iteration. Breaking early releases the snapshot.
func (db *View[K, V]) Scan(ctx context.Context) iter.Seq2[TypedEntry[K, V], error] {
	return db.ScanPrefix(ctx, nil)
}

// ScanPrefix decodes the entries selected by [DB.ScanPrefix]. The prefix is a
// prefix of the stored key bytes and does not need to encode a complete K.
func (db *View[K, V]) ScanPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[TypedEntry[K, V], error] {
	view := *db
	rows := view.core.ScanPrefix(ctx, encodedPrefix)
	return func(yield func(TypedEntry[K, V], error) bool) {
		view.decodeEntries(rows, yield)
	}
}

func (db *View[K, V]) decodeEntries(seq iter.Seq2[Entry, error], yield func(TypedEntry[K, V], error) bool) {
	for entry, err := range seq {
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

// Backup publishes a point-in-time copy of the underlying DB.
func (db *TypedDB[K, V]) Backup(ctx context.Context, destination string) error {
	return db.core.Backup(ctx, destination)
}

// CheckpointIndex persists the encoded-key index covering the captured catalog prefix.
func (db *TypedDB[K, V]) CheckpointIndex(ctx context.Context) error {
	return db.core.CheckpointIndex(ctx)
}

// ProtectParity seals the active DATA and completes protection for the underlying DB.
func (db *TypedDB[K, V]) ProtectParity(ctx context.Context) error {
	return db.core.ProtectParity(ctx)
}

// Compact builds a full DATA checkpoint of the underlying DB.
func (db *TypedDB[K, V]) Compact(ctx context.Context) (CompactResult, error) {
	return db.core.Compact(ctx)
}

// AutoCompactionStatus returns the underlying DB's automatic maintenance status.
func (db *TypedDB[K, V]) AutoCompactionStatus() AutoCompactionStatus {
	return db.core.AutoCompactionStatus()
}

// Close closes the underlying DB, including any other views of it.
func (db *TypedDB[K, V]) Close() error {
	return db.core.Close()
}
