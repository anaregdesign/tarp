package tarp

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
)

// TypedDB owns a DB opened by [OpenTyped]. Call the methods of [View] directly
// on the owner. For example, db.Get reads a single key ([View.Get]), db.Apply
// commits a mixed write batch ([View.Apply]), and db.Snapshot captures a
// reusable read view ([View.Snapshot]). Call [TypedDB.Close] to release the DB.
type TypedDB[K, V any] struct {
	boundView[K, V]
}

// Embed the view privately so the caller cannot replace an owner's DB.
type boundView[K, V any] = View[K, V]

// View borrows typed access to a DB. Its zero value is unbound. Obtain a usable
// View from [Bind] or [TypedDB.View]. Copies share the same live database.
// Each call retains its DB and codecs while user codec methods run. Closing
// the owner ends access.
type View[K, V any] struct {
	core       *DB
	keyCodec   Codec[K]
	valueCodec Codec[V]
}

// OpenTyped creates or reopens a DB for callers working with Go keys and values.
// The returned [TypedDB] owns the directory. To add typed access to a DB the
// caller already owns, use [Bind]. Supply non-nil codecs and reuse compatible
// codecs on reopen and after [Restore]. Encoded bytes determine key identity.
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

// Bind returns a [View] of an open DB using the supplied non-nil codecs.
// The DB does not store the codecs with its data.
func Bind[K, V any](core *DB, keyCodec Codec[K], valueCodec Codec[V]) (View[K, V], error) {
	if core == nil {
		return View[K, V]{}, errors.New("tarp: nil DB")
	}
	if err := checkCodecs(keyCodec, valueCodec); err != nil {
		return View[K, V]{}, err
	}
	return View[K, V]{core, keyCodec, valueCodec}, nil
}

// View returns a copy of the owner's typed binding.
// Closing the owner also ends access through the view.
func (db *TypedDB[K, V]) View() View[K, V] {
	return db.boundView
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

func decodeValue[V any](codec Codec[V], data []byte) (V, error) {
	value, err := codec.Decode(data)
	if err != nil {
		var zero V
		return zero, fmt.Errorf("tarp: decode value: %w", err)
	}
	return value, nil
}

// Get returns the decoded value or the zero value of V and an error.
func (db View[K, V]) Get(key K) (V, error) {
	return db.GetContext(context.Background(), key)
}

// GetContext reads and decodes the value of key, observing cancellation like
// [DB.GetContext]. User codec calls already in progress run to completion.
func (db View[K, V]) GetContext(ctx context.Context, key K) (V, error) {
	return db.getContext(ctx, key, nil)
}

func (db *View[K, V]) getContext(ctx context.Context, key K, snapshot *Snapshot) (V, error) {
	var zero V
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return zero, &OpError{Operation: "get", Phase: "context", Outcome: NotApplied, Err: err}
		}
	}
	k, err := db.encodeKey(key)
	if err != nil {
		return zero, err
	}
	var data []byte
	if snapshot == nil {
		data, err = db.core.GetContext(ctx, k)
	} else {
		data, err = snapshot.GetContext(ctx, k)
	}
	if err != nil {
		return zero, err
	}
	return decodeValue(db.valueCodec, data)
}

// Put durably inserts or replaces a value. Both encodings must succeed before
// the underlying DB receives the write.
func (db View[K, V]) Put(key K, value V) error {
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
func (db View[K, V]) Delete(key K) error {
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
func (db View[K, V]) Scan(ctx context.Context) iter.Seq2[TypedEntry[K, V], error] {
	return db.ScanPrefix(ctx, nil)
}

// ScanPrefix decodes the entries selected by [DB.ScanPrefix]. The prefix is a
// prefix of the stored key bytes and does not need to encode a complete K.
func (db View[K, V]) ScanPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[TypedEntry[K, V], error] {
	rows := db.core.ScanPrefix(ctx, encodedPrefix)
	return func(yield func(TypedEntry[K, V], error) bool) {
		db.decodeEntries(rows, yield)
	}
}

func (db View[K, V]) decodeEntries(seq iter.Seq2[Entry, error], yield func(TypedEntry[K, V], error) bool) {
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
		value, err := decodeValue(db.valueCodec, entry.Value)
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

// RefreshIndex advances the durable encoded-key index. See [DB.RefreshIndex].
func (db *TypedDB[K, V]) RefreshIndex(ctx context.Context) error {
	return db.core.RefreshIndex(ctx)
}

// ProtectParity seals the active DATA and completes protection for the underlying DB.
func (db *TypedDB[K, V]) ProtectParity(ctx context.Context) error {
	return db.core.ProtectParity(ctx)
}

// Compact builds a full DATA checkpoint of the underlying DB.
// [WithKeyOrder] orders records by their encoded key bytes.
func (db *TypedDB[K, V]) Compact(ctx context.Context, opts ...CompactOption) (CompactResult, error) {
	return db.core.Compact(ctx, opts...)
}

// AutoCompactionStatus returns the underlying DB's automatic maintenance status.
func (db *TypedDB[K, V]) AutoCompactionStatus() AutoCompactionStatus {
	return db.core.AutoCompactionStatus()
}

// Close closes the underlying DB, including any other views of it.
func (db *TypedDB[K, V]) Close() error {
	return db.core.Close()
}
