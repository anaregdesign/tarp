package tarp

import (
	"fmt"

	"github.com/anaregdesign/tarp/internal/engine"
)

// BatchCodecError identifies a failed codec call in a typed batch. Index is
// the zero-based position in the original input, including duplicate keys.
type BatchCodecError struct {
	Operation string // BatchGet, BatchPut or BatchDelete.
	Index     int
	Field     string // "key" or "value".
	Err       error
}

func (e *BatchCodecError) Error() string {
	return fmt.Sprintf("tarp: %s input[%d] %s: %v", e.Operation, e.Index, e.Field, e.Err)
}

func (e *BatchCodecError) Unwrap() error { return e.Err }

// TypedBatchResult holds a decoded BatchGet value. Missing keys have Found
// false and the zero value of V.
type TypedBatchResult[V any] struct {
	Value V
	Found bool
}

// BatchGet reads keys at one point in time, preserving input order. A codec
// or DB error discards the entire result. Only found values are decoded.
// MaxBatchGetValueBytes applies to encoded value bytes before decoding.
func (db *View[K, V]) BatchGet(keys []K) ([]TypedBatchResult[V], error) {
	if err := engine.CheckBatchCount(len(keys)); err != nil {
		return nil, err
	}
	// Only the call to the engine needs these slice headers. Keep a small
	// batch on the stack, including when the encoded bytes come from a codec.
	var encoded [][]byte
	const smallBatch = 32
	switch {
	case len(keys) <= 1:
		var one [1][]byte
		encoded = one[:len(keys)]
	case len(keys) <= smallBatch:
		var small [smallBatch][]byte
		encoded = small[:len(keys)]
	default:
		encoded = make([][]byte, len(keys))
	}
	for i, key := range keys {
		k, err := db.keyCodec.Encode(key)
		if err != nil {
			return nil, &BatchCodecError{Operation: "BatchGet", Index: i, Field: "key", Err: err}
		}
		encoded[i] = k
	}
	raw, err := db.core.BatchGet(encoded)
	if err != nil {
		return nil, err
	}
	results := make([]TypedBatchResult[V], len(raw))
	for i, r := range raw {
		if r.Found {
			value, err := db.valueCodec.Decode(r.Value)
			if err != nil {
				return nil, &BatchCodecError{Operation: "BatchGet", Index: i, Field: "value", Err: err}
			}
			results[i] = TypedBatchResult[V]{Value: value, Found: true}
		}
	}
	return results, nil
}

// BatchPut encodes the entire batch before its atomic commit. For repeated
// encoded keys the last value wins. DB.BatchPut limits apply to encoded bytes.
func (db *View[K, V]) BatchPut(entries []TypedEntry[K, V]) error {
	batch, err := (*engine.DB)(db.core).PreparePuts(len(entries), func(i int) (Entry, error) {
		entry := entries[i]
		key, err := db.keyCodec.Encode(entry.Key)
		if err != nil {
			return Entry{}, &BatchCodecError{Operation: "BatchPut", Index: i, Field: "key", Err: err}
		}
		value, err := db.valueCodec.Encode(entry.Value)
		if err != nil {
			return Entry{}, &BatchCodecError{Operation: "BatchPut", Index: i, Field: "value", Err: err}
		}
		return Entry{Key: key, Value: value}, nil
	})
	if err != nil {
		return err
	}
	return batch.Commit()
}

// BatchDelete atomically removes the distinct encoded keys. A missing key
// returns a BatchKeyError matching ErrNotFound before publication.
func (db *View[K, V]) BatchDelete(keys []K) error {
	batch, err := (*engine.DB)(db.core).PrepareDeletes(len(keys), func(i int) ([]byte, error) {
		key, err := db.keyCodec.Encode(keys[i])
		if err != nil {
			return nil, &BatchCodecError{Operation: "BatchDelete", Index: i, Field: "key", Err: err}
		}
		return key, nil
	})
	if err != nil {
		return err
	}
	return batch.Commit()
}
