package tarp

import (
	"fmt"

	"github.com/anaregdesign/tarp/internal/engine"
)

// TypedBatchResult holds a decoded BatchGet value. Missing keys have Found
// false and the zero value of V.
type TypedBatchResult[V any] struct {
	Value V
	Found bool
}

// BatchGet reads keys at one point in time, preserving input order. A codec
// or DB error discards the entire result. Only found values are decoded.
func (db *TypedDB[K, V]) BatchGet(keys []K) ([]TypedBatchResult[V], error) {
	if err := engine.CheckBatchCount(len(keys)); err != nil {
		return nil, err
	}
	// Only the call to the engine needs these slice headers. Keep a small
	// batch on the stack, including when the encoded bytes come from a codec.
	var small [32][]byte
	var encoded [][]byte
	if len(keys) <= len(small) {
		encoded = small[:len(keys)]
	} else {
		encoded = make([][]byte, len(keys))
	}
	for i, key := range keys {
		k, err := db.encodeKey(key)
		if err != nil {
			return nil, err
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
			value, err := db.decodeValue(r.Value)
			if err != nil {
				return nil, err
			}
			results[i] = TypedBatchResult[V]{Value: value, Found: true}
		}
	}
	return results, nil
}

// BatchPut encodes the entire batch before its atomic commit. For repeated
// encoded keys the last value wins. DB.BatchPut limits apply to encoded bytes.
func (db *TypedDB[K, V]) BatchPut(entries []TypedEntry[K, V]) error {
	if err := engine.CheckBatchCount(len(entries)); err != nil {
		return err
	}
	core := (*engine.DB)(db.core)
	encoded := make([]Entry, len(entries))
	var used int64
	for i, entry := range entries {
		key, err := db.encodeKey(entry.Key)
		if err != nil {
			return err
		}
		value, err := db.valueCodec.Encode(entry.Value)
		if err != nil {
			return fmt.Errorf("tarp: encode value: %w", err)
		}
		encoded[i] = Entry{Key: key, Value: value}
		used, err = core.CheckBatchPutEntry(encoded[i], used)
		if err != nil {
			return err
		}
	}
	return db.core.BatchPut(encoded)
}

// BatchDelete atomically removes the distinct encoded keys. A missing key
// returns ErrNotFound before publication.
func (db *TypedDB[K, V]) BatchDelete(keys []K) error {
	if err := engine.CheckBatchCount(len(keys)); err != nil {
		return err
	}
	core := (*engine.DB)(db.core)
	encoded := make([][]byte, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	var used int64
	for _, key := range keys {
		k, err := db.encodeKey(key)
		if err != nil {
			return err
		}
		if _, exists := seen[string(k)]; exists {
			continue
		}
		used, err = core.CheckBatchDeleteKey(k, used)
		if err != nil {
			return err
		}
		seen[string(k)] = struct{}{}
		encoded = append(encoded, k)
	}
	return db.core.BatchDelete(encoded)
}
