package tarp

import (
	"fmt"

	"github.com/anaregdesign/tarp/internal/engine"
)

// BatchCodecError identifies a failed codec call in a typed batch. Index is
// the zero-based position in the original input, including duplicate keys.
type BatchCodecError struct {
	Operation string // BatchGet, BatchPut, BatchDelete or Apply.
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
func (db View[K, V]) BatchGet(keys []K) ([]TypedBatchResult[V], error) {
	return db.batchGet(keys, nil)
}

func (db *View[K, V]) batchGet(keys []K, snapshot *Snapshot) ([]TypedBatchResult[V], error) {
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
		k, err := db.encodeBatchKey("BatchGet", i, key)
		if err != nil {
			return nil, err
		}
		encoded[i] = k
	}
	var raw []BatchResult
	var err error
	if snapshot != nil {
		raw, err = (*engine.Snapshot)(snapshot).BatchGetPrepared(encoded)
	} else {
		raw, err = (*engine.DB)(db.core).BatchGetPrepared(encoded)
	}
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
func (db View[K, V]) BatchPut(entries []TypedEntry[K, V]) error {
	batch, err := (*engine.DB)(db.core).PreparePuts(len(entries), func(i int) (Entry, error) {
		entry := entries[i]
		key, err := db.encodeBatchKey("BatchPut", i, entry.Key)
		if err != nil {
			return Entry{}, err
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
func (db View[K, V]) BatchDelete(keys []K) error {
	batch, err := (*engine.DB)(db.core).PrepareDeletes(len(keys), func(i int) ([]byte, error) {
		return db.encodeBatchKey("BatchDelete", i, keys[i])
	})
	if err != nil {
		return err
	}
	return batch.Commit()
}

type typedWriteOp[K, V any] struct {
	key K
	val V
	del bool
}

// TypedWriteBatch queues typed Put and idempotent Delete operations for atomic
// publication via [View.Apply], including on a [TypedDB]. Its zero value is ready to use.
// Keys and values are encoded at Apply. Slices, maps, and pointers retain their
// underlying data until Reset. Copy mutable inputs before queueing them if they
// will be reused before Apply returns. Pass the batch by pointer after first use.
// During Apply, keep the batch unchanged, including in codec callbacks. Call
// Put, Delete, or Reset only after Apply returns.
type TypedWriteBatch[K, V any] struct {
	ops []typedWriteOp[K, V]
}

// Put queues a typed key-value upsert.
func (b *TypedWriteBatch[K, V]) Put(key K, value V) {
	b.ops = append(b.ops, typedWriteOp[K, V]{key: key, val: value})
}

// Delete queues an idempotent removal of key.
func (b *TypedWriteBatch[K, V]) Delete(key K) {
	b.ops = append(b.ops, typedWriteOp[K, V]{key: key, del: true})
}

// Len returns the number of queued operations before intra-batch coalescing.
func (b *TypedWriteBatch[K, V]) Len() int {
	if b == nil {
		return 0
	}
	return len(b.ops)
}

// Reset clears queued operations while retaining backing slice capacity for reuse.
func (b *TypedWriteBatch[K, V]) Reset() {
	if b == nil {
		return
	}
	clear(b.ops)
	b.ops = b.ops[:0]
}

// Apply encodes all queued operations and commits them atomically via [DB.Apply].
// Codec errors return a *BatchCodecError with Operation "Apply" before modifying DB state.
func (db View[K, V]) Apply(batch *TypedWriteBatch[K, V]) error {
	count := batch.Len()
	prepared, err := (*engine.DB)(db.core).PrepareMixed(count, func(i int) (engine.MixedOp, error) {
		op := batch.ops[i]
		key, err := db.encodeBatchKey("Apply", i, op.key)
		if err != nil {
			return engine.MixedOp{}, err
		}
		if op.del {
			return engine.MixedOp{Key: key, Delete: true}, nil
		}
		val, err := db.valueCodec.Encode(op.val)
		if err != nil {
			return engine.MixedOp{}, &BatchCodecError{Operation: "Apply", Index: i, Field: "value", Err: err}
		}
		return engine.MixedOp{Key: key, Value: val}, nil
	})
	if err != nil {
		return err
	}
	return prepared.Commit()
}

func (db *View[K, V]) encodeBatchKey(operation string, i int, key K) ([]byte, error) {
	encoded, err := db.keyCodec.Encode(key)
	if err != nil {
		return nil, &BatchCodecError{Operation: operation, Index: i, Field: "key", Err: err}
	}
	return encoded, engine.CheckBatchKey(operation, i, encoded)
}
