package tarp

import "github.com/anaregdesign/tarp/internal/engine"

type writeBatchOp struct {
	keyOff int
	keyLen int
	valOff int
	valLen int
	del    bool
}

// WriteBatch queues Put and idempotent Delete operations for atomic publication
// via [DB.Apply]. Its zero value is ready to use. WriteBatch is not safe for
// concurrent mutation without external synchronization.
type WriteBatch struct {
	ops   []writeBatchOp
	arena []byte
}

// Put queues an upsert of key and value. Both slices are copied into internal
// contiguous storage immediately, so callers can reuse scratch buffers across calls.
func (b *WriteBatch) Put(key, value []byte) {
	keyOff := len(b.arena)
	b.arena = append(b.arena, key...)
	valOff := len(b.arena)
	b.arena = append(b.arena, value...)
	b.ops = append(b.ops, writeBatchOp{
		keyOff: keyOff,
		keyLen: len(key),
		valOff: valOff,
		valLen: len(value),
	})
}

// Delete queues an idempotent removal of key. Key is copied into internal
// storage immediately. Unlike [DB.Delete] and [DB.BatchDelete], deleting an
// absent key inside a WriteBatch succeeds as a no-op at commit time.
func (b *WriteBatch) Delete(key []byte) {
	keyOff := len(b.arena)
	b.arena = append(b.arena, key...)
	b.ops = append(b.ops, writeBatchOp{
		keyOff: keyOff,
		keyLen: len(key),
		del:    true,
	})
}

// Len returns the number of queued operations before intra-batch coalescing.
func (b *WriteBatch) Len() int {
	if b == nil {
		return 0
	}
	return len(b.ops)
}

// Reset clears all queued operations while retaining backing slice capacity for reuse.
func (b *WriteBatch) Reset() {
	if b == nil {
		return
	}
	b.ops = b.ops[:0]
	b.arena = b.arena[:0]
}

// Apply durably commits all operations in batch at one atomic commit frontier.
// Duplicate keys within the batch are folded in input order so the last
// operation per distinct key wins. Deleting a key that is absent at commit time
// is a no-op and writes no tombstone record. A nil or empty batch checks
// database admission and health without writing to DATA. The coalesced records
// must collectively fit in one DATA segment after its header.
func (db *DB) Apply(batch *WriteBatch) error {
	count := batch.Len()
	return (*engine.DB)(db).ApplyMixed(count, func(i int) engine.MixedOp {
		op := batch.ops[i]
		key := batch.arena[op.keyOff : op.keyOff+op.keyLen]
		if op.del {
			return engine.MixedOp{Key: key, Delete: true}
		}
		val := batch.arena[op.valOff : op.valOff+op.valLen]
		return engine.MixedOp{Key: key, Value: val}
	})
}
