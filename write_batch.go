package tarp

import (
	"slices"

	"github.com/anaregdesign/tarp/internal/engine"
)

type writeBatchOp struct {
	keyStart   int
	valueStart int
	end        int
	isDelete   bool
}

// WriteBatch queues Put and idempotent Delete operations for atomic publication
// via [DB.Apply]. Its zero value is ready to use. WriteBatch is not safe for
// concurrent mutation without external synchronization. Pass the batch by pointer
// after first use. During Apply, keep the batch unchanged. Call Put, Delete,
// or Reset only after Apply returns.
type WriteBatch struct {
	ops   []writeBatchOp
	arena []byte
}

// Put queues an upsert of key and value. Both slices are copied into internal
// contiguous storage immediately, so callers can reuse scratch buffers across calls.
func (b *WriteBatch) Put(key, value []byte) {
	b.appendOp(key, value, false)
}

// Delete queues an idempotent removal of key. Key is copied into internal
// storage immediately. Unlike [DB.Delete] and [DB.BatchDelete], deleting an
// absent key inside a WriteBatch succeeds as a no-op at commit time.
func (b *WriteBatch) Delete(key []byte) {
	b.appendOp(key, nil, true)
}

func (b *WriteBatch) appendOp(key, value []byte, isDelete bool) {
	keyStart := len(b.arena)
	b.arena = slices.Grow(b.arena, len(key)+len(value))
	b.arena = append(b.arena, key...)
	valueStart := len(b.arena)
	b.arena = append(b.arena, value...)
	b.ops = append(b.ops, writeBatchOp{
		keyStart:   keyStart,
		valueStart: valueStart,
		end:        len(b.arena),
		isDelete:   isDelete,
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
// Use it for transitions that must publish a new key and remove an old key
// together, or for deletes that should succeed when a key is already absent.
// [Options.MaxWriteBatchEntries] limits the total number of queued operations.
// Duplicate keys within the batch are folded in input order so the last
// operation per distinct key wins. Deleting a key that is absent at commit time
// is a no-op and consumes no segment space. The remaining records must fit
// in one DATA segment after its header. If no records remain, Apply checks
// database health and succeeds without writing, as it does for a nil batch.
func (db *DB) Apply(batch *WriteBatch) error {
	count := batch.Len()
	return (*engine.DB)(db).ApplyMixed(count, func(i int) engine.MixedOp {
		op := batch.ops[i]
		return engine.MixedOp{
			Key:    batch.arena[op.keyStart:op.valueStart],
			Value:  batch.arena[op.valueStart:op.end],
			Delete: op.isDelete,
		}
	})
}
