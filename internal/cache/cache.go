// Package cache implements an unsynchronized, exact global LRU.
package cache

import "math"

// MetadataBytes is a logical policy charge, not the allocator's actual cost.
const MetadataBytes = 128

type entry struct {
	key        string
	value      []byte
	prev, next *entry
	lastBatch  uint64
}

// Cache requires external serialization. Stored bytes must remain immutable.
type Cache struct {
	items       map[string]*entry
	head, tail  *entry
	used, limit int64
	evictions   uint64
	batch       uint64
}

func New(limit int64) *Cache {
	return &Cache{items: make(map[string]*entry), limit: limit}
}

func (c *Cache) Bytes() int64 { return c.used }

// Evictions counts entries removed to admit another entry within the byte limit.
func (c *Cache) Evictions() uint64 { return c.evictions }

// Clear releases all entries while retaining the capacity-eviction counter.
func (c *Cache) Clear() {
	*c = Cache{items: make(map[string]*entry), limit: c.limit, evictions: c.evictions}
}

// BeginBatch starts a group of peeks under the caller's lock.
func (c *Cache) BeginBatch() {
	c.batch++
	if c.batch == 0 {
		for _, e := range c.items {
			e.lastBatch = 0
		}
		c.batch = 1
	}
}

// PeekBatch returns the immutable value, its presence, and whether this key
// already occurred in this batch. Peeking preserves recency.
func (c *Cache) PeekBatch(key []byte) (value []byte, found, duplicate bool) {
	e, ok := c.items[string(key)]
	if !ok {
		return nil, false, false
	}
	if e.lastBatch == c.batch {
		return e.value, true, true
	}
	e.lastBatch = c.batch
	return e.value, true, false
}

func (c *Cache) Get(key []byte) ([]byte, bool) {
	e, ok := c.items[string(key)]
	if !ok {
		return nil, false
	}
	c.unlink(e)
	c.front(e)
	return e.value, true
}

// Put takes ownership of immutable bytes. The caller must preflight the charge.
func (c *Cache) Put(key string, value []byte) {
	charge := int64(MetadataBytes) + int64(len(key)) + int64(len(value))
	if charge > c.limit {
		panic("cache: unvalidated admission")
	}
	c.Delete(key)
	for c.used > c.limit-charge {
		c.Delete(c.tail.key)
		if c.evictions < math.MaxUint64 {
			c.evictions++
		}
	}
	e := &entry{key: key, value: value}
	c.items[key] = e
	c.used += charge
	c.front(e)
}

func (c *Cache) Delete(key string) {
	if e, ok := c.items[key]; ok {
		c.unlink(e)
		delete(c.items, key)
		c.used -= int64(MetadataBytes) + int64(len(e.key)) + int64(len(e.value))
	}
}

func (c *Cache) unlink(e *entry) {
	if e.prev == nil {
		c.head = e.next
	} else {
		e.prev.next = e.next
	}
	if e.next == nil {
		c.tail = e.prev
	} else {
		e.next.prev = e.prev
	}
}

func (c *Cache) front(e *entry) {
	e.prev, e.next = nil, c.head
	if c.head == nil {
		c.tail = e
	} else {
		c.head.prev = e
	}
	c.head = e
}
