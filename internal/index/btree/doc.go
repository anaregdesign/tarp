// Package btree implements a disposable, copy-on-write B+tree. Verified DATA
// records are its source of truth. All scratch descriptors are process-private
// and unlinked at creation. Reopening a database creates a fresh tree.
//
// The arena consists of numbered 8 KiB pages. Each little-endian header
// records a CRC32C, kind, slot or payload count, and the page's own ID. A node
// has a shared key prefix and a sorted array of two-byte offsets. A leaf cell
// contains a binary key and a DATA address. An internal page has a leftmost
// child and (lower bound, right child) cells. Equality with a separator
// descends right. Splitting a leaf constructs the shortest distinguishing
// prefix of its new right neighbor. Keys over 240 bytes have a 24-byte
// inline prefix, full length and an owned chain of checked overflow pages.
//
// A writer prepares pages privately and can update its resident private pages
// in place. Commit publishes the new root and count in memory. A view pins a
// generation; retired page IDs become reusable once older views are released.
// Page ID lists spill to scratch. Bulk construction packs sorted entries into
// leaves, then streams their separators to build one branch level at a time.
//
// Half the shard budget caps resident page charges (8 KiB plus bookkeeping).
// A dirty victim is written synchronously. The cache favors branches over
// ordinary pages and protects the current root. Up to one quarter funds
// fixed 48 KiB reservations for active reads. Bounded groups use page IDs
// for pending ranges, and iterators release reservations before yielding.
// The single writer has independent working capacity for page construction.
// The external bulk sorter can use a quarter of the budget while building.
//
// Charges are logical. Inputs, returned keys and values, caller-owned result
// buffers, Go allocator overhead and the OS file cache also contribute to RSS.
// Overflow comparison streams chunks; enumeration materializes the returned
// key and the external sorter can hold an oversized input record.
package btree
