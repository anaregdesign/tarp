// Package btree implements a disposable, copy-on-write B+tree. Verified DATA
// records are its source of truth. All scratch descriptors are process-private
// and unlinked at creation. Reopening a database creates a fresh tree.
//
// The private tree uses numbered 8 KiB B+tree pages. The little-endian
// header stores the CRC32C, page kind, slot count or overflow payload length,
// and page ID. A leaf or branch page encodes a node. Each slot holds the offset
// of one cell. Short keys share a page-wide key prefix. Each leaf cell maps
// an encoded key to a DATA record address. A branch page holds a leftmost
// child page ID and (separator key, right child page ID) cells. A search
// matching a separator descends right. Splitting a leaf constructs the
// shortest distinguishing prefix of its new right neighbor. Keys over 240 bytes have a 24-byte
// inline prefix, full length and an owned chain of checked overflow pages.
//
// A writer prepares pages privately and can update its resident private pages
// in place. Commit publishes the new root and count in memory. A view pins a
// root generation. Retired page IDs become reusable once older views close.
// Page ID lists spill to scratch. Bulk construction packs sorted entries into
// leaves, then streams their separators to build one branch level at a time.
//
// Half the per-tree memory budget caps resident page charges, including
// the 8 KiB page and its bookkeeping.
// A dirty victim is written synchronously. The page cache favors branch pages over
// leaf and overflow pages and protects the current root. Up to one quarter funds
// fixed 48 KiB reservations for active reads. Bounded groups use page IDs
// for pending ranges, and iterators release reservations before yielding.
// The single writer has independent working capacity for page construction.
// The external bulk sorter can use a quarter of the budget while building.
//
// Charges are logical. Inputs, returned keys and values, caller-owned result
// buffers, Go allocator overhead and the OS page cache also contribute to RSS.
// Overflow comparison streams chunks; enumeration materializes the returned
// key and the external sorter can hold an oversized input record.
package btree
