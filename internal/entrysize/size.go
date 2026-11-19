// Package entrysize defines the encoded and cached sizes of one key/value entry.
package entrysize

import "math"

const (
	RecordHeaderBytes  = 48
	CacheMetadataBytes = 128 // Logical policy charge, independent of allocator overhead.
)

// Representable reports whether both sizes fit in uint64, before enforcing
// native allocation limits and the configured storage and cache limits.
func Representable(keyBytes, valueBytes uint64) bool {
	return keyBytes <= math.MaxUint64-valueBytes &&
		keyBytes+valueBytes <= math.MaxUint64-max(RecordHeaderBytes, CacheMetadataBytes)
}

// RecordBytes returns the encoded length for validated key and value lengths.
func RecordBytes[T int | int64 | uint64](keyBytes, valueBytes T) T {
	return RecordHeaderBytes + keyBytes + valueBytes
}

// CacheCharge returns the logical cache charge for validated lengths.
func CacheCharge[T int64 | uint64](keyBytes, valueBytes T) T {
	return CacheMetadataBytes + keyBytes + valueBytes
}
