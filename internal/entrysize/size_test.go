package entrysize

import (
	"math"
	"testing"
)

func TestLargestRepresentableEntry(t *testing.T) {
	const keyBytes uint64 = math.MaxUint64 - 128 - 42
	const valueBytes uint64 = 42
	if !Representable(keyBytes, valueBytes) {
		t.Fatal("largest cache charge rejected")
	}
	if got := CacheCharge(keyBytes, valueBytes); got != math.MaxUint64 {
		t.Fatalf("cache charge = %d", got)
	}
	if got := RecordBytes(keyBytes, valueBytes); got != math.MaxUint64-80 {
		t.Fatalf("record size = %d", got)
	}
	if Representable(keyBytes+1, valueBytes) || Representable(math.MaxUint64, 1) {
		t.Fatal("accepted overflowing cache charge or payload")
	}
}
