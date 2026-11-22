package index

import (
	"strings"
	"testing"
)

func TestPrefixBoundsMatchBytes(t *testing.T) {
	samples := []string{"", "\x00", "\x00\xff", "\xfe", "\xfe\xff", "\xff", "\xff\x00", "\xff\xff", "\xff\xff\x00", "a", "ab", "a\xff\xff", "b"}
	for _, prefix := range samples {
		b := PrefixBounds([]byte(prefix))
		for _, key := range samples {
			within := b.Contains(key)
			if within != strings.HasPrefix(key, prefix) {
				t.Fatalf("range [%x, %x) for prefix %x, key %x: got %v", b.Lower, b.Upper, prefix, key, within)
			}
		}
	}
}
