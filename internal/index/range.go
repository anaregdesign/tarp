package index

import "context"

// Bounds is a half-open range of encoded keys. An empty upper bound is unbounded.
type Bounds struct {
	Lower, Upper string
}

// PrefixBounds takes ownership of a copy of prefix. The exclusive upper bound
// increments the last non-0xff byte and drops the suffix. For an all-0xff
// prefix every key at or above the lower bound matches.
func PrefixBounds(prefix []byte) Bounds {
	lower := string(prefix)
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xff {
			upper := []byte(lower[:i+1])
			upper[i]++
			return Bounds{Lower: lower, Upper: string(upper)}
		}
	}
	return Bounds{Lower: lower}
}

// RangeView can seek directly to the first entry at or above lower.
type RangeView interface {
	Range(lower, upper string, yield func(string, Address) bool) error
}

// Walk visits only matching addresses. An unordered view filters its snapshot
// before the caller reads DATA, so it needs no secondary ordered index.
func (b Bounds) Walk(ctx context.Context, v View, yield func(string, Address) bool) error {
	if b == (Bounds{}) {
		return v.All(yield)
	}
	if ordered, ok := v.(RangeView); ok {
		return ordered.Range(b.Lower, b.Upper, yield)
	}
	var canceled error
	err := v.All(func(k string, a Address) bool {
		if canceled = ctx.Err(); canceled != nil {
			return false
		}
		if k < b.Lower || (b.Upper != "" && k >= b.Upper) {
			return true
		}
		return yield(k, a)
	})
	if canceled != nil {
		return canceled
	}
	return err
}
