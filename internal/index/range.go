package index

import "context"

// Bounds is a half-open range of encoded keys. An empty upper bound is unbounded.
type Bounds struct {
	Lower, Upper string
}

// PrefixBounds copies an encoded key prefix. The exclusive upper bound
// increments the last non-0xff byte and drops the suffix. For an all-0xff
// prefix every key at or above the lower bound matches.
func PrefixBounds(encodedPrefix []byte) Bounds {
	lower := string(encodedPrefix)
	for i := len(encodedPrefix) - 1; i >= 0; i-- {
		if encodedPrefix[i] != 0xff {
			upper := []byte(lower[:i+1])
			upper[i]++
			return Bounds{Lower: lower, Upper: string(upper)}
		}
	}
	return Bounds{Lower: lower}
}

// Contains reports whether key is within the half-open bounds.
func (b Bounds) Contains(key string) bool {
	return key >= b.Lower && (b.Upper == "" || key < b.Upper)
}

// BoundedSnapshotter can capture just the matching portion of an index.
// BoundedView returns ctx.Err() on cancellation and otherwise succeeds.
// A canceled capture retains no snapshot.
type BoundedSnapshotter interface {
	BoundedView(ctx context.Context, bounds Bounds) (Snapshot, error)
}

// Scan selects a key range and its traversal order. Its zero value visits all
// keys in unspecified order.
type Scan struct {
	Bounds
	order scanOrder
}

type scanOrder uint8

const (
	unordered scanOrder = iota
	ascending
	descending
)

// PrefixScan visits only keys that start with prefix, in unspecified order.
func PrefixScan(prefix []byte) Scan { return Scan{Bounds: PrefixBounds(prefix)} }

// OrderedScan copies the encoded bounds before returning.
func OrderedScan(lower, upper []byte, reverse bool) Scan {
	order := ascending
	if reverse {
		order = descending
	}
	return Scan{Bounds: Bounds{Lower: string(lower), Upper: string(upper)}, order: order}
}

// ScanView pairs a captured index state with the selection still to apply.
type ScanView struct {
	Snapshot
	Scan
}

// Capture uses a bounded index view when available. The captured entries
// already match the bounds, so an unordered scan can visit them directly.
func (s Scan) Capture(ctx context.Context, backend Backend) (ScanView, error) {
	if s != (Scan{}) {
		if bounded, ok := backend.(BoundedSnapshotter); ok {
			view, err := bounded.BoundedView(ctx, s.Bounds)
			return ScanView{Snapshot: view, Scan: Scan{order: s.order}}, err
		}
	}
	return ScanView{Snapshot: backend.View(), Scan: s}, nil
}

// Walk visits the captured entries in the requested order. For a prefix on
// an unfiltered view, Range seeks to its first match.
func (v ScanView) Walk(yield func(string, Address) bool) error {
	switch {
	case v.order == descending:
		return v.RangeDesc(v.Lower, v.Upper, yield)
	case v.order == ascending || v.Bounds != (Bounds{}):
		return v.Range(v.Lower, v.Upper, yield)
	default:
		return v.All(yield)
	}
}
