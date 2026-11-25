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

// ScanView pairs a snapshot with any bounds still to apply during iteration.
type ScanView struct {
	view   Snapshot
	bounds Bounds
}

func (b Bounds) Capture(ctx context.Context, backend Backend) (ScanView, error) {
	if b != (Bounds{}) {
		if snapshot, ok := backend.(BoundedSnapshotter); ok {
			v, err := snapshot.BoundedView(ctx, b)
			return ScanView{view: v}, err
		}
	}
	return ScanView{view: backend.View(), bounds: b}, nil
}

func (s ScanView) Close() { s.view.Close() }

// RangeView can seek directly to the first entry at or above lower and visit
// [lower, upper) in strictly ascending key order.
type RangeView interface {
	Range(lower, upper string, yield func(string, Address) bool) error
}

// ReverseRangeView visits [lower, upper) in strictly descending key order.
type ReverseRangeView interface {
	RangeDesc(lower, upper string, yield func(string, Address) bool) error
}

// CaptureOrdered captures a snapshot prepared for ordered [lower, upper)
// traversal in ascending or descending key order.
func (b Bounds) CaptureOrdered(ctx context.Context, backend Backend) (ScanView, error) {
	if snapshot, ok := backend.(BoundedSnapshotter); ok {
		v, err := snapshot.BoundedView(ctx, b)
		return ScanView{view: v, bounds: b}, err
	}
	return ScanView{view: backend.View(), bounds: b}, nil
}

// WalkOrdered visits matching addresses in strictly ascending or descending
// encoded-key order.
func (s ScanView) WalkOrdered(reverse bool, yield func(string, Address) bool) error {
	b, v := s.bounds, s.view
	if b.Upper != "" && b.Lower >= b.Upper {
		return nil
	}
	if reverse {
		if ordered, ok := v.(ReverseRangeView); ok {
			return ordered.RangeDesc(b.Lower, b.Upper, yield)
		}
		panic("tarp: index view does not implement ReverseRangeView")
	}
	if ordered, ok := v.(RangeView); ok {
		return ordered.Range(b.Lower, b.Upper, yield)
	}
	panic("tarp: index view does not implement RangeView")
}

// Walk visits only matching addresses. An unordered view filters its snapshot
// before the caller reads DATA, so it needs no secondary ordered index.
func (s ScanView) Walk(ctx context.Context, yield func(string, Address) bool) error {
	b, v := s.bounds, s.view
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
		if !b.Contains(k) {
			return true
		}
		return yield(k, a)
	})
	if canceled != nil {
		return canceled
	}
	return err
}
