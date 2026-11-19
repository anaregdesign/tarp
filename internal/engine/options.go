package engine

import (
	"fmt"
	"math"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/format"
)

// Options uses zero for each default, never to disable caching or syncing.
// Bounds apply to encoded records and logically charged cache entries.
type Options struct {
	CacheBytes     int64
	MaxRecordBytes int64
	// TargetSegmentBytes sizes new logs and checkpoints and caps repack input bytes.
	// Zero selects 4 MiB. The target grows to fit a header and one maximum record.
	TargetSegmentBytes int64
	ReadFileHandles    int
	// BatchReadConcurrency limits concurrent DATA reads across BatchGet calls
	// on this DB. Zero selects 8. One runs the reads serially.
	BatchReadConcurrency int
}

func (o Options) normalized() (Options, error) {
	if o.CacheBytes < 0 || o.MaxRecordBytes < 0 || o.ReadFileHandles < 0 || o.TargetSegmentBytes < 0 || o.BatchReadConcurrency < 0 {
		return o, fmt.Errorf("%w: negative setting", ErrInvalidOptions)
	}
	if o.CacheBytes == 0 {
		o.CacheBytes = 64 << 20
	}
	if o.MaxRecordBytes == 0 {
		o.MaxRecordBytes = 1 << 20
	}
	if o.ReadFileHandles == 0 {
		o.ReadFileHandles = 64
	}
	if o.BatchReadConcurrency == 0 {
		o.BatchReadConcurrency = 8
	}
	if o.CacheBytes < cache.MetadataBytes+1 || o.MaxRecordBytes < format.RecordHeaderBytes+1 ||
		o.MaxRecordBytes > math.MaxInt64-format.SegmentHeaderBytes {
		return o, fmt.Errorf("%w: invalid cache or record size", ErrInvalidOptions)
	}
	if o.TargetSegmentBytes == 0 {
		o.TargetSegmentBytes = 4 << 20
	}
	o.TargetSegmentBytes = max(o.TargetSegmentBytes, o.MaxRecordBytes+format.SegmentHeaderBytes)
	return o, nil
}

func (o Options) checkSize(key, value uint64) error {
	if key == 0 {
		return ErrEmptyKey
	}
	e := &SizeError{KeyBytes: key, ValueBytes: value, Limits: o}
	if key > math.MaxUint64-value || key+value > math.MaxUint64-cache.MetadataBytes {
		e.Overflow = true
		return e
	}
	e.RecordBytes = format.RecordHeaderBytes + key + value
	e.CacheCharge = cache.MetadataBytes + key + value
	if e.RecordBytes > uint64(o.MaxRecordBytes) || e.RecordBytes > math.MaxInt || e.CacheCharge > uint64(o.CacheBytes) {
		return e
	}
	return nil
}
