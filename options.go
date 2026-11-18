package tarp

import (
	"fmt"
	"math"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/format"
)

// Options uses zero for each default, never to disable caching or syncing.
// Bounds apply to encoded records/files and logically charged cache entries.
type Options struct {
	CacheBytes       int64
	MaxRecordBytes   int64
	MaxDataFileBytes int64
	// TargetSegmentBytes sets the soft rotation target for new log appends.
	// Zero selects 4 MiB. Existing files are checked against MaxDataFileBytes.
	TargetSegmentBytes int64
	ReadFileHandles    int
}

func (o Options) normalized() (Options, error) {
	if o.CacheBytes < 0 || o.MaxRecordBytes < 0 || o.MaxDataFileBytes < 0 || o.ReadFileHandles < 0 || o.TargetSegmentBytes < 0 {
		return o, fmt.Errorf("%w: negative setting", ErrInvalidOptions)
	}
	if o.CacheBytes == 0 {
		o.CacheBytes = 64 << 20
	}
	if o.MaxRecordBytes == 0 {
		o.MaxRecordBytes = 1 << 20
	}
	if o.MaxDataFileBytes == 0 {
		o.MaxDataFileBytes = 128 << 20
	}
	if o.ReadFileHandles == 0 {
		o.ReadFileHandles = 64
	}
	if o.CacheBytes < cache.MetadataBytes+1 || o.MaxRecordBytes < format.RecordHeaderBytes+1 ||
		o.MaxDataFileBytes < format.SegmentHeaderBytes+format.RecordHeaderBytes+1 ||
		o.MaxRecordBytes > o.MaxDataFileBytes-format.SegmentHeaderBytes || o.ReadFileHandles < 1 {
		return o, fmt.Errorf("%w: inconsistent byte or handle bounds", ErrInvalidOptions)
	}
	if o.TargetSegmentBytes == 0 {
		o.TargetSegmentBytes = 4 << 20
	}
	o.TargetSegmentBytes = min(o.MaxDataFileBytes,
		max(o.TargetSegmentBytes, o.MaxRecordBytes+format.SegmentHeaderBytes))
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
	if e.RecordBytes > uint64(o.MaxRecordBytes) ||
		e.RecordBytes > uint64(o.MaxDataFileBytes-format.SegmentHeaderBytes) ||
		e.CacheCharge > uint64(o.CacheBytes) || e.CacheCharge > math.MaxInt64 {
		return e
	}
	return nil
}
