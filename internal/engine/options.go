package engine

import (
	"fmt"
	"math"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
)

// IndexMode selects a derived key index for this open handle.
type IndexMode uint8

const (
	IndexDefault IndexMode = iota
	IndexMemory
	IndexBTree
)

const defaultIndexBudget = 16 << 20

// Options uses zero for each default, never to disable caching or syncing.
// Bounds apply to encoded records and logically charged cache entries.
type Options struct {
	IndexMode        IndexMode
	IndexMemoryBytes int64
	CacheBytes       int64
	MaxRecordBytes   int64
	// TargetSegmentBytes sizes new logs and checkpoints and caps repack input bytes.
	// Zero selects 4 MiB. The target grows to fit a header and one maximum record.
	TargetSegmentBytes int64
	ReadFileHandles    int
	// BatchReadConcurrency limits concurrent DATA reads across BatchGet calls
	// on this DB. Zero selects 8. One runs the reads serially.
	BatchReadConcurrency int
	// MaxBatchResultBytes limits encoded value bytes returned by one BatchGet.
	// Each occurrence of a found key counts. Zero selects 64 MiB.
	MaxBatchResultBytes int64
}

// RestoreLimits bounds the logical cache charge and encoded size of each live
// record in a restored image. Zero fields select the same defaults as Open.
type RestoreLimits struct {
	CacheBytes     int64
	MaxRecordBytes int64
}

// RestoreLimits returns the settings used to verify a restored image.
func (o Options) RestoreLimits() RestoreLimits {
	return RestoreLimits{CacheBytes: o.CacheBytes, MaxRecordBytes: o.MaxRecordBytes}
}

func (l RestoreLimits) normalized() (RestoreLimits, error) {
	if l.CacheBytes == 0 {
		l.CacheBytes = 64 << 20
	}
	if l.MaxRecordBytes == 0 {
		l.MaxRecordBytes = 1 << 20
	}
	if l.CacheBytes < entrysize.CacheMetadataBytes+1 || l.MaxRecordBytes < format.RecordHeaderBytes+1 ||
		l.MaxRecordBytes > math.MaxInt64-format.SegmentHeaderBytes {
		return l, fmt.Errorf("%w: invalid cache or record size", ErrInvalidOptions)
	}
	return l, nil
}

func (o Options) normalized() (Options, error) {
	if o.IndexMode == IndexDefault {
		o.IndexMode = IndexMemory
	}
	switch o.IndexMode {
	case IndexMemory:
		if o.IndexMemoryBytes != 0 {
			return o, fmt.Errorf("%w: IndexMemoryBytes requires B+tree mode", ErrInvalidOptions)
		}
	case IndexBTree:
		if o.IndexMemoryBytes == 0 {
			o.IndexMemoryBytes = defaultIndexBudget
		}
		if o.IndexMemoryBytes < 1<<20 {
			return o, fmt.Errorf("%w: index budget must be at least 1 MiB", ErrInvalidOptions)
		}
	default:
		return o, fmt.Errorf("%w: unknown index mode %d", ErrInvalidOptions, o.IndexMode)
	}
	if o.ReadFileHandles < 0 || o.TargetSegmentBytes < 0 || o.BatchReadConcurrency < 0 || o.MaxBatchResultBytes < 0 {
		return o, fmt.Errorf("%w: negative setting", ErrInvalidOptions)
	}
	limits, err := o.RestoreLimits().normalized()
	if err != nil {
		return o, err
	}
	o.CacheBytes, o.MaxRecordBytes = limits.CacheBytes, limits.MaxRecordBytes
	if o.ReadFileHandles == 0 {
		o.ReadFileHandles = 64
	}
	if o.BatchReadConcurrency == 0 {
		o.BatchReadConcurrency = 8
	}
	if o.MaxBatchResultBytes == 0 {
		o.MaxBatchResultBytes = 64 << 20
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
	var recordBytes, cacheCharge uint64
	overflow := !entrysize.Representable(key, value)
	if !overflow {
		recordBytes = entrysize.RecordBytes(key, value)
		cacheCharge = entrysize.CacheCharge(key, value)
		if recordBytes <= uint64(o.MaxRecordBytes) && recordBytes <= math.MaxInt && cacheCharge <= uint64(o.CacheBytes) {
			return nil
		}
	}
	return &SizeError{
		KeyBytes: key, ValueBytes: value, RecordBytes: recordBytes, CacheCharge: cacheCharge,
		MaxRecordBytes: o.MaxRecordBytes, CacheBytes: o.CacheBytes, Overflow: overflow,
	}
}
