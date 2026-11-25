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
	IndexBTree IndexMode = iota
	IndexMemory
)

const defaultBTreeMemoryBudgetBytes = 16 << 20

// OpenValidation selects when DATA records covered by an index checkpoint are checked.
type OpenValidation uint8

const (
	Full OpenValidation = iota
	Indexed
)

// Options uses zero for each default, never to disable caching or syncing.
// Bounds apply to encoded records and logically charged value cache entries.
type Options struct {
	IndexMode      IndexMode
	OpenValidation OpenValidation
	// PreserveUnavailableKeys refuses checkpoints that would drop live keys.
	PreserveUnavailableKeys bool
	BTreeMemoryBudgetBytes  int64
	ValueCacheBudgetBytes   int64
	MaxRecordBytes          int64
	// TargetSegmentBytes sets the target for each new log or snapshot segment.
	// It also caps the input to small-segment repacking.
	// Zero selects 4 MiB. The target grows to fit a header and one maximum record.
	TargetSegmentBytes int64
	ReadFileHandles    int
	// BatchGetReadConcurrency limits concurrent DATA reads across BatchGet calls
	// on this DB. Zero selects 8. One runs the reads serially.
	BatchGetReadConcurrency int
	// MaxBatchGetValueBytes limits encoded value bytes returned by one BatchGet.
	// Each occurrence of a found key counts. Zero selects 64 MiB.
	MaxBatchGetValueBytes int64
	// MaxWriteBatchEntries caps input operations per write batch, including duplicates.
	// Zero selects MaxBatchEntries.
	MaxWriteBatchEntries int
}

// RestoreLimits bounds the logical value cache charge and encoded size of each live
// record in a restored image. Zero fields select the same defaults as Open.
type RestoreLimits struct {
	ValueCacheBudgetBytes int64
	MaxRecordBytes        int64
}

// RestoreLimits returns the settings used to verify a restored image.
func (o Options) RestoreLimits() RestoreLimits {
	return RestoreLimits{ValueCacheBudgetBytes: o.ValueCacheBudgetBytes, MaxRecordBytes: o.MaxRecordBytes}
}

func (l RestoreLimits) normalized() (RestoreLimits, error) {
	if l.ValueCacheBudgetBytes == 0 {
		l.ValueCacheBudgetBytes = 64 << 20
	}
	if l.MaxRecordBytes == 0 {
		l.MaxRecordBytes = 1 << 20
	}
	if l.ValueCacheBudgetBytes < entrysize.CacheMetadataBytes+1 || l.MaxRecordBytes < format.RecordHeaderBytes+1 ||
		l.MaxRecordBytes > math.MaxInt64-format.SegmentHeaderBytes {
		return l, fmt.Errorf("%w: invalid value cache budget or maximum record size", ErrInvalidOptions)
	}
	return l, nil
}

func (o Options) normalized() (Options, error) {
	if o.OpenValidation > Indexed {
		return o, fmt.Errorf("%w: unknown open validation %d", ErrInvalidOptions, o.OpenValidation)
	}
	switch o.IndexMode {
	case IndexMemory:
		if o.BTreeMemoryBudgetBytes != 0 {
			return o, fmt.Errorf("%w: BTreeMemoryBudgetBytes requires B+tree mode", ErrInvalidOptions)
		}
	case IndexBTree:
		if o.BTreeMemoryBudgetBytes == 0 {
			o.BTreeMemoryBudgetBytes = defaultBTreeMemoryBudgetBytes
		}
		if o.BTreeMemoryBudgetBytes < 1<<20 {
			return o, fmt.Errorf("%w: BTreeMemoryBudgetBytes must be at least 1 MiB", ErrInvalidOptions)
		}
	default:
		return o, fmt.Errorf("%w: unknown index mode %d", ErrInvalidOptions, o.IndexMode)
	}
	if o.ReadFileHandles < 0 || o.TargetSegmentBytes < 0 || o.BatchGetReadConcurrency < 0 || o.MaxBatchGetValueBytes < 0 {
		return o, fmt.Errorf("%w: negative setting", ErrInvalidOptions)
	}
	limits, err := o.RestoreLimits().normalized()
	if err != nil {
		return o, err
	}
	o.ValueCacheBudgetBytes, o.MaxRecordBytes = limits.ValueCacheBudgetBytes, limits.MaxRecordBytes
	if o.ReadFileHandles == 0 {
		o.ReadFileHandles = 64
	}
	if o.BatchGetReadConcurrency == 0 {
		o.BatchGetReadConcurrency = 8
	}
	if o.MaxBatchGetValueBytes == 0 {
		o.MaxBatchGetValueBytes = 64 << 20
	}
	if o.MaxWriteBatchEntries < 0 || o.MaxWriteBatchEntries > MaxWriteBatchEntries {
		return o, fmt.Errorf("%w: MaxWriteBatchEntries must be between 0 and %d", ErrInvalidOptions, MaxWriteBatchEntries)
	}
	if o.MaxWriteBatchEntries == 0 {
		o.MaxWriteBatchEntries = MaxBatchEntries
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
	if err := o.checkEntryLimits(key, value); err != nil {
		return err
	}
	return nil
}

func (o Options) checkEntryLimits(key, value uint64) *SizeError {
	var recordBytes, cacheCharge uint64
	overflow := !entrysize.Representable(key, value)
	if !overflow {
		recordBytes = entrysize.RecordBytes(key, value)
		cacheCharge = entrysize.CacheCharge(key, value)
		if recordBytes <= uint64(o.MaxRecordBytes) && recordBytes <= math.MaxInt && cacheCharge <= uint64(o.ValueCacheBudgetBytes) {
			return nil
		}
	}
	return &SizeError{
		KeyBytes: key, ValueBytes: value, RecordBytes: recordBytes, ValueCacheChargeBytes: cacheCharge,
		MaxRecordBytes: o.MaxRecordBytes, ValueCacheBudgetBytes: o.ValueCacheBudgetBytes, Overflow: overflow,
	}
}
