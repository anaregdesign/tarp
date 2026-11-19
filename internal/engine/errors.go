package engine

import (
	"errors"
	"fmt"
	"syscall"
)

var (
	ErrEmptyKey            = errors.New("tarp: empty key")
	ErrNotFound            = errors.New("tarp: key not found")
	ErrTooLarge            = errors.New("tarp: entry too large")
	ErrInvalidOptions      = errors.New("tarp: invalid options")
	ErrIncompatibleOptions = errors.New("tarp: incompatible options")
	ErrLocked              = errors.New("tarp: directory already owned")
	ErrClosed              = errors.New("tarp: database is closing or closed")
	ErrUnhealthy           = errors.New("tarp: database requires close and reopen")
	ErrCorrupt             = errors.New("tarp: corrupt storage")
	ErrUnsupportedFormat   = errors.New("tarp: unsupported format")
	ErrUnsupportedPlatform = errors.New("tarp: unsupported platform")
	ErrNoSpace             = errors.New("tarp: insufficient storage space")
	ErrBusy                = errors.New("tarp: maintenance operation already active")
	ErrCommitUncertain     = errors.New("tarp: commit outcome uncertain")
)

// Outcome describes publication of the requested operation, not temporary space.
type Outcome string

const (
	NotApplied Outcome = "not-applied"
	Committed  Outcome = "committed"
	Unknown    Outcome = "unknown"
)

// OpError preserves the failed phase, publication outcome and underlying cause.
// An Unknown database commit requires close/reopen. An Unknown Backup or Restore
// destination publication requires inspecting the destination.
type OpError struct {
	Operation string
	Phase     string
	Outcome   Outcome
	Err       error
}

func (e *OpError) Error() string {
	return fmt.Sprintf("tarp: %s (%s, %s): %v", e.Operation, e.Phase, e.Outcome, e.Err)
}

func (e *OpError) Unwrap() error { return e.Err }

func (e *OpError) Is(target error) bool {
	return target == ErrCommitUncertain && e.Outcome == Unknown ||
		target == ErrNoSpace && (errors.Is(e.Err, syscall.ENOSPC) || errors.Is(e.Err, syscall.EDQUOT))
}

// SizeError reports admission costs. Overflow means costs exceed uint64.
type SizeError struct {
	KeyBytes, ValueBytes, RecordBytes, CacheCharge uint64
	Overflow                                       bool
	Limits                                         Options
}

func (e *SizeError) Error() string {
	return fmt.Sprintf("%v: key=%d value=%d record=%d cache=%d overflow=%t; limits record=%d cache=%d",
		ErrTooLarge, e.KeyBytes, e.ValueBytes, e.RecordBytes, e.CacheCharge, e.Overflow,
		e.Limits.MaxRecordBytes, e.Limits.CacheBytes)
}

func (e *SizeError) Unwrap() error { return ErrTooLarge }

// BatchLimitKind identifies the admission limit exceeded by a batch.
type BatchLimitKind string

const (
	BatchInputCountLimit BatchLimitKind = "input-count"
	BatchEntrySizeLimit  BatchLimitKind = "entry-size"
	BatchPayloadLimit    BatchLimitKind = "payload-bytes"
)

// BatchLimitError reports an oversized batch before publication. Index is the
// original zero-based input position, or -1 for the whole request's input count.
type BatchLimitError struct {
	Kind  BatchLimitKind
	Index int
	// Actual and Limit are input counts for BatchInputCountLimit, or encoded
	// record bytes for BatchPayloadLimit. Actual includes the offending input
	// and counts each distinct delete key once. Both fields are zero for
	// BatchEntrySizeLimit, whose Entry reports both record and cache limits.
	Actual, Limit uint64
	Entry         *SizeError
}

func (e *BatchLimitError) Error() string {
	switch e.Kind {
	case BatchInputCountLimit:
		return fmt.Sprintf("%v: batch has %d inputs, limit %d", ErrTooLarge, e.Actual, e.Limit)
	case BatchEntrySizeLimit:
		return fmt.Sprintf("tarp: batch input[%d]: %v", e.Index, e.Entry)
	default:
		return fmt.Sprintf("%v: batch input[%d] raises the encoded payload to %d bytes, limit %d", ErrTooLarge, e.Index, e.Actual, e.Limit)
	}
}

func (e *BatchLimitError) Unwrap() error {
	if e.Entry != nil {
		return e.Entry
	}
	return ErrTooLarge
}

// CompatibilityError reports minima derived from the validated existing image.
type CompatibilityError struct {
	RequiredCacheBytes, RequiredRecordBytes int64
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("%v: require cache >= %d, record >= %d",
		ErrIncompatibleOptions, e.RequiredCacheBytes, e.RequiredRecordBytes)
}

func (e *CompatibilityError) Unwrap() error { return ErrIncompatibleOptions }
