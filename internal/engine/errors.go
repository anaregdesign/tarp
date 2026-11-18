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
// After an Unknown result, close/reopen before inspecting the resulting state.
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

// CompatibilityError reports minima derived from the validated existing image.
type CompatibilityError struct {
	RequiredCacheBytes, RequiredRecordBytes int64
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("%v: require cache >= %d, record >= %d",
		ErrIncompatibleOptions, e.RequiredCacheBytes, e.RequiredRecordBytes)
}

func (e *CompatibilityError) Unwrap() error { return ErrIncompatibleOptions }
