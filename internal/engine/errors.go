package engine

import (
	"errors"
	"fmt"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
	"github.com/anaregdesign/tarp/internal/storage"
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
	ErrBackupDirectory     = storage.ErrBackupDirectory
	ErrCorrupt             = errors.New("tarp: corrupt storage")
	ErrDataUnavailable     = storage.ErrDataUnavailable
	ErrUnsupportedFormat   = errors.New("tarp: unsupported format")
	ErrUnsupportedPlatform = errors.New("tarp: unsupported platform")
	ErrNoSpace             = errors.New("tarp: insufficient storage space")
	ErrBusy                = errors.New("tarp: maintenance operation already active")
	ErrCommitUncertain     = errors.New("tarp: commit outcome uncertain")
)

// UnavailableDataError identifies the immutable DATA file that could not
// supply a requested record. Its cause is available through errors.Unwrap.
type UnavailableDataError = storage.UnavailableDataError

// Keep the underlying error tree intact so a caller can distinguish a
// localized DATA failure from a separate failure joined during cleanup.
type localizedReadError struct{ cause error }

func (e *localizedReadError) Error() string { return e.cause.Error() }
func (e *localizedReadError) Unwrap() error { return e.cause }
func (e *localizedReadError) Is(target error) bool {
	switch target {
	case ErrCorrupt:
		return errors.Is(e.cause, format.ErrCorrupt)
	case ErrNoSpace:
		return errors.Is(e.cause, syscall.ENOSPC) || errors.Is(e.cause, syscall.EDQUOT)
	}
	return false
}

// Outcome describes publication of the requested operation, not temporary space.
type Outcome = publication.Outcome

const (
	NotApplied = publication.NotApplied
	Committed  = publication.Committed
	Unknown    = publication.Unknown
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
	KeyBytes, ValueBytes, RecordBytes, ValueCacheChargeBytes uint64
	Overflow                                                 bool
	// MaxRecordBytes limits RecordBytes, including the encoded header.
	MaxRecordBytes int64
	// ValueCacheBudgetBytes limits ValueCacheChargeBytes, including logical value cache metadata.
	ValueCacheBudgetBytes int64
}

func (e *SizeError) Error() string {
	return fmt.Sprintf("%v: key=%d bytes, value=%d bytes, record=%d bytes (max %d), value cache charge=%d bytes (budget %d), overflow=%t",
		ErrTooLarge, e.KeyBytes, e.ValueBytes, e.RecordBytes, e.MaxRecordBytes,
		e.ValueCacheChargeBytes, e.ValueCacheBudgetBytes, e.Overflow)
}

func (e *SizeError) Unwrap() error { return ErrTooLarge }

// BatchKeyError identifies an empty input key or a missing BatchDelete key.
type BatchKeyError struct {
	Operation string // BatchGet, BatchPut, BatchDelete or Apply.
	Index     int    // Zero-based position in the original request.
	Err       error  // ErrEmptyKey or ErrNotFound.
}

func (e *BatchKeyError) Error() string {
	return fmt.Sprintf("tarp: %s input[%d]: %v", e.Operation, e.Index, e.Err)
}

func (e *BatchKeyError) Unwrap() error { return e.Err }

// BatchLimitKind identifies the admission limit exceeded by a batch.
type BatchLimitKind string

const (
	BatchInputCountLimit    BatchLimitKind = "input-count"
	BatchEntrySizeLimit     BatchLimitKind = "entry-size"
	BatchRecordBytesLimit   BatchLimitKind = "encoded-record-bytes"
	BatchGetValueBytesLimit BatchLimitKind = "result-value-bytes"
)

// BatchLimitError reports an oversized batch before publication. Index is the
// original zero-based input position, or -1 for the whole request's input count.
type BatchLimitError struct {
	Kind  BatchLimitKind
	Index int
	// Actual and Limit are input counts for BatchInputCountLimit, or encoded
	// record bytes for BatchRecordBytesLimit, or returned value bytes for
	// BatchGetValueBytesLimit. Actual includes the offending input
	// and counts each distinct delete key once. Both fields are zero for
	// BatchEntrySizeLimit, whose Entry reports both record and value cache limits.
	Actual, Limit uint64
	Entry         *SizeError
}

func (e *BatchLimitError) Error() string {
	switch e.Kind {
	case BatchInputCountLimit:
		return fmt.Sprintf("%v: batch has %d inputs, limit %d", ErrTooLarge, e.Actual, e.Limit)
	case BatchEntrySizeLimit:
		return fmt.Sprintf("tarp: batch input[%d]: %v", e.Index, e.Entry)
	case BatchGetValueBytesLimit:
		return fmt.Sprintf("%v: BatchGet input[%d] raises the encoded value total to %d bytes, limit %d", ErrTooLarge, e.Index, e.Actual, e.Limit)
	default:
		return fmt.Sprintf("%v: batch input[%d] raises the encoded record total to %d bytes, limit %d", ErrTooLarge, e.Index, e.Actual, e.Limit)
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
	RequiredValueCacheBudgetBytes, RequiredMaxRecordBytes int64
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("%v: require value cache budget >= %d bytes, max record size >= %d bytes",
		ErrIncompatibleOptions, e.RequiredValueCacheBudgetBytes, e.RequiredMaxRecordBytes)
}

func (e *CompatibilityError) Unwrap() error { return ErrIncompatibleOptions }
