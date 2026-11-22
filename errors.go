package tarp

import (
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/publication"
)

var (
	ErrEmptyKey            = engine.ErrEmptyKey
	ErrNotFound            = engine.ErrNotFound
	ErrTooLarge            = engine.ErrTooLarge
	ErrInvalidOptions      = engine.ErrInvalidOptions
	ErrIncompatibleOptions = engine.ErrIncompatibleOptions
	ErrLocked              = engine.ErrLocked
	ErrClosed              = engine.ErrClosed
	ErrUnhealthy           = engine.ErrUnhealthy
	ErrCorrupt             = engine.ErrCorrupt
	ErrUnsupportedFormat   = engine.ErrUnsupportedFormat
	ErrUnsupportedPlatform = engine.ErrUnsupportedPlatform
	ErrNoSpace             = engine.ErrNoSpace
	ErrBusy                = engine.ErrBusy
	ErrCommitUncertain     = engine.ErrCommitUncertain
)

// ErrBackupDirectory indicates that an open operation detected the BACKUP marker.
// Use [VerifyBackup] to check its contents or [Restore] to create a database.
var ErrBackupDirectory = engine.ErrBackupDirectory

// ErrDataUnavailable identifies a localized failure reading sealed DATA.
// Other operations can continue. Use errors.As to retrieve [UnavailableDataError].
var ErrDataUnavailable = engine.ErrDataUnavailable

// UnavailableDataError identifies the affected sealed DATA by FileID and
// retains the original read and repair errors. FileID can be used with [SalvageData].
type UnavailableDataError = engine.UnavailableDataError

// Outcome describes publication of the requested operation.
type Outcome = publication.Outcome

const (
	NotApplied = publication.NotApplied
	Committed  = publication.Committed
	Unknown    = publication.Unknown
)

// OpError identifies the failed operation, phase, publication outcome and cause.
// An unknown database commit requires close/reopen before checking its effects.
// An unknown Backup or Restore destination publication requires inspecting the
// destination. A destination error alone allows continued use of a backup source.
type OpError = engine.OpError

// SizeError compares the encoded RecordBytes with MaxRecordBytes and the
// logical ValueCacheChargeBytes with ValueCacheBudgetBytes. Overflow means
// the costs do not fit in uint64.
type SizeError = engine.SizeError

// BatchKeyError identifies the first empty input key in BatchGet, BatchPut or
// BatchDelete, or the first missing key in BatchDelete. Operation names the
// method. Index is the zero-based position in the original request, including
// duplicates. Typed batches check the encoded key, so a codec can produce an
// empty key from a nonzero Go value. Err matches ErrEmptyKey or ErrNotFound
// via errors.Is.
type BatchKeyError = engine.BatchKeyError

// BatchLimitKind identifies which batch limit was exceeded. The limits count input
// items, individual entry sizes, total encoded record bytes, or result value bytes.
type BatchLimitKind = engine.BatchLimitKind

const (
	BatchInputCountLimit    = engine.BatchInputCountLimit
	BatchEntrySizeLimit     = engine.BatchEntrySizeLimit
	BatchRecordBytesLimit   = engine.BatchRecordBytesLimit
	BatchGetValueBytesLimit = engine.BatchGetValueBytesLimit
)

// BatchLimitError matches ErrTooLarge. Index is the original zero-based input
// position, or -1 for a request-wide count failure. Actual and Limit are
// counts for BatchInputCountLimit or total encoded record bytes through Index for
// BatchRecordBytesLimit. For BatchGetValueBytesLimit they describe encoded value
// bytes, counting each requested occurrence of a found key. For
// BatchEntrySizeLimit both are zero and errors.As can
// also retrieve the underlying *SizeError with its record and cache charges.
type BatchLimitError = engine.BatchLimitError

// CompatibilityError reports the minimum settings required by an existing image.
type CompatibilityError = engine.CompatibilityError
