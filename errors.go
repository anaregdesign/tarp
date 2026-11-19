package tarp

import "github.com/anaregdesign/tarp/internal/engine"

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

// Outcome describes publication of the requested operation.
type Outcome = engine.Outcome

const (
	NotApplied = engine.NotApplied
	Committed  = engine.Committed
	Unknown    = engine.Unknown
)

// OpError identifies the failed operation, phase, publication outcome and cause.
// An unknown database commit requires close/reopen before checking its effects.
// An unknown Backup or Restore destination publication requires inspecting the
// destination. A destination error alone allows continued use of a backup source.
type OpError = engine.OpError

// SizeError reports the size of an entry that exceeds admission limits.
type SizeError = engine.SizeError

// BatchLimitKind identifies a batch input-count, entry-size or payload limit.
type BatchLimitKind = engine.BatchLimitKind

const (
	BatchInputCountLimit = engine.BatchInputCountLimit
	BatchEntrySizeLimit  = engine.BatchEntrySizeLimit
	BatchPayloadLimit    = engine.BatchPayloadLimit
)

// BatchLimitError matches ErrTooLarge. Index is the original zero-based input
// position, or -1 for a request-wide count failure. Actual and Limit are
// counts for BatchInputCountLimit or encoded bytes through Index for
// BatchPayloadLimit. For BatchEntrySizeLimit both are zero and errors.As can
// also retrieve the underlying *SizeError with its record and cache charges.
type BatchLimitError = engine.BatchLimitError

// CompatibilityError reports the minimum settings required by an existing image.
type CompatibilityError = engine.CompatibilityError
