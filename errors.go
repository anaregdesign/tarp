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

// OpError preserves the failed phase, publication outcome and underlying cause.
type OpError = engine.OpError

// SizeError reports the size of an entry that exceeds admission limits.
type SizeError = engine.SizeError

// CompatibilityError reports the minimum settings required by an existing image.
type CompatibilityError = engine.CompatibilityError
