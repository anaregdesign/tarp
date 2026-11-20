// Package publication describes the result of publishing a durable operation.
package publication

// Outcome describes publication of the requested operation.
type Outcome string

const (
	NotApplied Outcome = "not-applied"
	Committed  Outcome = "committed"
	Unknown    Outcome = "unknown"
)
