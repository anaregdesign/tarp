package index

import "context"

// SortedBuilder can construct an initially empty backend from a strictly
// increasing stream of live binary keys. The caller sorts and validates
// record history. A failure leaves the backend empty.
type SortedBuilder interface {
	// BulkWorkspace gives the scratch location and memory allowance for
	// external sorting alongside this backend during exclusive construction.
	BulkWorkspace() (directory string, bytes int64)
	BuildSorted(ctx context.Context, produce func(put func(string, Address) error) error) error
}
