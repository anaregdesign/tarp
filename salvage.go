package tarp

import (
	"context"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

// SalvagedRecord is a historical Put or Delete from one physical DATA segment.
// Key and Value belong to the caller. Deleted distinguishes Delete from an empty Put.
type SalvagedRecord = storage.SalvagedRecord

// DataByteRange is a half-open range of physical DATA byte offsets.
type DataByteRange = storage.DataByteRange

// SalvageReport counts successful visitor calls and locates unrecoverable DATA blocks.
// A record crossing any of those blocks is omitted in its entirety.
// The report stores one range per contiguous group of missing bytes.
type SalvageReport = storage.SalvageReport

// SalvageData exports verified historical records from one selected sealed DATA file
// in a closed database. Use it to extract readable records across gaps left by
// damaged blocks. For an export of current live entries, use [DB.Scan].
// fileID is the hexadecimal suffix of its DATA name, parsed as a uint64.
// maxRecordBytes bounds each encoded record and defaults to 1 MiB.
// A valid PARITY sidecar is required. SalvageData leaves the source DATA untouched.
// The visitor receives records in ascending segment offset order, including superseded Puts and
// Deletes. They carry no claim of current key state or complete batch membership.
// Stage exported records until the call returns and inspect the error and gaps
// before publishing output. Final validation can fail after earlier callbacks.
func SalvageData(ctx context.Context, dir string, fileID uint64, maxRecordBytes int64, visit func(SalvagedRecord) error) (SalvageReport, error) {
	return engine.SalvageData(ctx, dir, fileID, maxRecordBytes, visit)
}
