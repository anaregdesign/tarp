package storage

import "github.com/anaregdesign/tarp/internal/index"

// Address replaces the process-local DATA segment reference with its file ID.
func (loc Location) Address() index.Address {
	return index.Address{FileID: loc.Segment.ID, RecordOffset: loc.RecordOffset, RecordBytes: loc.RecordBytes, ValueBytes: loc.ValueBytes, Sequence: loc.Sequence}
}

// At resolves a record address to its process-local DATA segment. The caller
// keeps the segment available while using the location.
func At(segment *DataSegment, a index.Address) Location {
	return Location{Segment: segment, RecordOffset: a.RecordOffset, RecordBytes: a.RecordBytes, ValueBytes: a.ValueBytes, Sequence: a.Sequence}
}
