package storage

import "github.com/anaregdesign/tarp/internal/index"

// Address removes the process-local file handle from a location.
func (loc Location) Address() index.Address {
	return index.Address{FileID: loc.File.ID, Offset: loc.Offset, Bytes: loc.Bytes, ValueBytes: loc.ValueBytes, Sequence: loc.Sequence}
}

// At binds an indexed record to the DATA file protected by the caller's lease.
func At(file *DataSegment, a index.Address) Location {
	return Location{File: file, Offset: a.Offset, Bytes: a.Bytes, ValueBytes: a.ValueBytes, Sequence: a.Sequence}
}
