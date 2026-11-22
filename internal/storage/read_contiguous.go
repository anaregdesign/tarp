package storage

import (
	"context"
	"io"
	"math"
	"sync"

	"github.com/anaregdesign/tarp/internal/format"
)

// MaxContiguousReadBytes bounds each caller's pooled DATA read buffer.
const MaxContiguousReadBytes = 8 << 10

var contiguousBuffers = sync.Pool{New: func() any { return new([MaxContiguousReadBytes]byte) }}

type ContiguousRead struct {
	Location
	Key   []byte
	Value []byte
}

// ReadContiguous reads contiguous DATA records from one pinned segment. Each record retains
// its own identity, checksum check, fault boundaries and owned value.
func (s *Store) ReadContiguous(group []ContiguousRead) error {
	if len(group) == 0 {
		return nil
	}
	first := group[0].Location
	end := first.RecordOffset
	for i := range group {
		r := &group[i]
		if r.Segment != first.Segment || r.RecordOffset != end || end < 0 || r.RecordBytes <= 0 ||
			r.RecordBytes > MaxContiguousReadBytes-(end-first.RecordOffset) || end > math.MaxInt64-r.RecordBytes {
			return r.readError(ErrCorrupt)
		}
		if err := s.IO.Check("read:before"); err != nil {
			return r.readError(err)
		}
		end += r.RecordBytes
	}
	buf := contiguousBuffers.Get().(*[MaxContiguousReadBytes]byte)
	defer contiguousBuffers.Put(buf)
	b := buf[:end-first.RecordOffset]
	loc, err, sourceFailure := s.readContiguousValues(group, b)
	if sourceFailure {
		if err = s.RepairRead(context.Background(), first.Segment, err); err != nil {
			return loc.readError(err)
		}
		loc, err, _ = s.readContiguousValues(group, b)
	}
	if err != nil {
		return loc.readError(err)
	}
	return nil
}

// sourceFailure identifies DATA read or decoding errors eligible for repair.
func (s *Store) readContiguousValues(group []ContiguousRead, b []byte) (loc Location, err error, sourceFailure bool) {
	first := group[0].Location
	h, err := s.readers.acquire(first.Segment)
	if err != nil {
		return first, err, true
	}
	if err := s.IO.Check("read.contiguous:before"); err != nil {
		s.readers.release(h)
		return first, err, false
	}
	n, err := h.fd.ReadAt(b, first.RecordOffset)
	if n == len(b) && err == io.EOF {
		err = nil
	}
	if err == nil && n == len(b) {
		err = h.verifyReadIdentity()
		if err != nil {
			s.readers.release(h)
			return first, err, true
		}
	}
	s.readers.release(h)
	if err == nil && n != len(b) {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		for _, r := range group {
			if r.RecordOffset+r.RecordBytes-first.RecordOffset > int64(n) {
				return r.Location, err, true
			}
		}
		return group[len(group)-1].Location, err, true
	}
	if err := s.IO.Check("read.contiguous:after"); err != nil {
		return group[len(group)-1].Location, err, false
	}
	for i := range group {
		r := &group[i]
		offset := r.RecordOffset - first.RecordOffset
		r.Value, err = format.DecodeIndexedValue(b[offset:offset+r.RecordBytes], r.Key, r.Sequence)
		if err != nil {
			return r.Location, err, true
		}
		if err := s.IO.Check("read:after"); err != nil {
			return r.Location, err, false
		}
	}
	return first, nil, false
}
