package storage

import (
	"context"
	"io"
	"math"
	"sync"

	"github.com/anaregdesign/tarp/internal/format"
)

// MaxAdjacentReadBytes bounds each caller's pooled DATA read buffer.
const MaxAdjacentReadBytes = 8 << 10

var adjacentBuffers = sync.Pool{New: func() any { return new([MaxAdjacentReadBytes]byte) }}

type AdjacentRead struct {
	Location
	Key   []byte
	Value []byte
}

// ReadAdjacent reads a single range from one pinned file. Each entry retains
// its own identity, checksum check, fault boundaries and owned value.
func (s *Store) ReadAdjacent(group []AdjacentRead) error {
	if len(group) == 0 {
		return nil
	}
	first := group[0].Location
	end := first.Offset
	for i := range group {
		r := &group[i]
		if r.File != first.File || r.Offset != end || end < 0 || r.Bytes <= 0 ||
			r.Bytes > MaxAdjacentReadBytes-(end-first.Offset) || end > math.MaxInt64-r.Bytes {
			return r.readError(ErrCorrupt)
		}
		if err := s.IO.Check("read:before"); err != nil {
			return r.readError(err)
		}
		end += r.Bytes
	}
	buf := adjacentBuffers.Get().(*[MaxAdjacentReadBytes]byte)
	defer adjacentBuffers.Put(buf)
	b := buf[:end-first.Offset]
	loc, err, physical := s.readAdjacentValues(group, b)
	if physical {
		if err = s.RepairRead(context.Background(), first.File, err); err != nil {
			return loc.readError(err)
		}
		loc, err, _ = s.readAdjacentValues(group, b)
	}
	if err != nil {
		return loc.readError(err)
	}
	return nil
}

// physical identifies DATA read or decoding errors eligible for repair.
func (s *Store) readAdjacentValues(group []AdjacentRead, b []byte) (loc Location, err error, physical bool) {
	first := group[0].Location
	h, err := s.readers.acquire(first.File)
	if err != nil {
		return first, err, true
	}
	if err := s.IO.Check("read.adjacent:before"); err != nil {
		s.readers.release(h)
		return first, err, false
	}
	n, err := h.fd.ReadAt(b, first.Offset)
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
			if r.Offset+r.Bytes-first.Offset > int64(n) {
				return r.Location, err, true
			}
		}
		return group[len(group)-1].Location, err, true
	}
	if err := s.IO.Check("read.adjacent:after"); err != nil {
		return group[len(group)-1].Location, err, false
	}
	for i := range group {
		r := &group[i]
		offset := r.Offset - first.Offset
		r.Value, err = format.DecodeIndexedValue(b[offset:offset+r.Bytes], r.Key, r.Sequence)
		if err != nil {
			return r.Location, err, true
		}
		if err := s.IO.Check("read:after"); err != nil {
			return r.Location, err, false
		}
	}
	return first, nil, false
}
