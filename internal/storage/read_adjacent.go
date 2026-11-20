package storage

import (
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
	h, err := s.readers.acquire(first.File)
	if err != nil {
		return first.readError(err)
	}
	buf := adjacentBuffers.Get().(*[MaxAdjacentReadBytes]byte)
	defer adjacentBuffers.Put(buf)
	b := buf[:end-first.Offset]
	var n int
	if err = s.IO.Check("read.adjacent:before"); err == nil {
		n, err = h.fd.ReadAt(b, first.Offset)
		if n == len(b) && err == io.EOF {
			err = nil
		}
		if err == nil && n != len(b) {
			err = io.ErrUnexpectedEOF
		}
		if err == nil {
			err = s.IO.Check("read.adjacent:after")
		}
	}
	s.readers.release(h)
	if err != nil {
		for _, r := range group {
			if r.Offset+r.Bytes-first.Offset > int64(n) {
				return r.readError(err)
			}
		}
		return group[len(group)-1].readError(err)
	}
	for i := range group {
		r := &group[i]
		offset := r.Offset - first.Offset
		r.Value, err = format.DecodeIndexedValue(b[offset:offset+r.Bytes], r.Key, r.Sequence)
		if err != nil {
			return r.readError(err)
		}
		if err := s.IO.Check("read:after"); err != nil {
			return r.readError(err)
		}
	}
	return nil
}
