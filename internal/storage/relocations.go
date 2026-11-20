package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

// Relocations streams the address changes owned by one maintenance operation.
// Only one record and the referenced DATA file registry need to be resident.
// The scratch file has no name after creation and never enters a backup.
type Relocations struct {
	fd     *os.File
	writer *bufio.Writer
	files  map[uint64]*File
	count  int
	maxKey int
	bytes  int64
	fs     *IO
}

const relocationHeader = 7*8 + 4

func (m *Relocations) open() error {
	fd, err := platform.OpenScratch("", "tarp-relocations-*")
	if err != nil {
		return err
	}
	m.fd = fd
	m.writer = bufio.NewWriterSize(fd, 32<<10)
	m.files = make(map[uint64]*File)
	return nil
}

func (m *Relocations) append(key string, old, next Location) error {
	err := m.fs.Do("index.moves.write", func() error {
		if m.fd == nil {
			if err := m.open(); err != nil {
				return err
			}
		}
		var h [relocationHeader]byte
		binary.LittleEndian.PutUint64(h[0:], old.File.ID)
		binary.LittleEndian.PutUint64(h[8:], uint64(old.Offset))
		binary.LittleEndian.PutUint64(h[16:], next.File.ID)
		binary.LittleEndian.PutUint64(h[24:], uint64(next.Offset))
		binary.LittleEndian.PutUint64(h[32:], uint64(old.Bytes))
		binary.LittleEndian.PutUint64(h[40:], uint64(old.ValueBytes))
		binary.LittleEndian.PutUint64(h[48:], old.Sequence)
		size, err := writeScratchRecord(m.writer, h[:], key, m.bytes)
		if err != nil {
			return err
		}
		m.bytes = size
		m.files[old.File.ID] = old.File
		m.files[next.File.ID] = next.File
		m.count++
		m.maxKey = max(m.maxKey, len(key))
		return nil
	})
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	return nil
}

// seal finishes all buffered writes before CURRENT can select the new layout.
func (m *Relocations) seal() error {
	if m.writer == nil {
		return nil
	}
	if err := m.fs.Do("index.moves.flush", m.writer.Flush); err != nil {
		return &IndexWorkError{Err: err}
	}
	m.writer = nil
	return nil
}

func (m *Relocations) Close() error {
	if m.fd == nil {
		return nil
	}
	return m.fd.Close()
}

// Each checks the spool before exposing each location to the conditional remap.
func (m *Relocations) Each(visit func(key string, old, next Location) error) error {
	if m.count == 0 {
		return nil
	}
	r := bufio.NewReaderSize(io.NewSectionReader(m.fd, 0, m.bytes), 32<<10)
	left := m.bytes
	for range m.count {
		var h [relocationHeader]byte
		if _, err := io.ReadFull(r, h[:]); err != nil {
			return &IndexWorkError{Err: err}
		}
		get := binary.LittleEndian.Uint64
		old := Location{File: m.files[get(h[0:])], Offset: int64(get(h[8:])), Bytes: int64(get(h[32:])), ValueBytes: int64(get(h[40:])), Sequence: get(h[48:])}
		next := old
		next.File, next.Offset = m.files[get(h[16:])], int64(get(h[24:]))
		if old.File == nil || next.File == nil || old.Offset < format.SegmentHeaderBytes || next.Offset < format.SegmentHeaderBytes {
			return &IndexWorkError{Err: errors.New("invalid relocation header")}
		}
		left -= relocationHeader
		key, err := readScratchKey(r, h[:], old.Bytes, old.ValueBytes, left, m.maxKey)
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		left -= int64(len(key))
		if err := visit(key, old, next); err != nil {
			return err
		}
	}
	if left != 0 {
		return &IndexWorkError{Err: errors.New("invalid relocation length")}
	}
	return nil
}
