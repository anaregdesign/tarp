package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

type Relocation struct {
	Key       string
	Old, Next Location
}

// Relocations keeps one small batch in memory. Larger operations spill all
// moves into an unnamed file in the owning DB workspace.
type Relocations struct {
	small  []Relocation
	fd     *os.File
	writer *bufio.Writer
	files  map[uint64]*DataSegment
	count  int
	maxKey int
	bytes  int64
	store  *Store
}

const (
	relocationHeader = 7*8 + 4
	moveBatchBytes   = 8 << 10

	// RelocationBatchCount is the default number of moves to stage or replay at once.
	RelocationBatchCount = 32
)

// NewRelocations starts an in-memory batch and opens its workspace only on spill.
func (s *Store) NewRelocations() *Relocations { return &Relocations{store: s} }

func (m *Relocations) open() error {
	dir, err := m.store.WorkspaceDir()
	if err != nil {
		return err
	}
	fd, err := platform.OpenScratch(dir, "tarp-relocations-*")
	if err != nil {
		return err
	}
	m.fd = fd
	m.writer = bufio.NewWriterSize(fd, 32<<10)
	m.files = make(map[uint64]*DataSegment)
	return nil
}

func (m *Relocations) append(key string, old, next Location) error {
	err := m.store.IO.Do("index.moves.write", func() error {
		move := Relocation{Key: key, Old: old, Next: next}
		if m.fd == nil {
			if m.count < RelocationBatchCount && int64(len(key)) <= moveBatchBytes-relocationHeader-m.bytes {
				m.small = append(m.small, move)
				m.bytes += relocationHeader + int64(len(key))
				m.count++
				return nil
			}
			if err := m.open(); err != nil {
				return err
			}
			m.bytes = 0
			for _, prev := range m.small {
				if err := m.write(prev); err != nil {
					return err
				}
			}
			m.small = nil
		}
		if err := m.write(move); err != nil {
			return err
		}
		m.count++
		return nil
	})
	if err != nil {
		return &IndexWorkError{Err: err}
	}
	return nil
}

func (m *Relocations) write(move Relocation) error {
	old, next := move.Old, move.Next
	var h [relocationHeader]byte
	put := binary.LittleEndian.PutUint64
	put(h[0:], old.File.ID)
	put(h[8:], uint64(old.Offset))
	put(h[16:], next.File.ID)
	put(h[24:], uint64(next.Offset))
	put(h[32:], uint64(old.Bytes))
	put(h[40:], uint64(old.ValueBytes))
	put(h[48:], old.Sequence)
	size, err := writeScratchRecord(m.writer, h[:], move.Key, m.bytes)
	if err != nil {
		return err
	}
	m.bytes = size
	m.files[old.File.ID] = old.File
	m.files[next.File.ID] = next.File
	m.maxKey = max(m.maxKey, len(move.Key))
	return nil
}

// seal finishes all buffered writes before CURRENT can select the new layout.
func (m *Relocations) seal() error {
	if m.count == 0 {
		return nil
	}
	if err := m.store.IO.Do("index.moves.flush", func() error {
		if m.writer == nil {
			return nil
		}
		return m.writer.Flush()
	}); err != nil {
		return &IndexWorkError{Err: err}
	}
	m.writer = nil
	return nil
}

// Resident returns the read-only batch when enumeration needs no scratch I/O.
func (m *Relocations) Resident() ([]Relocation, bool) { return m.small, m.fd == nil }

func (m *Relocations) Close() error {
	if m.fd == nil {
		return nil
	}
	return m.fd.Close()
}

func (m *Relocations) Each(visit func(key string, old, next Location) error) error {
	return m.EachBatch(RelocationBatchCount, func(batch []Relocation) error {
		for _, move := range batch {
			if err := visit(move.Key, move.Old, move.Next); err != nil {
				return err
			}
		}
		return nil
	})
}

// EachBatch streams up to maxCount moves (which must be positive) within
// the byte limit. A larger key is visited by itself. The caller must not
// retain the slice after returning.
func (m *Relocations) EachBatch(maxCount int, visit func([]Relocation) error) error {
	if m.count == 0 {
		return nil
	}
	if m.fd == nil {
		for batch := range slices.Chunk(m.small, maxCount) {
			if err := visit(batch); err != nil {
				return err
			}
		}
		return nil
	}
	batch := make([]Relocation, min(m.count, maxCount))
	used, size := 0, 0
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
		if used > 0 && (used == len(batch) || len(key) > moveBatchBytes-relocationHeader-size) {
			if err := visit(batch[:used]); err != nil {
				return err
			}
			clear(batch[:used])
			used, size = 0, 0
		}
		batch[used] = Relocation{Key: key, Old: old, Next: next}
		used++
		size += relocationHeader + len(key)
	}
	if left != 0 {
		return &IndexWorkError{Err: errors.New("invalid relocation length")}
	}
	return visit(batch[:used])
}
