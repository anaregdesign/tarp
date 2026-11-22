package storage

import (
	"bufio"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/platform"
)

// A replay event carries the record address and kinds used to rebuild the index.
type replayEvent struct {
	key        string
	addr       index.Address
	fileKind   uint32
	recordKind uint8
}

// sortedReplay groups events by encoded key and preserves their input order
// within each key. It keeps one replay chunk and two merge cursors in memory.
type sortedReplay struct {
	ctx         context.Context
	dir         string
	limit, used int64
	chunk       []replayEvent
	// Consecutive ordered replay chunks can share a single on-disk sort run.
	// Sort runs merge in pairs. Each level holds at most one older run.
	pending *replayRun
	last    string
	levels  [64]*replayRun
}

func (s *sortedReplay) apply(f *DataSegment, off int64, r format.RecordHeader, key string) error {
	addr := index.Address{FileID: f.ID, RecordOffset: off, RecordBytes: r.EncodedRecordBytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence}
	return replayWorkError(s.add(replayEvent{key: key, addr: addr, fileKind: f.Kind, recordKind: r.Kind}))
}

func (s *sortedReplay) add(e replayEvent) error {
	charge := int64(len(e.key)) + replayKeyCharge
	if s.used != 0 && charge > s.limit-s.used {
		if err := s.flushChunk(); err != nil {
			return err
		}
	}
	s.chunk = append(s.chunk, e)
	s.used += charge
	return nil
}

func orderEvents(chunk []replayEvent) {
	slices.SortStableFunc(chunk, func(a, b replayEvent) int { return cmp.Compare(a.key, b.key) })
}

func (s *sortedReplay) flushChunk() error {
	orderEvents(s.chunk)
	if s.pending != nil && s.last > s.chunk[0].key {
		old := s.pending
		s.pending = nil
		if err := s.push(old); err != nil {
			return err
		}
	}
	if s.pending == nil {
		run, err := newReplayRun(s.dir)
		if err != nil {
			return err
		}
		s.pending = run
	}
	w := bufio.NewWriter(s.pending.fd)
	for _, e := range s.chunk {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if err := s.pending.write(w, e); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	s.last = s.chunk[len(s.chunk)-1].key
	clear(s.chunk)
	s.chunk = s.chunk[:0]
	s.used = 0
	return nil
}

// push takes ownership of r. Equal keys from the older run are always read
// first, so sorting requires neither a sequence-sized offset nor a key map.
func (s *sortedReplay) push(r *replayRun) error {
	for i, old := range s.levels {
		if old == nil {
			s.levels[i] = r
			return nil
		}
		s.levels[i] = nil
		merged, err := s.merge(old, r)
		if err != nil {
			return err
		}
		r = merged
	}
	return errors.Join(errors.New("too many replay runs"), r.fd.Close())
}

func (s *sortedReplay) merge(old, newer *replayRun) (result *replayRun, err error) {
	defer func() {
		err = errors.Join(err, old.fd.Close(), newer.fd.Close())
		if err != nil && result != nil {
			err = errors.Join(err, result.fd.Close())
			result = nil
		}
	}()
	result, err = newReplayRun(s.dir)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(result.fd)
	err = mergeReplay(s.ctx, old.cursor(), newer.cursor(), func(e replayEvent) error { return result.write(w, e) })
	if err == nil {
		err = w.Flush()
	}
	return result, err
}

func (s *sortedReplay) close() (err error) {
	if s.pending != nil {
		err = s.pending.fd.Close()
		s.pending = nil
	}
	for i, run := range s.levels {
		if run != nil {
			err = errors.Join(err, run.fd.Close())
			s.levels[i] = nil
		}
	}
	return err
}

func (s *sortedReplay) each(visit func(replayEvent) error) error {
	// Verification may have reserved its first run to check the scratch directory.
	// Keep the final reduction in memory if no chunk ever spilled into that file.
	if s.pending == nil || s.pending.bytes == 0 {
		orderEvents(s.chunk)
		for _, e := range s.chunk {
			if err := s.ctx.Err(); err != nil {
				return err
			}
			if err := visit(e); err != nil {
				return err
			}
		}
		return s.ctx.Err()
	}
	if len(s.chunk) > 0 {
		if err := s.flushChunk(); err != nil {
			return err
		}
	}
	// The newest run is pending. Increasing levels contain successively
	// older runs. Merging in this order keeps the total I/O logarithmic.
	for i, old := range s.levels {
		if old == nil {
			continue
		}
		s.levels[i] = nil
		newer := s.pending
		s.pending = nil
		merged, err := s.merge(old, newer)
		if err != nil {
			return err
		}
		s.pending = merged
	}
	r := s.pending.cursor()
	for r.left > 0 {
		if err := s.ctx.Err(); err != nil {
			return err
		}
		e, err := r.next()
		if err != nil {
			return err
		}
		if err := visit(e); err != nil {
			return err
		}
	}
	return s.ctx.Err()
}

// reduce validates the same key transitions as chronological replay. The
// record sequence is preserved as data; it need not equal traversal order.
func (s *sortedReplay) reduce(pruned uint64, emit func(string, index.Address) error) error {
	var key string
	var addr index.Address
	present := false
	flush := func() error {
		if !present || emit == nil {
			return nil
		}
		return emit(key, addr)
	}
	err := s.each(func(e replayEvent) error {
		if key != e.key {
			if err := flush(); err != nil {
				return err
			}
			key = e.key
			present = false
		}
		if err := validateKey(e.fileKind, format.RecordHeader{Kind: e.recordKind, Sequence: e.addr.Sequence}, present, pruned); err != nil {
			return &SourceError{Err: fmt.Errorf("DATA %d offset %d: %w", e.addr.FileID, e.addr.RecordOffset, err)}
		}
		present = e.recordKind != format.Delete
		addr = e.addr
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}

// A replayRun is a sorted stream of replay events in a scratch file.
type replayRun struct {
	fd     *os.File
	bytes  int64
	maxKey int
}

func newReplayRun(dir string) (*replayRun, error) {
	fd, err := platform.OpenScratch(dir, "tarp-replay-*")
	if err != nil {
		return nil, err
	}
	return &replayRun{fd: fd}, nil
}

// The scratch header and exact key are protected by one CRC. The key length
// can be derived from record size, value size and the DATA record header.
const replayHeaderBytes = 49

func (r *replayRun) write(w *bufio.Writer, e replayEvent) error {
	var h [replayHeaderBytes]byte
	put := binary.LittleEndian.PutUint64
	put(h[0:], e.addr.FileID)
	put(h[8:], uint64(e.addr.RecordOffset))
	put(h[16:], uint64(e.addr.RecordBytes))
	put(h[24:], uint64(e.addr.ValueBytes))
	put(h[32:], e.addr.Sequence)
	binary.LittleEndian.PutUint32(h[40:], e.fileKind)
	h[44] = e.recordKind
	size, err := writeScratchRecord(w, h[:], e.key, r.bytes)
	if err != nil {
		return err
	}
	r.bytes = size
	r.maxKey = max(r.maxKey, len(e.key))
	return nil
}

type runCursor struct {
	r      *bufio.Reader
	left   int64
	maxKey int
}

func (r *replayRun) cursor() *runCursor {
	return &runCursor{r: bufio.NewReader(io.NewSectionReader(r.fd, 0, r.bytes)), left: r.bytes, maxKey: r.maxKey}
}

func (r *runCursor) next() (replayEvent, error) {
	var h [replayHeaderBytes]byte
	if _, err := io.ReadFull(r.r, h[:]); err != nil {
		return replayEvent{}, err
	}
	get := binary.LittleEndian.Uint64
	a := index.Address{FileID: get(h[0:]), RecordOffset: int64(get(h[8:])), RecordBytes: int64(get(h[16:])), ValueBytes: int64(get(h[24:])), Sequence: get(h[32:])}
	r.left -= replayHeaderBytes
	key, err := readScratchKey(r.r, h[:], a.RecordBytes, a.ValueBytes, r.left, r.maxKey)
	if err != nil {
		return replayEvent{}, err
	}
	r.left -= int64(len(key))
	return replayEvent{key: key, addr: a, fileKind: binary.LittleEndian.Uint32(h[40:]), recordKind: h[44]}, nil
}

func mergeReplay(ctx context.Context, a, b *runCursor, visit func(replayEvent) error) error {
	var left, right replayEvent
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		if left.key == "" && a.left > 0 {
			if left, err = a.next(); err != nil {
				return err
			}
		}
		if right.key == "" && b.left > 0 {
			if right, err = b.next(); err != nil {
				return err
			}
		}
		if left.key == "" && right.key == "" {
			return nil
		}
		next := &left
		if left.key == "" || (right.key != "" && right.key < left.key) {
			next = &right
		}
		if err := visit(*next); err != nil {
			return err
		}
		*next = replayEvent{}
	}
}
