package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"
	"os"

	"github.com/anaregdesign/tarp/internal/index/btree"
)

const indexHashBlockSize = 4096
const indexHashFanout = indexHashBlockSize / sha256.Size

// Each level groups the hashes from the preceding level into fixed-size
// blocks. The first level authenticates packed pages, and the hash of the
// single top block is included in the selected INDEX summary.
type indexHashLevel struct {
	offset, blocks int64
}

func indexHashLayout(packedStart, packedSize int64) ([]indexHashLevel, int64, error) {
	if packedSize < btree.PackedHeaderBytes || packedStart > math.MaxInt64-packedSize ||
		(packedSize-btree.PackedHeaderBytes)%btree.PackedPageBytes != 0 {
		return nil, 0, ErrIndexCheckpointUnusable
	}
	end := packedStart + packedSize
	pages := (packedSize - btree.PackedHeaderBytes) / btree.PackedPageBytes
	var levels []indexHashLevel
	for pages != 0 {
		pages = (pages-1)/indexHashFanout + 1
		if pages > (math.MaxInt64-end)/indexHashBlockSize {
			return nil, 0, ErrIndexCheckpointUnusable
		}
		levels = append(levels, indexHashLevel{offset: end, blocks: pages})
		end += pages * indexHashBlockSize
		if pages == 1 {
			break
		}
	}
	return levels, end, nil
}

type indexHashBuffer struct {
	data    [indexHashBlockSize]byte
	used    int
	written int64
}

// The packed exporter writes its header and then one page per call. Hash
// blocks are written at known offsets after the packed image even when pages
// are still buffered, so only one hash block per level needs to stay resident.
type indexPageWriter struct {
	packed    io.Writer
	fs        *IO
	fd        *os.File
	start     int64
	header    []byte
	size, end int64
	written   int64
	levels    []indexHashLevel
	buffers   []indexHashBuffer
	root      [sha256.Size]byte
}

func (w *indexPageWriter) Write(p []byte) (int, error) {
	if w.header == nil {
		if len(p) != btree.PackedHeaderBytes {
			return 0, ErrIndexCheckpointUnusable
		}
		return w.writeHeader(p, binary.LittleEndian.Uint64(p[24:32]))
	}
	if len(p) != btree.PackedPageBytes || int64(len(p)) > w.size-w.written {
		return 0, ErrIndexCheckpointUnusable
	}
	if err := w.add(0, sha256.Sum256(p)); err != nil {
		return 0, err
	}
	return w.write(p)
}

func (w *indexPageWriter) writeHeader(p []byte, pages uint64) (int, error) {
	if len(p) != btree.PackedHeaderBytes || w.header != nil ||
		pages > uint64((math.MaxInt64-btree.PackedHeaderBytes)/btree.PackedPageBytes) {
		return 0, ErrIndexCheckpointUnusable
	}
	w.size = btree.PackedHeaderBytes + int64(pages)*btree.PackedPageBytes
	var err error
	w.levels, w.end, err = indexHashLayout(w.start, w.size)
	if err != nil {
		return 0, err
	}
	w.buffers = make([]indexHashBuffer, len(w.levels))
	w.header = append([]byte(nil), p...)
	return w.write(p)
}

func (w *indexPageWriter) write(p []byte) (int, error) {
	n, err := w.packed.Write(p)
	w.written += int64(n)
	return n, err
}

func (w *indexPageWriter) add(level int, digest [sha256.Size]byte) error {
	if level == len(w.buffers) {
		w.root = digest
		return nil
	}
	block := &w.buffers[level]
	copy(block.data[block.used:], digest[:])
	block.used += sha256.Size
	if block.used != len(block.data) {
		return nil
	}
	return w.flush(level)
}

func (w *indexPageWriter) flush(level int) error {
	block := &w.buffers[level]
	layout := w.levels[level]
	if block.written == layout.blocks {
		return ErrIndexCheckpointUnusable
	}
	if err := w.fs.Write(w.fd, block.data[:], layout.offset+block.written*indexHashBlockSize, "index.hash.write"); err != nil {
		return err
	}
	digest := sha256.Sum256(block.data[:])
	*block = indexHashBuffer{written: block.written + 1}
	return w.add(level+1, digest)
}

func (w *indexPageWriter) finish() error {
	if w.header == nil || w.written != w.size {
		return ErrIndexCheckpointUnusable
	}
	for i := range w.buffers {
		if w.buffers[i].used != 0 {
			if err := w.flush(i); err != nil {
				return err
			}
		}
		if w.buffers[i].written != w.levels[i].blocks {
			return ErrIndexCheckpointUnusable
		}
	}
	return nil
}

type verifiedIndexPages struct {
	r      io.ReaderAt
	start  int64
	size   int64
	root   [sha256.Size]byte
	header []byte
	levels []indexHashLevel
	cached []indexHashCache
}

type indexHashCache struct {
	data   [indexHashBlockSize]byte
	number int64
	valid  bool
}

func (v *verifiedIndexPages) ReadAt(p []byte, offset int64) (int, error) {
	if offset == 0 && len(p) == len(v.header) {
		return copy(p, v.header), nil
	}
	pageOffset := offset - btree.PackedHeaderBytes
	if offset < btree.PackedHeaderBytes || offset > v.size-btree.PackedPageBytes ||
		len(p) != btree.PackedPageBytes || pageOffset%btree.PackedPageBytes != 0 {
		return 0, ErrIndexCheckpointUnusable
	}
	want, err := v.hash(0, pageOffset/btree.PackedPageBytes)
	if err != nil {
		return 0, err
	}
	n, err := v.r.ReadAt(p, v.start+offset)
	if err != nil {
		return n, indexPageReadError(err)
	}
	if sha256.Sum256(p) != want {
		return 0, indexPageCorruption{}
	}
	return n, nil
}

func (v *verifiedIndexPages) hash(level int, child int64) ([sha256.Size]byte, error) {
	if level == len(v.levels) {
		return v.root, nil
	}
	layout := v.levels[level]
	block, offset := child/indexHashFanout, child%indexHashFanout*sha256.Size
	entry := &v.cached[level]
	if !entry.valid || entry.number != block {
		entry.valid = false
		want, err := v.hash(level+1, block)
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		if _, err := v.r.ReadAt(entry.data[:], layout.offset+block*indexHashBlockSize); err != nil {
			return [sha256.Size]byte{}, indexPageReadError(err)
		}
		if sha256.Sum256(entry.data[:]) != want {
			return [sha256.Size]byte{}, indexPageCorruption{}
		}
		entry.number, entry.valid = block, true
	}
	return [sha256.Size]byte(entry.data[offset : offset+sha256.Size]), nil
}

func indexPageReadError(err error) error {
	if IsIndexCheckpointUnusable(err, io.EOF, io.ErrUnexpectedEOF) {
		return indexPageCorruption{}
	}
	return err
}
