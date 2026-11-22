package storage

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

// recoveredBlocks holds one private DATA image and a three-byte descriptor per
// block in the same unnamed file. The trailing descriptors distinguish original
// or corrected blocks from raw bytes in stripes exceeding their parity budget.
type recoveredBlocks struct {
	*os.File
	end, rangesAt, ranges, unowned int64
}

// An EIO is not authority to permanently discard a key. The parity codec
// normally counts EIO as an erasure for full-file reconstruction. Remember it
// here so a best-effort checkpoint cannot turn an I/O outage into data loss.
type recoveringReader struct {
	io.ReaderAt
	fs  *IO
	err error
}

func (r *recoveringReader) ReadAt(b []byte, off int64) (int, error) {
	var n int
	err := r.fs.Check("compact.recover.source:before")
	if err == nil {
		n, err = r.ReaderAt.ReadAt(b, off)
	}
	if errors.Is(err, syscall.EIO) {
		r.err = err
	}
	return n, err
}

// Classify failures on the PARITY descriptor before the decoder can also
// encounter a failure on the separate private recovery output.
type recoveryParityReader struct {
	io.Reader
	fs          *IO
	unavailable func(error) error
}

func (r recoveryParityReader) Read(b []byte) (int, error) {
	var n int
	err := r.fs.Check("compact.recover.parity:before")
	if err == nil {
		n, err = r.Reader.Read(b)
	}
	if errors.Is(err, syscall.EIO) {
		err = r.unavailable(err)
	}
	return n, err
}

func (c *DataCheckpoint) stageBlocks(ctx context.Context, src CapturedSegment) (_ *recoveredBlocks, result error) {
	e := src.Entry
	unavailable := func(err error) error {
		if rootErr := c.store.checkRepairRoot(c.capture.Current.UUID); rootErr != nil {
			return errors.Join(err, rootErr)
		}
		return &UnavailableDataError{FileID: e.ID, Err: err}
	}
	side, err := OpenRegular(filepath.Join(c.store.Dir, parityName(c.capture.Current.UUID, e.ID)))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.EIO) {
			return nil, unavailable(err)
		}
		return nil, err
	}
	defer func() { result = errors.Join(result, side.Close()) }()
	var original io.ReaderAt = bytes.NewReader(nil)
	fd, err := OpenRegular(src.Segment.Path)
	if err == nil {
		defer func() { result = errors.Join(result, fd.Close()) }()
		original = fd
	} else if errors.Is(err, syscall.EIO) {
		return nil, unavailable(err)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	data := &recoveringReader{ReaderAt: original, fs: c.store.IO}
	// Only the original segment header authenticates raw bytes from a block
	// that parity could not verify. A reconstructed first block identifies the
	// desired segment, but cannot identify the file actually present on disk.
	var header [format.SegmentHeaderBytes]byte
	n, err := data.ReadAt(header[:], 0)
	if data.err != nil {
		return nil, unavailable(data.err)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	useRaw := n == len(header) && verifySegmentHeader(header[:], c.capture.Current.UUID, e) == nil
	dir, err := c.store.WorkspaceDir()
	if err != nil {
		return nil, err
	}
	scratch, err := platform.OpenScratch(dir, "tarp-checkpoint-recover-*")
	if err != nil {
		return nil, err
	}
	// The caller owns stage even if a later DATA or sidecar operation fails.
	stage := &recoveredBlocks{File: scratch, end: e.Bytes}
	blocks := (e.Bytes-1)/format.DataBlockBytes + 1
	if blocks > (math.MaxInt64-e.Bytes)/3 {
		return stage, fmt.Errorf("storage: recovery image too large")
	}
	stage.rangesAt = e.Bytes + blocks*3
	dataHash, descHash := sha256.New(), sha256.New()
	var raw [format.DataBlockBytes]byte
	var desc [3]byte
	reader := bufio.NewReaderSize(recoveryParityReader{side, c.store.IO, unavailable}, parityIOBufferBytes(e.Bytes))
	err = format.WalkDataBlocksFromParity(ctx, data, reader, c.capture.Current.UUID, e, func(off int64, first uint16, block []byte) error {
		if data.err != nil {
			return unavailable(data.err)
		}
		binary.LittleEndian.PutUint16(desc[:], first)
		desc[2] = 0
		if block == nil {
			desc[2] = 1
			block = raw[:min(int64(len(raw)), e.Bytes-off)]
			if useRaw {
				n, err := data.ReadAt(block, off)
				if data.err != nil {
					return unavailable(data.err)
				}
				if err != nil && !errors.Is(err, io.EOF) {
					return err
				}
				if n < len(block) && err == nil {
					return io.ErrNoProgress
				}
				clear(block[n:])
			} else {
				clear(block)
			}
		}
		dataHash.Write(block)
		descHash.Write(desc[:])
		if err := c.store.IO.Write(scratch, block, off, "compact.recover.data"); err != nil {
			return err
		}
		return c.store.IO.Write(scratch, desc[:], e.Bytes+off/format.DataBlockBytes*3, "compact.recover.descriptor")
	})
	if errors.Is(err, format.ErrInvalidParity) {
		return stage, unavailable(err)
	}
	if err != nil {
		return stage, err
	}
	// Read back both regions before treating any private byte as evidence of
	// source damage. Output failures must never expand the allowed loss.
	for _, region := range []struct {
		off, n int64
		sum    []byte
	}{{0, e.Bytes, dataHash.Sum(nil)}, {e.Bytes, blocks * 3, descHash.Sum(nil)}} {
		h := sha256.New()
		var buf [32 << 10]byte
		for n := int64(0); n < region.n; {
			if err := ctx.Err(); err != nil {
				return stage, err
			}
			chunk := buf[:min(int64(len(buf)), region.n-n)]
			if _, err := scratch.ReadAt(chunk, region.off+n); err != nil {
				return stage, err
			}
			h.Write(chunk)
			n += int64(len(chunk))
		}
		if !bytes.Equal(h.Sum(nil), region.sum) {
			return stage, errors.New("storage: private recovery image checksum mismatch")
		}
	}
	return stage, nil
}

func (b *recoveredBlocks) descriptor(i int64) (uint16, bool, error) {
	var d [3]byte
	_, err := b.ReadAt(d[:], b.end+i*3)
	return binary.LittleEndian.Uint16(d[:]), d[2] != 0, err
}

func (b *recoveredBlocks) overlapsGap(off, size int64) (bool, error) {
	for i := off / format.DataBlockBytes; i <= (off+size-1)/format.DataBlockBytes; i++ {
		_, gap, err := b.descriptor(i)
		if err != nil || gap {
			return gap, err
		}
	}
	return false, nil
}

// nextAnchor resumes at an original record start even when its preceding
// record's length could not be decoded. A valid raw record at the anchored
// offset can still be rescued when other bytes in the block are damaged.
func (b *recoveredBlocks) nextAnchor(ctx context.Context, off int64) (int64, error) {
	for i := off / format.DataBlockBytes; i <= (b.end-1)/format.DataBlockBytes; i++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		anchor, _, err := b.descriptor(i)
		if err != nil {
			return 0, err
		}
		next := i*format.DataBlockBytes + int64(anchor)
		if anchor != format.NoRecordAnchor && next > off {
			return next, nil
		}
	}
	return b.end, nil
}

// The scan records only independently established damaged record extents. An
// unparsed interval can contain multiple records when a header was lost. A
// parsed interval belongs to exactly one record. The sorted private spool
// bounds memory for very large files and rejects forged offsets inside
// readable records even if another byte in the same DATA block was damaged.
const recoveryRangeBytes = 21
const (
	rangeUnparsed byte = 1 << iota
	rangeNeedsOwner
	rangeClaimed
)

func (b *recoveredBlocks) addRange(start, end int64, flags byte) error {
	var p [recoveryRangeBytes]byte
	binary.LittleEndian.PutUint64(p[:], uint64(start))
	binary.LittleEndian.PutUint64(p[8:], uint64(end))
	p[16] = flags
	if b.ranges >= (math.MaxInt64-b.rangesAt)/recoveryRangeBytes {
		return errors.New("storage: recovery range spool too large")
	}
	if err := b.writeRange(b.ranges, &p); err != nil {
		return err
	}
	b.ranges++
	if flags&rangeNeedsOwner != 0 {
		b.unowned++
	}
	return nil
}

func (b *recoveredBlocks) writeRange(index int64, p *[recoveryRangeBytes]byte) error {
	binary.LittleEndian.PutUint32(p[17:], crc32.ChecksumIEEE(p[:17]))
	_, err := b.WriteAt(p[:], b.rangesAt+index*recoveryRangeBytes)
	return err
}

func (b *recoveredBlocks) claimDamage(start, end int64) (bool, error) {
	for lo, hi := int64(0), b.ranges; lo < hi; {
		mid := lo + (hi-lo)/2
		var p [recoveryRangeBytes]byte
		if _, err := b.ReadAt(p[:], b.rangesAt+mid*recoveryRangeBytes); err != nil {
			return false, err
		}
		if crc32.ChecksumIEEE(p[:17]) != binary.LittleEndian.Uint32(p[17:]) {
			return false, errors.New("storage: recovery range checksum mismatch")
		}
		a, z := int64(binary.LittleEndian.Uint64(p[:])), int64(binary.LittleEndian.Uint64(p[8:]))
		switch {
		case start < a:
			hi = mid
		case start >= z:
			lo = mid + 1
		case p[16]&rangeUnparsed != 0:
			return end <= z, nil
		case start != a || end != z:
			return false, nil
		default:
			if p[16]&rangeClaimed != 0 {
				return false, fmt.Errorf("%w: multiple keys claim one damaged record", ErrCorrupt)
			}
			if p[16]&rangeNeedsOwner != 0 {
				b.unowned--
			}
			p[16] |= rangeClaimed
			return true, b.writeRange(mid, &p)
		}
	}
	return false, nil
}
