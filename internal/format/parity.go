package format

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"syscall"
)

const (
	ParityBlockBytes = 4096
	ParityDataBlocks = 16
	ParityNoAnchor   = uint16(0xffff)

	parityStripeBytes = ParityBlockBytes * ParityDataBlocks
	parityHeaderBytes = 88
	parityAnchorStart = 8 + 4*ParityDataBlocks
	parityCRCOffset   = parityAnchorStart + 2*ParityDataBlocks
	parityMetaBytes   = parityCRCOffset + 8
)

// ErrInvalidParity identifies damaged or mismatched repair material. A corrupt
// DATA source or a stripe exceeding its repair capacity matches ErrCorrupt.
var ErrInvalidParity = fmt.Errorf("%w: invalid parity sidecar", ErrCorrupt)

// ErrParityGap identifies DATA blocks that the checked stripe cannot reconstruct.
var ErrParityGap = fmt.Errorf("%w: unrecoverable parity stripe", ErrCorrupt)

// ParityBytes returns the encoded sidecar length for a physical DATA file.
// Metadata-only compaction intervals consume no sidecar space.
func ParityBytes(dataBytes uint64) uint64 {
	if dataBytes == 0 {
		return 0
	}
	full, tail := dataBytes/parityStripeBytes, dataBytes%parityStripeBytes
	size := uint64(parityHeaderBytes) + full*(parityMetaBytes+ParityBlockBytes)
	if tail != 0 {
		size += parityMetaBytes + min(tail, ParityBlockBytes)
	}
	return size
}

func parityHeader(uuid [16]byte, e CatalogEntry) ([parityHeaderBytes]byte, error) {
	var h [parityHeaderBytes]byte
	if e.ID == 0 || e.Bytes < SegmentHeaderBytes+RecordHeaderBytes+1 || e.DataFileDigest == ([32]byte{}) ||
		(e.Kind != RawLogKind && e.Kind != SnapshotKind && e.Kind != CleanedLogKind) {
		return h, bad("parity requires a physical sealed catalog entry")
	}
	copy(h[:8], "TARPPAR2")
	copy(h[8:24], uuid[:])
	order.PutUint64(h[24:], e.ID)
	order.PutUint64(h[32:], uint64(e.Bytes))
	copy(h[40:72], e.DataFileDigest[:])
	order.PutUint32(h[72:], e.Kind)
	order.PutUint32(h[76:], ParityBlockBytes)
	order.PutUint32(h[80:], ParityDataBlocks)
	order.PutUint32(h[84:], crc32.Checksum(h[:84], table))
	return h, nil
}

// Seed each stripe checksum with the checked header CRC. An intact stripe
// copied from another DATA file at the same ordinal must fail verification
// even when damage elsewhere prevents a whole-DATA digest check.
func parityStripeCRC(headerCRC uint32, meta []byte) uint32 {
	return crc32.Update(headerCRC, table, meta[:parityCRCOffset+4])
}

type parityStripe struct {
	meta []byte
	xor  []byte
}

func newParityStripe(dataBytes int64) parityStripe {
	image := make([]byte, parityMetaBytes+min(dataBytes, ParityBlockBytes))
	return parityStripe{meta: image[:parityMetaBytes:parityMetaBytes], xor: image[parityMetaBytes:]}
}

func (s *parityStripe) dataCRC(i int) uint32 { return order.Uint32(s.meta[8+i*4:]) }
func (s *parityStripe) anchor(i int) uint16  { return order.Uint16(s.meta[parityAnchorStart+i*2:]) }

func (s *parityStripe) reset(ordinal uint64, data []byte) {
	clear(s.meta)
	clear(s.xor)
	order.PutUint64(s.meta, ordinal)
	for i := range ParityDataBlocks {
		order.PutUint16(s.meta[parityAnchorStart+2*i:], ParityNoAnchor)
	}
	for i := 0; i*ParityBlockBytes < len(data); i++ {
		block := data[i*ParityBlockBytes : min((i+1)*ParityBlockBytes, len(data))]
		order.PutUint32(s.meta[8+4*i:], crc32.Checksum(block, table))
		subtle.XORBytes(s.xor[:len(block)], s.xor[:len(block)], block)
	}
}

// anchorScan consumes each record payload in the same stripe buffer used to
// calculate parity. A header split across stripes takes one small lookahead.
type anchorScan struct {
	headerAt  int64
	end       int64
	sum       uint32
	want      uint32
	header    [RecordHeaderBytes]byte
	checksum  [RecordHeaderBytes]byte
	lookahead int // bytes from the crossing header that must match the next stripe
}

func (a *anchorScan) consume(ctx context.Context, data io.ReaderAt, e CatalogEntry, off int64, raw []byte, s *parityStripe) error {
	if a.lookahead != 0 {
		if !bytes.Equal(raw[:a.lookahead], a.header[RecordHeaderBytes-a.lookahead:]) {
			return bad("cross-stripe record header changed")
		}
		a.lookahead = 0
	}
	limit := off + int64(len(raw))
	for a.headerAt < limit {
		if a.end == 0 {
			if e.Bytes-a.headerAt < RecordHeaderBytes {
				return bad("truncated parity source header")
			}
			i := int(a.headerAt - off)
			h := raw[i:]
			if len(h) < RecordHeaderBytes {
				if err := readParitySource(ctx, data, a.header[:], a.headerAt); err != nil {
					return err
				}
				if !bytes.Equal(h, a.header[:len(h)]) {
					return bad("cross-stripe record header changed")
				}
				a.lookahead = RecordHeaderBytes - len(h)
				h = a.header[:]
			}
			r, err := decodeRecordHeader(h[:RecordHeaderBytes], e.Bytes-a.headerAt, &a.checksum)
			if err != nil {
				return err
			}
			block := i / ParityBlockBytes
			if s.anchor(block) == ParityNoAnchor {
				order.PutUint16(s.meta[parityAnchorStart+2*block:], uint16(i%ParityBlockBytes))
			}
			a.end, a.sum, a.want = a.headerAt+r.EncodedRecordBytes(), 0, r.PayloadCRC
		}
		first := max(a.headerAt+RecordHeaderBytes, off)
		last := min(a.end, limit)
		if first < last {
			a.sum = crc32.Update(a.sum, table, raw[first-off:last-off])
		}
		if a.end > limit {
			break
		}
		if a.sum != a.want {
			return bad("parity source record payload checksum")
		}
		a.headerAt, a.end = a.end, 0
	}
	return nil
}

func readParitySource(ctx context.Context, data io.ReaderAt, block []byte, off int64) error {
	err := readChunk(ctx, data, block, off)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.Join(bad("short parity source"), err)
	}
	return err
}

// WriteParity builds an immutable sidecar from a catalog-selected DATA file.
// Stage its output privately until this function has verified the entire input.
func WriteParity(ctx context.Context, data io.ReaderAt, out io.Writer, uuid [16]byte, entry CatalogEntry) error {
	h, err := parityHeader(uuid, entry)
	if err != nil {
		return err
	}
	if err := writeChunk(ctx, out, h[:]); err != nil {
		return err
	}
	stripe := newParityStripe(entry.Bytes)
	raw := make([]byte, min(entry.Bytes, parityStripeBytes))
	scan := anchorScan{headerAt: SegmentHeaderBytes}
	digest := sha256.New()
	for off := int64(0); off < entry.Bytes; {
		chunk := raw[:min(int64(len(raw)), entry.Bytes-off)]
		if err := readParitySource(ctx, data, chunk, off); err != nil {
			return err
		}
		if off == 0 {
			segment, err := DecodeSegmentHeader(chunk[:SegmentHeaderBytes])
			if err != nil {
				return err
			}
			if segment != (SegmentHeader{UUID: uuid, ID: entry.ID, Kind: entry.Kind}) {
				return bad("parity source segment identity")
			}
		}
		digest.Write(chunk)
		stripe.reset(uint64(off/parityStripeBytes), chunk)
		if err := scan.consume(ctx, data, entry, off, chunk, &stripe); err != nil {
			return err
		}
		p := stripe.xor[:min(ParityBlockBytes, len(chunk))]
		order.PutUint32(stripe.meta[parityCRCOffset:], crc32.Checksum(p, table))
		order.PutUint32(stripe.meta[parityCRCOffset+4:], parityStripeCRC(order.Uint32(h[84:]), stripe.meta))
		if err := writeChunk(ctx, out, stripe.meta); err != nil {
			return err
		}
		if err := writeChunk(ctx, out, p); err != nil {
			return err
		}
		off += int64(len(chunk))
	}
	if [32]byte(digest.Sum(nil)) != entry.DataFileDigest {
		return bad("parity source sealed DATA digest")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var extra [1]byte
	n, err := data.ReadAt(extra[:], entry.Bytes)
	if n != 0 {
		return bad("oversize parity source")
	}
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return io.ErrNoProgress
	}
	return err
}

func readParityBytes(r io.Reader, dst []byte) error {
	_, err := io.ReadFull(r, dst)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: truncated file", ErrInvalidParity)
	}
	return err
}

func walkParity(ctx context.Context, r io.Reader, uuid [16]byte, e CatalogEntry, visit func(parityStripe, int64, int) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	want, err := parityHeader(uuid, e)
	if err != nil {
		return err
	}
	var actual [parityHeaderBytes]byte
	if err := readParityBytes(r, actual[:]); err != nil {
		return err
	}
	if actual != want {
		return fmt.Errorf("%w: header or sealed identity", ErrInvalidParity)
	}
	s := newParityStripe(e.Bytes)
	for off := int64(0); off < e.Bytes; {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := readParityBytes(r, s.meta); err != nil {
			return err
		}
		size := int(min(int64(parityStripeBytes), e.Bytes-off))
		if order.Uint64(s.meta) != uint64(off/parityStripeBytes) ||
			order.Uint32(s.meta[parityCRCOffset+4:]) != parityStripeCRC(order.Uint32(want[84:]), s.meta) {
			return fmt.Errorf("%w: stripe metadata at DATA offset %d", ErrInvalidParity, off)
		}
		for i := range ParityDataBlocks {
			pos := i * ParityBlockBytes
			anchor := s.anchor(i)
			if pos >= size {
				if anchor != ParityNoAnchor || s.dataCRC(i) != 0 {
					return fmt.Errorf("%w: unused stripe slot", ErrInvalidParity)
				}
			} else if anchor != ParityNoAnchor && (int(anchor) >= min(ParityBlockBytes, size-pos) || int64(anchor) > e.Bytes-off-int64(pos)-RecordHeaderBytes) {
				return fmt.Errorf("%w: record anchor outside DATA", ErrInvalidParity)
			}
		}
		if off == 0 && s.anchor(0) != SegmentHeaderBytes {
			return fmt.Errorf("%w: initial record anchor", ErrInvalidParity)
		}
		p := s.xor[:min(size, ParityBlockBytes)]
		if err := readParityBytes(r, p); err != nil {
			return err
		}
		if crc32.Checksum(p, table) != order.Uint32(s.meta[parityCRCOffset:]) {
			return fmt.Errorf("%w: parity block checksum at DATA offset %d", ErrInvalidParity, off)
		}
		if visit != nil {
			if err := visit(s, off, size); err != nil {
				return err
			}
		}
		off += int64(size)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var extra [1]byte
	n, err := r.Read(extra[:])
	if n != 0 {
		return fmt.Errorf("%w: trailing bytes", ErrInvalidParity)
	}
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return io.ErrNoProgress
	}
	return err
}

// ValidateParity verifies a sidecar's binding, checksums, anchors and exact
// length. Verification of the authoritative source remains a separate operation.
func ValidateParity(ctx context.Context, parity io.Reader, uuid [16]byte, entry CatalogEntry) error {
	return walkParity(ctx, parity, uuid, entry, nil)
}

func unreadableParityBlock(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.EIO)
}

// recoverStripe returns a bit mask of chunks that could not be recovered.
func recoverStripe(ctx context.Context, data io.ReaderAt, off int64, raw, header []byte, s parityStripe) (uint16, error) {
	// Use one sequential read on healthy stripes. Isolate a short read or
	// media error to an individual block before charging it as an erasure.
	stripeErr := readChunk(ctx, data, raw, off)
	if stripeErr != nil && !unreadableParityBlock(stripeErr) {
		return 0, stripeErr
	}
	var gaps uint16
	for pos := 0; pos < len(raw); pos += ParityBlockBytes {
		block := raw[pos:min(pos+ParityBlockBytes, len(raw))]
		var err error
		if stripeErr != nil {
			err = readChunk(ctx, data, block, off+int64(pos))
		}
		if err != nil && !unreadableParityBlock(err) {
			return 0, err
		}
		// The catalog specifies the exact segment header. Its own trailing
		// CRC lets even a valid foreign header have the same enclosing block
		// CRC. Normalize those bytes before checking the complete first block.
		if err == nil && off == 0 && pos == 0 {
			copy(block, header)
		}
		if err != nil || crc32.Checksum(block, table) != s.dataCRC(pos/ParityBlockBytes) {
			gaps |= 1 << (pos / ParityBlockBytes)
		}
	}
	if bits.OnesCount16(gaps) != 1 {
		return gaps, nil
	}
	// XOR all good blocks with the checked parity to recover the missing
	// bytes. XORing a shorter block implicitly pads it with zero.
	missing := bits.TrailingZeros16(gaps) * ParityBlockBytes
	for pos := 0; pos < len(raw); pos += ParityBlockBytes {
		if pos == missing {
			continue
		}
		block := raw[pos:min(pos+ParityBlockBytes, len(raw))]
		subtle.XORBytes(s.xor[:len(block)], s.xor[:len(block)], block)
	}
	dst := raw[missing:min(missing+ParityBlockBytes, len(raw))]
	copy(dst, s.xor[:len(dst)])
	if crc32.Checksum(dst, table) != s.dataCRC(missing/ParityBlockBytes) {
		return 0, fmt.Errorf("%w: XOR disagrees with block checksum at DATA offset %d", ErrInvalidParity, off+int64(missing))
	}
	return 0, nil
}

func walkParityData(ctx context.Context, data io.ReaderAt, parity io.Reader, uuid [16]byte, entry CatalogEntry, visit func(parityStripe, int64, []byte, uint16) error) error {
	var raw []byte
	header := EncodeSegmentHeader(SegmentHeader{UUID: uuid, ID: entry.ID, Kind: entry.Kind})
	digest := sha256.New()
	complete := true
	err := walkParity(ctx, parity, uuid, entry, func(s parityStripe, off int64, size int) error {
		if off == 0 {
			raw = make([]byte, size)
		}
		chunk := raw[:size]
		gaps, err := recoverStripe(ctx, data, off, chunk, header, s)
		if err != nil {
			return err
		}
		if gaps != 0 {
			complete = false
		}
		if complete {
			digest.Write(chunk)
		}
		return visit(s, off, chunk, gaps)
	})
	if err != nil {
		return err
	}
	if complete && [32]byte(digest.Sum(nil)) != entry.DataFileDigest {
		return bad("reconstructed sealed DATA digest")
	}
	return ctx.Err()
}

// WalkParityBlocks streams checked original or reconstructed blocks in physical
// order. A nil block denotes a gap; first is a byte offset within that block
// or ParityNoAnchor. The callback borrows each block until it returns. When
// there are no gaps, success additionally verifies the complete DATA digest.
// Discard staged output if a later check or callback fails.
func WalkParityBlocks(ctx context.Context, data io.ReaderAt, parity io.Reader, uuid [16]byte, entry CatalogEntry, visit func(blockOffset int64, first uint16, data []byte) error) error {
	return walkParityData(ctx, data, parity, uuid, entry, func(s parityStripe, off int64, raw []byte, gaps uint16) error {
		for pos := 0; pos < len(raw); pos += ParityBlockBytes {
			if err := ctx.Err(); err != nil {
				return err
			}
			i := pos / ParityBlockBytes
			var block []byte
			if gaps&(1<<i) == 0 {
				end := min(pos+ParityBlockBytes, len(raw))
				block = raw[pos:end:end]
			}
			if err := visit(off+int64(pos), s.anchor(i), block); err != nil {
				return err
			}
		}
		return nil
	})
}

// RecoverParity reconstructs at most one unavailable block per stripe, verifies
// the exact candidate against its authoritative SHA-256, and never modifies
// source DATA. Discard the private output on any error.
func RecoverParity(ctx context.Context, data io.ReaderAt, parity io.Reader, out io.Writer, uuid [16]byte, entry CatalogEntry) error {
	return walkParityData(ctx, data, parity, uuid, entry, func(_ parityStripe, off int64, raw []byte, gaps uint16) error {
		if gaps != 0 {
			return fmt.Errorf("%w at DATA offset %d (mask %#x)", ErrParityGap, off, gaps)
		}
		return writeChunk(ctx, out, raw)
	})
}
