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
	DataBlockBytes      = 4096
	DataBlocksPerStripe = 16
	NoRecordAnchor      = uint16(0xffff)

	stripeDataBytes     = DataBlockBytes * DataBlocksPerStripe
	parityHeaderBytes   = 88
	parityAnchorStart   = 8 + 4*DataBlocksPerStripe
	parityCRCOffset     = parityAnchorStart + 2*DataBlocksPerStripe
	stripeMetadataBytes = parityCRCOffset + 8
)

// ErrInvalidParity identifies a damaged or mismatched PARITY sidecar. A corrupt
// DATA source or a stripe exceeding its repair capacity matches ErrCorrupt.
var ErrInvalidParity = fmt.Errorf("%w: invalid PARITY sidecar", ErrCorrupt)

// ErrUnrecoverableDataBlocks identifies DATA blocks that the checked stripe cannot reconstruct.
var ErrUnrecoverableDataBlocks = fmt.Errorf("%w: unrecoverable DATA blocks in parity stripe", ErrCorrupt)

// ParitySidecarBytes returns the encoded sidecar length for a physical DATA file.
// Metadata-only compaction intervals consume no sidecar space.
func ParitySidecarBytes(dataBytes uint64) uint64 {
	if dataBytes == 0 {
		return 0
	}
	full, tail := dataBytes/stripeDataBytes, dataBytes%stripeDataBytes
	size := uint64(parityHeaderBytes) + full*(stripeMetadataBytes+DataBlockBytes)
	if tail != 0 {
		size += stripeMetadataBytes + min(tail, DataBlockBytes)
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
	order.PutUint32(h[76:], DataBlockBytes)
	order.PutUint32(h[80:], DataBlocksPerStripe)
	order.PutUint32(h[84:], crc32.Checksum(h[:84], table))
	return h, nil
}

// Seed each stripe checksum with the checked header CRC. An intact stripe
// copied from another DATA file at the same ordinal must fail verification
// even when damage elsewhere prevents a whole-DATA digest check.
func parityStripeCRC(headerCRC uint32, metadata []byte) uint32 {
	return crc32.Update(headerCRC, table, metadata[:parityCRCOffset+4])
}

type parityStripe struct {
	metadata   []byte
	xorPayload []byte
}

func newParityStripe(dataBytes int64) parityStripe {
	image := make([]byte, stripeMetadataBytes+min(dataBytes, DataBlockBytes))
	return parityStripe{metadata: image[:stripeMetadataBytes:stripeMetadataBytes], xorPayload: image[stripeMetadataBytes:]}
}

func (s *parityStripe) dataCRC(i int) uint32 { return order.Uint32(s.metadata[8+i*4:]) }
func (s *parityStripe) anchor(i int) uint16  { return order.Uint16(s.metadata[parityAnchorStart+i*2:]) }

func (s *parityStripe) reset(ordinal uint64, data []byte) {
	clear(s.metadata)
	clear(s.xorPayload)
	order.PutUint64(s.metadata, ordinal)
	for i := range DataBlocksPerStripe {
		order.PutUint16(s.metadata[parityAnchorStart+2*i:], NoRecordAnchor)
	}
	for i := 0; i*DataBlockBytes < len(data); i++ {
		block := data[i*DataBlockBytes : min((i+1)*DataBlockBytes, len(data))]
		order.PutUint32(s.metadata[8+4*i:], crc32.Checksum(block, table))
		subtle.XORBytes(s.xorPayload[:len(block)], s.xorPayload[:len(block)], block)
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
			block := i / DataBlockBytes
			if s.anchor(block) == NoRecordAnchor {
				order.PutUint16(s.metadata[parityAnchorStart+2*block:], uint16(i%DataBlockBytes))
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

func readParitySource(ctx context.Context, data io.ReaderAt, dst []byte, off int64) error {
	err := readChunk(ctx, data, dst, off)
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.Join(bad("short parity source"), err)
	}
	return err
}

// WriteParitySidecar builds an immutable sidecar from a catalog-selected DATA file.
// Stage its output privately until this function has verified the entire input.
func WriteParitySidecar(ctx context.Context, data io.ReaderAt, out io.Writer, uuid [16]byte, entry CatalogEntry) error {
	h, err := parityHeader(uuid, entry)
	if err != nil {
		return err
	}
	if err := writeChunk(ctx, out, h[:]); err != nil {
		return err
	}
	stripe := newParityStripe(entry.Bytes)
	raw := make([]byte, min(entry.Bytes, stripeDataBytes))
	scan := anchorScan{headerAt: SegmentHeaderBytes}
	digest := sha256.New()
	for off := int64(0); off < entry.Bytes; {
		stripeData := raw[:min(int64(len(raw)), entry.Bytes-off)]
		if err := readParitySource(ctx, data, stripeData, off); err != nil {
			return err
		}
		if off == 0 {
			segment, err := DecodeSegmentHeader(stripeData[:SegmentHeaderBytes])
			if err != nil {
				return err
			}
			if segment != (SegmentHeader{UUID: uuid, ID: entry.ID, Kind: entry.Kind}) {
				return bad("parity source segment identity")
			}
		}
		digest.Write(stripeData)
		stripe.reset(uint64(off/stripeDataBytes), stripeData)
		if err := scan.consume(ctx, data, entry, off, stripeData, &stripe); err != nil {
			return err
		}
		xorPayload := stripe.xorPayload[:min(DataBlockBytes, len(stripeData))]
		order.PutUint32(stripe.metadata[parityCRCOffset:], crc32.Checksum(xorPayload, table))
		order.PutUint32(stripe.metadata[parityCRCOffset+4:], parityStripeCRC(order.Uint32(h[84:]), stripe.metadata))
		if err := writeChunk(ctx, out, stripe.metadata); err != nil {
			return err
		}
		if err := writeChunk(ctx, out, xorPayload); err != nil {
			return err
		}
		off += int64(len(stripeData))
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

func readParitySidecarBytes(r io.Reader, dst []byte) error {
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
	if err := readParitySidecarBytes(r, actual[:]); err != nil {
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
		if err := readParitySidecarBytes(r, s.metadata); err != nil {
			return err
		}
		size := int(min(int64(stripeDataBytes), e.Bytes-off))
		if order.Uint64(s.metadata) != uint64(off/stripeDataBytes) ||
			order.Uint32(s.metadata[parityCRCOffset+4:]) != parityStripeCRC(order.Uint32(want[84:]), s.metadata) {
			return fmt.Errorf("%w: stripe metadata at DATA offset %d", ErrInvalidParity, off)
		}
		for i := range DataBlocksPerStripe {
			pos := i * DataBlockBytes
			anchor := s.anchor(i)
			if pos >= size {
				if anchor != NoRecordAnchor || s.dataCRC(i) != 0 {
					return fmt.Errorf("%w: unused stripe slot", ErrInvalidParity)
				}
			} else if anchor != NoRecordAnchor && (int(anchor) >= min(DataBlockBytes, size-pos) || int64(anchor) > e.Bytes-off-int64(pos)-RecordHeaderBytes) {
				return fmt.Errorf("%w: record anchor outside DATA", ErrInvalidParity)
			}
		}
		if off == 0 && s.anchor(0) != SegmentHeaderBytes {
			return fmt.Errorf("%w: initial record anchor", ErrInvalidParity)
		}
		xorPayload := s.xorPayload[:min(size, DataBlockBytes)]
		if err := readParitySidecarBytes(r, xorPayload); err != nil {
			return err
		}
		if crc32.Checksum(xorPayload, table) != order.Uint32(s.metadata[parityCRCOffset:]) {
			return fmt.Errorf("%w: XOR payload checksum at DATA offset %d", ErrInvalidParity, off)
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

// ValidateParitySidecar verifies a sidecar's binding, checksums, anchors and exact
// length. Verification of the authoritative source remains a separate operation.
func ValidateParitySidecar(ctx context.Context, parity io.Reader, uuid [16]byte, entry CatalogEntry) error {
	return walkParity(ctx, parity, uuid, entry, nil)
}

func unreadableDataBlock(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.EIO)
}

// recoverStripe returns a bit mask of unrecoverable DATA blocks in the stripe.
func recoverStripe(ctx context.Context, data io.ReaderAt, off int64, raw, header []byte, s parityStripe) (uint16, error) {
	// Use one sequential read on healthy stripes. Isolate a short read or
	// media error to an individual block before charging it as an erasure.
	stripeErr := readChunk(ctx, data, raw, off)
	if stripeErr != nil && !unreadableDataBlock(stripeErr) {
		return 0, stripeErr
	}
	var gaps uint16
	for pos := 0; pos < len(raw); pos += DataBlockBytes {
		block := raw[pos:min(pos+DataBlockBytes, len(raw))]
		var err error
		if stripeErr != nil {
			err = readChunk(ctx, data, block, off+int64(pos))
		}
		if err != nil && !unreadableDataBlock(err) {
			return 0, err
		}
		// The catalog specifies the exact segment header. Its own trailing
		// CRC lets even a valid foreign header have the same enclosing block
		// CRC. Normalize those bytes before checking the complete first block.
		if err == nil && off == 0 && pos == 0 {
			copy(block, header)
		}
		if err != nil || crc32.Checksum(block, table) != s.dataCRC(pos/DataBlockBytes) {
			gaps |= 1 << (pos / DataBlockBytes)
		}
	}
	if bits.OnesCount16(gaps) != 1 {
		return gaps, nil
	}
	// XOR all good blocks with the checked XOR payload to recover the missing
	// bytes. XORing a shorter block implicitly pads it with zero.
	missing := bits.TrailingZeros16(gaps) * DataBlockBytes
	for pos := 0; pos < len(raw); pos += DataBlockBytes {
		if pos == missing {
			continue
		}
		block := raw[pos:min(pos+DataBlockBytes, len(raw))]
		subtle.XORBytes(s.xorPayload[:len(block)], s.xorPayload[:len(block)], block)
	}
	dst := raw[missing:min(missing+DataBlockBytes, len(raw))]
	copy(dst, s.xorPayload[:len(dst)])
	if crc32.Checksum(dst, table) != s.dataCRC(missing/DataBlockBytes) {
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
		stripeData := raw[:size]
		gaps, err := recoverStripe(ctx, data, off, stripeData, header, s)
		if err != nil {
			return err
		}
		if gaps != 0 {
			complete = false
		}
		if complete {
			digest.Write(stripeData)
		}
		return visit(s, off, stripeData, gaps)
	})
	if err != nil {
		return err
	}
	if complete && [32]byte(digest.Sum(nil)) != entry.DataFileDigest {
		return bad("reconstructed sealed DATA digest")
	}
	return ctx.Err()
}

// WalkDataBlocksFromParity streams checked original or reconstructed DATA blocks in
// ascending segment offset order. A nil block denotes a gap. The first record anchor
// is a byte offset within the DATA block or NoRecordAnchor. The callback borrows each block until it returns. When
// there are no gaps, success additionally verifies the complete DATA digest.
// Discard staged output if a later check or callback fails.
func WalkDataBlocksFromParity(ctx context.Context, data io.ReaderAt, parity io.Reader, uuid [16]byte, entry CatalogEntry, visit func(blockOffset int64, first uint16, data []byte) error) error {
	return walkParityData(ctx, data, parity, uuid, entry, func(s parityStripe, off int64, raw []byte, gaps uint16) error {
		for pos := 0; pos < len(raw); pos += DataBlockBytes {
			if err := ctx.Err(); err != nil {
				return err
			}
			i := pos / DataBlockBytes
			var block []byte
			if gaps&(1<<i) == 0 {
				end := min(pos+DataBlockBytes, len(raw))
				block = raw[pos:end:end]
			}
			if err := visit(off+int64(pos), s.anchor(i), block); err != nil {
				return err
			}
		}
		return nil
	})
}

// RecoverDataFromParity reconstructs at most one unavailable DATA block per parity stripe.
// It verifies the completed output against the sealed DATA digest in the catalog.
// Callers discard partial output on error.
func RecoverDataFromParity(ctx context.Context, data io.ReaderAt, parity io.Reader, out io.Writer, uuid [16]byte, entry CatalogEntry) error {
	return walkParityData(ctx, data, parity, uuid, entry, func(_ parityStripe, off int64, raw []byte, gaps uint16) error {
		if gaps != 0 {
			return fmt.Errorf("%w at DATA offset %d (mask %#x)", ErrUnrecoverableDataBlocks, off, gaps)
		}
		return writeChunk(ctx, out, raw)
	})
}
