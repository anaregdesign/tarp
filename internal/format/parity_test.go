package format

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"slices"
	"syscall"
	"testing"
	"testing/iotest"
)

var parityTestUUID = [16]byte{1, 2, 3}

// Each value length gives the fixture precise physical record boundaries.
func parityFixture(lengths ...int) ([]byte, File, []int) {
	raw := EncodeSegment(Segment{UUID: parityTestUUID, ID: 7, Kind: Log})
	var headers []int
	for i, size := range lengths {
		key := []byte{0, 255}
		value := make([]byte, size)
		for j := range value {
			value[j] = byte(j*11 + i)
		}
		headers = append(headers, len(raw))
		raw = append(raw, EncodeRecord(Put, uint64(i+1), key, value)...)
	}
	return raw, File{ID: 7, Kind: Log, First: 1, Last: uint64(len(lengths)), Bytes: int64(len(raw)), Digest: sha256.Sum256(raw)}, headers
}

func parityFor(t testing.TB, raw []byte, e File) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := WriteParity(t.Context(), bytes.NewReader(raw), &out, parityTestUUID, e); err != nil {
		t.Fatal(err)
	}
	b := out.Bytes()
	if err := ValidateParity(t.Context(), bytes.NewReader(b), parityTestUUID, e); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestParityGoldenSingleDataBlock(t *testing.T) {
	raw := EncodeSegment(Segment{UUID: parityTestUUID, ID: 7, Kind: Log})
	raw = append(raw, EncodeRecord(Put, 1, []byte{0, 255}, []byte{1, 2, 3})...)
	e := File{ID: 7, Kind: Log, Bytes: int64(len(raw)), Digest: sha256.Sum256(raw)}
	sidecar := parityFor(t, raw, e)
	const header = "54415250504152310102030000000000000000000000000007000000000000007500000000000000fe0d896c3bb484d389ef04942770c118c95d5e5cff9619b8f459101c5dee4b2f010000000010000010000000d3638a41"
	const stripe = "0000000000000000f5a4c0b60000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000004000fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff5a4c0b6524346c5"
	if hex.EncodeToString(sidecar[:200]) != header+stripe || !bytes.Equal(sidecar[200:], raw) {
		t.Fatal("golden parity image mismatch")
	}
	// An unavailable one-block segment is recoverable, including its header.
	var out bytes.Buffer
	if err := RecoverParity(t.Context(), bytes.NewReader(nil), bytes.NewReader(sidecar), &out, parityTestUUID, e); err != nil || !bytes.Equal(out.Bytes(), raw) {
		t.Fatalf("recovered tiny DATA: %v", err)
	}
}

func TestParityFixesIndependentErasuresAndPartialTail(t *testing.T) {
	raw, e, _ := parityFixture(2*parityStripeBytes + ParityBlockBytes + 123)
	sidecar := parityFor(t, raw, e)
	for _, tc := range []string{"intact", "header and one per stripe", "partial block truncated", "complete block unreadable", "extra bytes"} {
		t.Run(tc, func(t *testing.T) {
			input := slices.Clone(raw)
			var src io.ReaderAt = bytes.NewReader(input)
			switch tc {
			case "header and one per stripe":
				for _, i := range []int{0, parityStripeBytes + ParityBlockBytes, len(raw) - 1} {
					input[i] ^= 0x53
				}
			case "partial block truncated":
				src = bytes.NewReader(input[:len(input)-90])
			case "complete block unreadable":
				src = readerAtFunc(func(b []byte, off int64) (int, error) {
					if off <= 3*ParityBlockBytes && off+int64(len(b)) > 3*ParityBlockBytes {
						return 0, syscall.EIO
					}
					return bytes.NewReader(input).ReadAt(b, off)
				})
			case "extra bytes":
				src = bytes.NewReader(append(input, 1, 2, 3))
			}
			var out bytes.Buffer
			err := RecoverParity(t.Context(), src, bytes.NewReader(sidecar), &out, parityTestUUID, e)
			if err != nil || !bytes.Equal(out.Bytes(), raw) {
				t.Fatalf("recover: %v", err)
			}
		})
	}
}

func TestParityAnchorsCrossStripeHeadersAndReportOnlyLostBlocks(t *testing.T) {
	// Start a header 7 bytes before a stripe boundary, and one at the next
	// block boundary. Include long payloads and multiple headers per block.
	raw, e, headers := parityFixture(parityStripeBytes-7-SegmentHeaderBytes-RecordHeaderBytes-2, ParityBlockBytes+7-RecordHeaderBytes-2, 0, 91, parityStripeBytes, 100)
	sidecar := parityFor(t, raw, e)
	input := slices.Clone(raw)
	input[0] ^= 1
	input[2*ParityBlockBytes+4] ^= 1
	input[parityStripeBytes+2] ^= 1 // independently recoverable in next stripe
	count := 0
	var lost []int
	err := WalkParityBlocks(t.Context(), bytes.NewReader(input), bytes.NewReader(sidecar), parityTestUUID, e, func(off int64, first uint16, b []byte) error {
		i := count
		count++
		if off != int64(i)*ParityBlockBytes {
			t.Fatal("block walk skipped a position")
		}
		want := ParityNoAnchor
		for _, pos := range headers {
			if int64(pos) >= off && int64(pos) < off+ParityBlockBytes {
				want = uint16(int64(pos) - off)
				break
			}
		}
		if first != want {
			t.Fatalf("DATA block %d: first header %d, want %d", i, first, want)
		}
		if b == nil {
			lost = append(lost, i)
			return nil
		}
		if !bytes.Equal(b, raw[off:min(e.Bytes, off+ParityBlockBytes)]) {
			t.Fatal("incorrect survivor or reconstructed chunk")
		}
		return nil
	})
	if err != nil || !slices.Equal(lost, []int{0, 2}) || count != (len(raw)+ParityBlockBytes-1)/ParityBlockBytes {
		t.Fatalf("walk: lost %v, blocks %d, error %v", lost, count, err)
	}
	err = RecoverParity(t.Context(), bytes.NewReader(input), bytes.NewReader(sidecar), io.Discard, parityTestUUID, e)
	if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrInvalidParity) {
		t.Fatalf("unrecoverable stripe: %v", err)
	}
}

func TestParityRejectsDamagedSidecarAndForeignIdentity(t *testing.T) {
	raw, e, _ := parityFixture(parityStripeBytes + 30)
	good := parityFor(t, raw, e)
	for _, tc := range []string{"uuid", "kind", "ID", "digest", "length", "signature", "header CRC", "stripe ordinal", "anchor bounds", "unused CRC", "unused anchor", "initial anchor", "metadata CRC", "parity bytes", "truncated header", "truncated metadata", "truncated tail", "trailing byte"} {
		t.Run(tc, func(t *testing.T) {
			b := slices.Clone(good)
			entry, uuid := e, parityTestUUID
			first := b[parityHeaderBytes : parityHeaderBytes+parityMetaBytes]
			lastAt := parityHeaderBytes + parityMetaBytes + ParityBlockBytes
			last := b[lastAt : lastAt+parityMetaBytes]
			seal := func(p []byte) { order.PutUint32(p[parityCRCOffset+4:], crc32.Checksum(p[:parityCRCOffset+4], table)) }
			switch tc {
			case "uuid":
				uuid[7]++
			case "kind":
				entry.Kind = Cleaned
			case "ID":
				entry.ID++
			case "digest":
				entry.Digest[0]++
			case "length":
				entry.Bytes++
			case "signature":
				b[0]++
			case "header CRC":
				b[84]++
			case "stripe ordinal":
				order.PutUint64(last, 0)
				seal(last)
			case "anchor bounds":
				order.PutUint16(first[parityAnchorStart+2:], ParityBlockBytes)
				seal(first)
			case "unused CRC":
				order.PutUint32(last[12:], 3)
				seal(last)
			case "unused anchor":
				order.PutUint16(last[parityAnchorStart+2:], 0)
				seal(last)
			case "initial anchor":
				order.PutUint16(first[parityAnchorStart:], 50)
				seal(first)
			case "metadata CRC":
				first[parityCRCOffset+4]++
			case "parity bytes":
				b[lastAt-1] ^= 8
			case "truncated header":
				b = b[:parityHeaderBytes-1]
			case "truncated metadata":
				b = b[:parityHeaderBytes+2]
			case "truncated tail":
				b = b[:len(b)-1]
			case "trailing byte":
				b = append(b, 1)
			}
			err := ValidateParity(t.Context(), bytes.NewReader(b), uuid, entry)
			if !errors.Is(err, ErrInvalidParity) || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted %s: %v", tc, err)
			}
		})
	}
}

func TestParitySourceChecksRejectCorruptionAndInvalidHistory(t *testing.T) {
	raw, e, _ := parityFixture(100)
	for _, tc := range []string{"catalog digest", "extra", "short", "header identity", "record metadata", "record payload", "trailing partial header", "invalid entry"} {
		t.Run(tc, func(t *testing.T) {
			b := slices.Clone(raw)
			entry := e
			switch tc {
			case "catalog digest":
				entry.Digest[0]++
			case "extra":
				b = append(b, 2)
			case "short":
				b = b[:len(b)-1]
			case "header identity":
				copy(b, EncodeSegment(Segment{UUID: parityTestUUID, ID: 11, Kind: Log}))
				entry.Digest = sha256.Sum256(b)
			case "record metadata":
				b[SegmentHeaderBytes+1]++
				entry.Digest = sha256.Sum256(b)
			case "record payload":
				b[len(b)-1]++
				entry.Digest = sha256.Sum256(b)
			case "trailing partial header":
				b = append(b, 1)
				entry.Bytes++
				entry.Digest = sha256.Sum256(b)
			case "invalid entry":
				entry.ID = 0
			}
			err := WriteParity(t.Context(), bytes.NewReader(b), io.Discard, parityTestUUID, entry)
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrInvalidParity) {
				t.Fatalf("source error: %v", err)
			}
		})
	}
}

func TestParityWholeFileDigestRejectsCoherentlyAlteredChunks(t *testing.T) {
	raw, e, _ := parityFixture(200)
	sidecar := parityFor(t, raw, e)
	// The assumed CRC table and XOR can jointly encode the wrong data.
	// Only the catalog SHA-256 can validate the completed candidate.
	changed := slices.Clone(raw)
	changed[0] ^= 2
	meta := sidecar[parityHeaderBytes : parityHeaderBytes+parityMetaBytes]
	copy(sidecar[parityHeaderBytes+parityMetaBytes:], changed)
	sum := crc32.Checksum(changed, table)
	order.PutUint32(meta[8:], sum)
	order.PutUint32(meta[parityCRCOffset:], sum)
	order.PutUint32(meta[parityCRCOffset+4:], crc32.Checksum(meta[:parityCRCOffset+4], table))
	for _, b := range [][]byte{nil, changed} {
		err := WalkParityBlocks(t.Context(), bytes.NewReader(b), bytes.NewReader(sidecar), parityTestUUID, e, func(int64, uint16, []byte) error { return nil })
		if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrInvalidParity) {
			t.Fatalf("accepted wrong original DATA digest: %v", err)
		}
	}
}

type parityFailWriter struct{ err error }

func (w parityFailWriter) Write([]byte) (int, error) { return 0, w.err }

func TestParityPreservesContextAndIOErrors(t *testing.T) {
	raw, e, _ := parityFixture(10000)
	sidecar := parityFor(t, raw, e)
	source := readerAtFunc(func([]byte, int64) (int, error) { return 0, syscall.EACCES })
	blocked := errors.New("write failed")
	for _, err := range []error{
		WriteParity(t.Context(), source, io.Discard, parityTestUUID, e),
		RecoverParity(t.Context(), source, bytes.NewReader(sidecar), io.Discard, parityTestUUID, e),
		ValidateParity(t.Context(), iotest.ErrReader(syscall.EACCES), parityTestUUID, e),
	} {
		if !errors.Is(err, syscall.EACCES) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("reader cause lost: %v", err)
		}
	}
	for _, err := range []error{
		WriteParity(t.Context(), bytes.NewReader(raw), parityFailWriter{blocked}, parityTestUUID, e),
		RecoverParity(t.Context(), bytes.NewReader(raw), bytes.NewReader(sidecar), parityFailWriter{blocked}, parityTestUUID, e),
		WalkParityBlocks(t.Context(), bytes.NewReader(raw), bytes.NewReader(sidecar), parityTestUUID, e, func(int64, uint16, []byte) error { return blocked }),
	} {
		if !errors.Is(err, blocked) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("writer/visitor cause lost: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	err := WalkParityBlocks(ctx, bytes.NewReader(raw), bytes.NewReader(sidecar), parityTestUUID, e, func(int64, uint16, []byte) error { calls++; cancel(); return nil })
	cancel()
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("walk after cancel: %d, %v", calls, err)
	}
}

func TestParityBuildReadsStripesOnceExceptCrossingHeader(t *testing.T) {
	raw, e, _ := parityFixture(parityStripeBytes-9-SegmentHeaderBytes-RecordHeaderBytes-2, parityStripeBytes+10)
	reads, lookahead := 0, 0
	src := readerAtFunc(func(b []byte, off int64) (int, error) {
		if off < int64(len(raw)) {
			reads++
			if len(b) == RecordHeaderBytes && off == parityStripeBytes-9 {
				lookahead++
			}
		}
		return bytes.NewReader(raw).ReadAt(b, off)
	})
	if err := WriteParity(t.Context(), src, io.Discard, parityTestUUID, e); err != nil {
		t.Fatal(err)
	}
	stripes := (len(raw) + parityStripeBytes - 1) / parityStripeBytes
	if reads != stripes+1 || lookahead != 1 {
		t.Fatalf("reads %d, expected %d stripes + one header", reads, stripes)
	}
}

func TestParityBuildReusesRecordAndStripeWorkspace(t *testing.T) {
	count := func(lengths []int) float64 {
		raw, entry, _ := parityFixture(lengths...)
		in := bytes.NewReader(raw)
		ctx := t.Context()
		return testing.AllocsPerRun(3, func() {
			if err := WriteParity(ctx, in, io.Discard, parityTestUUID, entry); err != nil {
				t.Fatal(err)
			}
		})
	}
	small := count([]int{3})
	sizes := make([]int, 2000)
	for i := range sizes {
		sizes[i] = 91
	}
	large := count(sizes)
	if large > small+2 {
		t.Fatalf("allocations grew from %.0f to %.0f across 2,000 records", small, large)
	}
}

func TestParityCrossStripeLookaheadMustMatchHashedSource(t *testing.T) {
	raw, e, _ := parityFixture(parityStripeBytes-9-SegmentHeaderBytes-RecordHeaderBytes-2, 100)
	src := readerAtFunc(func(b []byte, off int64) (int, error) {
		n, err := bytes.NewReader(raw).ReadAt(b, off)
		if off == parityStripeBytes-9 && len(b) == RecordHeaderBytes {
			// A coherent stale header with a different sequence would pass its
			// own checksum. The stripe reads still yield the original DATA.
			order.PutUint64(b[16:], 3)
			clear(b[12:16])
			order.PutUint32(b[12:], crc32.Checksum(b, table))
		}
		return n, err
	})
	if err := WriteParity(t.Context(), src, io.Discard, parityTestUUID, e); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("accepted inconsistent lookahead: %v", err)
	}
}
