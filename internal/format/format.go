// Package format contains the storage and backup wire codecs.
package format

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sync"
)

const (
	SegmentHeaderBytes = 64
	RecordHeaderBytes  = 48
	FrontierBytes      = 128
	Log                = 1
	Snapshot           = 2
	Cleaned            = 3
	Put                = 1
	Delete             = 2
)

var (
	ErrCorrupt     = errors.New("format: corrupt data")
	ErrUnsupported = errors.New("format: unsupported storage signature")
	table          = crc32.MakeTable(crc32.Castagnoli)
	order          = binary.LittleEndian
)

func bad(message string) error { return fmt.Errorf("%w: %s", ErrCorrupt, message) }

func zero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

type Segment struct {
	UUID [16]byte
	ID   uint64
	Kind uint32
}

func EncodeSegment(s Segment) []byte {
	b := make([]byte, SegmentHeaderBytes)
	copy(b, "TARPSEGM")
	order.PutUint16(b[10:], SegmentHeaderBytes)
	order.PutUint32(b[12:], s.Kind)
	copy(b[16:], s.UUID[:])
	order.PutUint64(b[32:], s.ID)
	order.PutUint32(b[60:], crc32.Checksum(b[:60], table))
	return b
}

func DecodeSegment(b []byte) (Segment, error) {
	var s Segment
	if len(b) != SegmentHeaderBytes {
		return s, bad("segment header length")
	}
	if string(b[:8]) != "TARPSEGM" || !zero(b[8:10]) {
		return s, bad("segment signature or reserved bytes")
	}
	if order.Uint16(b[10:]) != SegmentHeaderBytes || !zero(b[40:60]) ||
		crc32.Checksum(b[:60], table) != order.Uint32(b[60:]) {
		return s, bad("segment header")
	}
	s.Kind = order.Uint32(b[12:])
	if s.Kind != Log && s.Kind != Snapshot && s.Kind != Cleaned {
		return s, bad("segment kind")
	}
	copy(s.UUID[:], b[16:32])
	s.ID = order.Uint64(b[32:])
	if s.ID == 0 {
		return s, bad("zero segment identity")
	}
	return s, nil
}

type Record struct {
	Kind                 uint8
	Sequence             uint64
	KeyBytes, ValueBytes int64
	PayloadCRC           uint32
}

func (r Record) Bytes() int64 { return RecordHeaderBytes + r.KeyBytes + r.ValueBytes }

func EncodeRecord(kind uint8, sequence uint64, key, value []byte) []byte {
	b := make([]byte, RecordHeaderBytes+len(key)+len(value))
	copy(b, "TARPRECD")
	b[10] = kind
	order.PutUint64(b[16:], sequence)
	order.PutUint64(b[24:], uint64(len(key)))
	order.PutUint64(b[32:], uint64(len(value)))
	copy(b[RecordHeaderBytes:], key)
	copy(b[RecordHeaderBytes+len(key):], value)
	order.PutUint32(b[40:], crc32.Checksum(b[RecordHeaderBytes:], table))
	order.PutUint32(b[12:], crc32.Checksum(b[:RecordHeaderBytes], table))
	return b
}

func DecodeRecord(b []byte, available int64) (Record, error) {
	var r Record
	if len(b) != RecordHeaderBytes || available < RecordHeaderBytes {
		return r, bad("incomplete committed record header")
	}
	if string(b[:8]) != "TARPRECD" || !zero(b[8:10]) {
		return r, bad("record signature or reserved bytes")
	}
	h := [RecordHeaderBytes]byte{}
	copy(h[:], b)
	clear(h[12:16])
	if crc32.Checksum(h[:], table) != order.Uint32(b[12:]) {
		return r, bad("record header checksum")
	}
	if b[11] != 0 {
		return r, bad("record flags")
	}
	if !zero(b[44:48]) {
		return r, bad("record reserved bytes")
	}
	r.Kind, r.Sequence = b[10], order.Uint64(b[16:])
	k, v := order.Uint64(b[24:]), order.Uint64(b[32:])
	if k == 0 || k > math.MaxInt64-RecordHeaderBytes ||
		v > math.MaxInt64-RecordHeaderBytes-k || k+v > uint64(available-RecordHeaderBytes) {
		return r, bad("record lengths")
	}
	r.KeyBytes, r.ValueBytes, r.PayloadCRC = int64(k), int64(v), order.Uint32(b[40:])
	if r.Sequence == 0 || r.Kind != Put && r.Kind != Delete || r.Kind == Delete && v != 0 {
		return r, bad("record kind, sequence or tombstone")
	}
	return r, nil
}

// Scan verifies complete records without allocating their values.
func Scan(f io.ReaderAt, end int64, visit func(int64, Record, string) error) error {
	if end < SegmentHeaderBytes {
		return bad("segment end")
	}
	buffer := make([]byte, streamChunk)
	for offset := int64(SegmentHeaderBytes); offset < end; {
		record, key, err := ReadRecordKey(context.Background(), f, offset, end-offset, buffer)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w: truncated record: %w", ErrCorrupt, err)
		}
		if err != nil {
			return err
		}
		if err := visit(offset, record, key); err != nil {
			return err
		}
		offset += record.Bytes()
	}
	return nil
}

// Read small indexed records in one I/O. Only a value-sized copy enters the
// cache. Larger reads validate the header before allocating the payload.
const indexedReadBufferBytes = 8 << 10

var indexedReadBuffers = sync.Pool{
	New: func() any { return new([indexedReadBufferBytes]byte) },
}

func ReadValue(f io.ReaderAt, offset, size int64, key string, sequence uint64) ([]byte, error) {
	if size <= RecordHeaderBytes || size > math.MaxInt || offset < 0 || offset > math.MaxInt64-size {
		return nil, bad("indexed record bounds")
	}
	var first []byte
	if size <= indexedReadBufferBytes {
		buf := indexedReadBuffers.Get().(*[indexedReadBufferBytes]byte)
		defer indexedReadBuffers.Put(buf)
		first = buf[:size]
	} else {
		first = make([]byte, RecordHeaderBytes)
	}
	if err := readChunk(context.Background(), f, first, offset); err != nil {
		return nil, err
	}
	r, err := DecodeRecord(first[:RecordHeaderBytes], size)
	if err != nil {
		return nil, err
	}
	if r.Bytes() != size || r.Sequence != sequence || r.Kind != Put || r.KeyBytes != int64(len(key)) {
		return nil, bad("indexed record identity")
	}
	if int64(len(first)) == size {
		payload := first[RecordHeaderBytes:]
		if !bytes.Equal(payload[:len(key)], []byte(key)) || crc32.Checksum(payload, table) != r.PayloadCRC {
			return nil, bad("indexed record payload")
		}
		return bytes.Clone(payload[len(key):]), nil
	}
	k := make([]byte, len(key))
	if err := readChunk(context.Background(), f, k, offset+RecordHeaderBytes); err != nil {
		return nil, err
	}
	value := make([]byte, int(r.ValueBytes))
	if len(value) != 0 {
		if err := readChunk(context.Background(), f, value, offset+RecordHeaderBytes+r.KeyBytes); err != nil {
			return nil, err
		}
	}
	if !bytes.Equal(k, []byte(key)) || crc32.Update(crc32.Checksum(k, table), table, value) != r.PayloadCRC {
		return nil, bad("indexed record payload")
	}
	return value, nil
}

type Frontier struct {
	UUID                   [16]byte
	Generation, ManifestID uint64
	ManifestHash           [32]byte
	ActiveID               uint64
	ActiveEnd              int64
	Sequence               uint64
}

func EncodeFrontier(f Frontier) []byte {
	b := make([]byte, FrontierBytes)
	copy(b, "TARPCURR")
	order.PutUint16(b[10:], FrontierBytes)
	copy(b[16:], f.UUID[:])
	order.PutUint64(b[32:], f.Generation)
	order.PutUint64(b[40:], f.ManifestID)
	copy(b[48:], f.ManifestHash[:])
	order.PutUint64(b[80:], f.ActiveID)
	order.PutUint64(b[88:], uint64(f.ActiveEnd))
	order.PutUint64(b[96:], f.Sequence)
	order.PutUint32(b[124:], crc32.Checksum(b[:124], table))
	return b
}

// CheckFrontierSignature identifies the canonical outer envelope without
// inspecting data files or acquiring a potentially creating directory lock.
func CheckFrontierSignature(prefix []byte) error {
	if len(prefix) < 8 {
		return bad("incomplete frontier signature")
	}
	if string(prefix[:8]) != "TARPCURR" {
		return ErrUnsupported
	}
	return nil
}

func DecodeFrontier(b []byte) (Frontier, error) {
	var f Frontier
	if err := CheckFrontierSignature(b); err != nil {
		return f, err
	}
	if len(b) != FrontierBytes {
		return f, bad("frontier length")
	}
	if !zero(b[8:10]) || order.Uint16(b[10:]) != FrontierBytes || !zero(b[12:16]) || !zero(b[104:124]) ||
		crc32.Checksum(b[:124], table) != order.Uint32(b[124:]) {
		return f, bad("frontier checksum or fields")
	}
	copy(f.UUID[:], b[16:32])
	f.Generation, f.ManifestID = order.Uint64(b[32:]), order.Uint64(b[40:])
	copy(f.ManifestHash[:], b[48:80])
	f.ActiveID = order.Uint64(b[80:])
	end := order.Uint64(b[88:])
	if end < SegmentHeaderBytes || end > math.MaxInt64 || f.Generation == 0 || f.ManifestID == 0 || f.ActiveID == 0 || f.ActiveID == f.ManifestID {
		return f, bad("frontier bounds")
	}
	f.ActiveEnd, f.Sequence = int64(end), order.Uint64(b[96:])
	return f, nil
}

type File struct {
	ID          uint64
	Bytes       int64
	Kind        uint32
	First, Last uint64
}

type Catalog struct {
	UUID                  [16]byte
	Cutoff, PrunedThrough uint64
	ActiveID              uint64
	Files                 []File
}

func envelope(magic string, payload []byte) []byte {
	b := make([]byte, 32+len(payload))
	copy(b, magic)
	order.PutUint64(b[12:], uint64(len(payload)))
	order.PutUint32(b[20:], crc32.Checksum(payload, table))
	order.PutUint32(b[24:], crc32.Checksum(b[:32], table))
	copy(b[32:], payload)
	return b
}

func payload(b []byte, magic string) ([]byte, error) {
	if len(b) < 8 {
		return nil, bad("incomplete metadata signature")
	}
	if string(b[:8]) != magic {
		if magic == "TARPBACK" {
			return nil, ErrUnsupported
		}
		return nil, bad("selected metadata signature")
	}
	if len(b) < 32 {
		return nil, bad("metadata header")
	}
	h := [32]byte{}
	copy(h[:], b[:32])
	clear(h[24:28])
	if !zero(b[8:12]) || !zero(b[28:32]) || order.Uint64(b[12:]) != uint64(len(b)-32) ||
		crc32.Checksum(h[:], table) != order.Uint32(b[24:]) ||
		crc32.Checksum(b[32:], table) != order.Uint32(b[20:]) {
		return nil, bad("metadata bounds or checksum")
	}
	return b[32:], nil
}

const (
	catalogHeaderBytes = 48
	catalogEntryBytes  = 40
)

func EncodeCatalog(c Catalog) []byte {
	p := make([]byte, catalogHeaderBytes+catalogEntryBytes*len(c.Files))
	copy(p, c.UUID[:])
	order.PutUint64(p[16:], c.Cutoff)
	order.PutUint64(p[24:], c.PrunedThrough)
	order.PutUint64(p[32:], c.ActiveID)
	order.PutUint64(p[40:], uint64(len(c.Files)))
	for i, f := range c.Files {
		q := p[catalogHeaderBytes+catalogEntryBytes*i:]
		order.PutUint64(q, f.ID)
		order.PutUint64(q[8:], uint64(f.Bytes))
		order.PutUint64(q[16:], uint64(f.Kind))
		order.PutUint64(q[24:], f.First)
		order.PutUint64(q[32:], f.Last)
	}
	return envelope("TARPCATL", p)
}

func DecodeCatalog(b []byte) (Catalog, error) {
	var c Catalog
	p, err := payload(b, "TARPCATL")
	if err != nil {
		return c, err
	}
	if len(p) < catalogHeaderBytes {
		return c, bad("catalog length")
	}
	entries := p[catalogHeaderBytes:]
	if len(entries)%catalogEntryBytes != 0 || order.Uint64(p[40:]) != uint64(len(entries)/catalogEntryBytes) {
		return c, bad("catalog count")
	}
	copy(c.UUID[:], p[:16])
	c.Cutoff, c.PrunedThrough, c.ActiveID = order.Uint64(p[16:]), order.Uint64(p[24:]), order.Uint64(p[32:])
	if c.ActiveID == 0 || c.Cutoff > c.PrunedThrough {
		return c, bad("catalog identity or pruning boundary")
	}
	seen := map[uint64]bool{c.ActiveID: true}
	previous := c.Cutoff
	for q := entries; len(q) != 0; q = q[catalogEntryBytes:] {
		id, n, kind := order.Uint64(q), order.Uint64(q[8:]), order.Uint64(q[16:])
		first, last := order.Uint64(q[24:]), order.Uint64(q[32:])
		if id == 0 {
			if kind != Cleaned || n != 0 {
				return c, bad("empty cleaned interval")
			}
		} else {
			if seen[id] || n < SegmentHeaderBytes+RecordHeaderBytes+1 || n > math.MaxInt64 {
				return c, bad("catalog duplicate ID or file length")
			}
			seen[id] = true
		}
		switch kind {
		case Snapshot:
			if c.Cutoff == 0 || previous != c.Cutoff || first != 0 || last != 0 {
				return c, bad("snapshot outside checkpoint prefix")
			}
		case Log, Cleaned:
			if previous == math.MaxUint64 || first != previous+1 || last < first {
				return c, bad("catalog log coverage")
			}
			if kind == Cleaned && last > c.PrunedThrough {
				return c, bad("cleaned interval beyond pruning boundary")
			}
			if kind == Log && last-first+1 > (n-SegmentHeaderBytes)/(RecordHeaderBytes+1) {
				return c, bad("raw interval cannot fit file")
			}
			previous = last
		default:
			return c, bad("catalog entry kind")
		}
		c.Files = append(c.Files, File{ID: id, Bytes: int64(n), Kind: uint32(kind), First: first, Last: last})
	}
	return c, nil
}

type BackupFile struct {
	Name  string
	Bytes int64
	Hash  [sha256.Size]byte
}

// Backup source settings are advisory; restore derives live minima by replay.
type Backup struct {
	UUID                                [16]byte
	Sequence, Cutoff                    uint64
	SourceCacheBytes, SourceRecordBytes int64
	Files                               []BackupFile
}

func EncodeBackup(b Backup) []byte {
	p := make([]byte, 64)
	copy(p, b.UUID[:])
	for i, n := range []uint64{b.Sequence, b.Cutoff, uint64(b.SourceCacheBytes), uint64(b.SourceRecordBytes), 0, uint64(len(b.Files))} {
		order.PutUint64(p[16+8*i:], n)
	}
	for _, f := range b.Files {
		q := make([]byte, 2+len(f.Name)+8+sha256.Size)
		order.PutUint16(q, uint16(len(f.Name)))
		copy(q[2:], f.Name)
		order.PutUint64(q[2+len(f.Name):], uint64(f.Bytes))
		copy(q[10+len(f.Name):], f.Hash[:])
		p = append(p, q...)
	}
	return envelope("TARPBACK", p)
}

func DecodeBackup(b []byte) (Backup, error) {
	var result Backup
	p, err := payload(b, "TARPBACK")
	if err != nil {
		return result, err
	}
	if len(p) < 64 || order.Uint64(p[56:]) > uint64((len(p)-64)/43) {
		return result, bad("backup count")
	}
	copy(result.UUID[:], p[:16])
	result.Sequence, result.Cutoff = order.Uint64(p[16:]), order.Uint64(p[24:])
	result.SourceCacheBytes = int64(order.Uint64(p[32:]))
	result.SourceRecordBytes = int64(order.Uint64(p[40:]))
	// Older backups used the slot at offset 48 for an advisory file size limit.
	if result.SourceCacheBytes < 0 || result.SourceRecordBytes < 0 || order.Uint64(p[48:]) > math.MaxInt64 {
		return result, bad("backup source settings")
	}
	count := order.Uint64(p[56:])
	p = p[64:]
	seen := make(map[string]bool)
	for i := uint64(0); i < count; i++ {
		if len(p) < 2 {
			return result, bad("backup file name")
		}
		n := int(order.Uint16(p))
		if n == 0 || n > 96 || len(p) < n+42 {
			return result, bad("backup entry length")
		}
		f := BackupFile{Name: string(p[2 : 2+n])}
		size := order.Uint64(p[2+n:])
		if size > math.MaxInt64 || seen[f.Name] {
			return result, bad("backup size or duplicate file")
		}
		f.Bytes = int64(size)
		copy(f.Hash[:], p[10+n:42+n])
		seen[f.Name] = true
		result.Files = append(result.Files, f)
		p = p[42+n:]
	}
	if len(p) != 0 || result.Cutoff > result.Sequence {
		return result, bad("backup trailing bytes or cutoff")
	}
	return result, nil
}
