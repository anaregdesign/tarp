// Package format contains the private version-one wire codecs.
package format

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

const (
	Version            = 1
	SegmentHeaderBytes = 64
	RecordHeaderBytes  = 48
	FrontierBytes      = 128
	Log                = 1
	Snapshot           = 2
	Put                = 1
	Delete             = 2
)

var (
	ErrCorrupt     = errors.New("format: corrupt data")
	ErrUnsupported = errors.New("format: unsupported version or flags")
	table          = crc32.MakeTable(crc32.Castagnoli)
	order          = binary.LittleEndian
)

func bad(message string) error { return fmt.Errorf("%w: %s", ErrCorrupt, message) }

func validVersion(b []byte, magic string, versionOffset int) error {
	if string(b[:8]) != magic {
		return bad("magic")
	}
	if order.Uint16(b[versionOffset:]) != Version {
		return ErrUnsupported
	}
	return nil
}

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
	copy(b, "TARPSEG1")
	order.PutUint16(b[8:], Version)
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
	if err := validVersion(b, "TARPSEG1", 8); err != nil {
		return s, err
	}
	if order.Uint16(b[10:]) != SegmentHeaderBytes || !zero(b[40:60]) ||
		crc32.Checksum(b[:60], table) != order.Uint32(b[60:]) {
		return s, bad("segment header")
	}
	s.Kind = order.Uint32(b[12:])
	if s.Kind != Log && s.Kind != Snapshot {
		return s, ErrUnsupported
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
	copy(b, "TARPREC1")
	order.PutUint16(b[8:], Version)
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
	if err := validVersion(b, "TARPREC1", 8); err != nil {
		return r, err
	}
	h := [RecordHeaderBytes]byte{}
	copy(h[:], b)
	clear(h[12:16])
	if crc32.Checksum(h[:], table) != order.Uint32(b[12:]) {
		return r, bad("record header checksum")
	}
	if b[11] != 0 {
		return r, ErrUnsupported
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
	var header [RecordHeaderBytes]byte
	buf := make([]byte, 32<<10)
	for offset := int64(SegmentHeaderBytes); offset < end; {
		if _, err := f.ReadAt(header[:], offset); err != nil {
			return fmt.Errorf("%w: record header: %w", ErrCorrupt, err)
		}
		r, err := DecodeRecord(header[:], end-offset)
		if err != nil {
			return err
		}
		h := crc32.New(table)
		n, copyErr := io.CopyBuffer(h, io.NewSectionReader(f, offset+RecordHeaderBytes, r.KeyBytes+r.ValueBytes), buf)
		if copyErr != nil {
			return copyErr
		}
		if n != r.KeyBytes+r.ValueBytes {
			return bad("incomplete committed payload")
		}
		if h.Sum32() != r.PayloadCRC {
			return bad("record payload checksum")
		}
		if uint64(r.KeyBytes) > uint64(int(^uint(0)>>1)) {
			return bad("key does not fit native memory")
		}
		key := make([]byte, int(r.KeyBytes))
		if _, err := f.ReadAt(key, offset+RecordHeaderBytes); err != nil {
			return err
		}
		if err := visit(offset, r, string(key)); err != nil {
			return err
		}
		offset += r.Bytes()
	}
	return nil
}

func ReadValue(f io.ReaderAt, offset, size int64, key string, sequence uint64) ([]byte, error) {
	var h [RecordHeaderBytes]byte
	if _, err := f.ReadAt(h[:], offset); err != nil {
		return nil, err
	}
	r, err := DecodeRecord(h[:], size)
	if err != nil {
		return nil, err
	}
	if r.Bytes() != size || r.Sequence != sequence || r.Kind != Put || r.KeyBytes != int64(len(key)) ||
		uint64(size) > uint64(int(^uint(0)>>1)) {
		return nil, bad("indexed record identity")
	}
	k := make([]byte, len(key))
	if _, err := f.ReadAt(k, offset+RecordHeaderBytes); err != nil {
		return nil, err
	}
	value := make([]byte, int(r.ValueBytes))
	if len(value) != 0 {
		if _, err := f.ReadAt(value, offset+RecordHeaderBytes+r.KeyBytes); err != nil {
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
	copy(b, "TARPCUR1")
	order.PutUint16(b[8:], Version)
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

func DecodeFrontier(b []byte) (Frontier, error) {
	var f Frontier
	if len(b) != FrontierBytes {
		return f, bad("frontier length")
	}
	if err := validVersion(b, "TARPCUR1", 8); err != nil {
		return f, err
	}
	if order.Uint16(b[10:]) != FrontierBytes || !zero(b[12:16]) || !zero(b[104:124]) ||
		crc32.Checksum(b[:124], table) != order.Uint32(b[124:]) {
		return f, bad("frontier checksum or fields")
	}
	copy(f.UUID[:], b[16:32])
	f.Generation, f.ManifestID = order.Uint64(b[32:]), order.Uint64(b[40:])
	copy(f.ManifestHash[:], b[48:80])
	f.ActiveID = order.Uint64(b[80:])
	end := order.Uint64(b[88:])
	if end < SegmentHeaderBytes || end > math.MaxInt64 || f.Generation == 0 || f.ManifestID == 0 || f.ActiveID == 0 {
		return f, bad("frontier bounds")
	}
	f.ActiveEnd, f.Sequence = int64(end), order.Uint64(b[96:])
	return f, nil
}

type File struct {
	ID    uint64
	Bytes int64
	Kind  uint32
}

type Catalog struct {
	UUID     [16]byte
	Cutoff   uint64
	ActiveID uint64
	Files    []File
}

func envelope(magic string, payload []byte) []byte {
	b := make([]byte, 32+len(payload))
	copy(b, magic)
	order.PutUint16(b[8:], Version)
	order.PutUint64(b[12:], uint64(len(payload)))
	order.PutUint32(b[20:], crc32.Checksum(payload, table))
	order.PutUint32(b[24:], crc32.Checksum(b[:32], table))
	copy(b[32:], payload)
	return b
}

func payload(b []byte, magic string) ([]byte, error) {
	if len(b) < 32 {
		return nil, bad("metadata header")
	}
	if err := validVersion(b, magic, 8); err != nil {
		return nil, err
	}
	h := [32]byte{}
	copy(h[:], b[:32])
	clear(h[24:28])
	if !zero(b[10:12]) || !zero(b[28:32]) || order.Uint64(b[12:]) != uint64(len(b)-32) ||
		crc32.Checksum(h[:], table) != order.Uint32(b[24:]) ||
		crc32.Checksum(b[32:], table) != order.Uint32(b[20:]) {
		return nil, bad("metadata bounds or checksum")
	}
	return b[32:], nil
}

func EncodeCatalog(c Catalog) []byte {
	p := make([]byte, 40+24*len(c.Files))
	copy(p, c.UUID[:])
	order.PutUint64(p[16:], c.Cutoff)
	order.PutUint64(p[24:], c.ActiveID)
	order.PutUint64(p[32:], uint64(len(c.Files)))
	for i, f := range c.Files {
		q := p[40+24*i:]
		order.PutUint64(q, f.ID)
		order.PutUint64(q[8:], uint64(f.Bytes))
		order.PutUint64(q[16:], uint64(f.Kind))
	}
	return envelope("TARPCAT1", p)
}

func DecodeCatalog(b []byte) (Catalog, error) {
	var c Catalog
	p, err := payload(b, "TARPCAT1")
	if err != nil {
		return c, err
	}
	if len(p) < 40 || (len(p)-40)%24 != 0 || order.Uint64(p[32:]) != uint64((len(p)-40)/24) {
		return c, bad("catalog count")
	}
	copy(c.UUID[:], p[:16])
	c.Cutoff, c.ActiveID = order.Uint64(p[16:]), order.Uint64(p[24:])
	if c.ActiveID == 0 {
		return c, bad("catalog active identity")
	}
	seen := map[uint64]bool{c.ActiveID: true}
	logSeen := false
	for q := p[40:]; len(q) != 0; q = q[24:] {
		id, n, kind := order.Uint64(q), order.Uint64(q[8:]), order.Uint64(q[16:])
		if id == 0 || seen[id] || n < SegmentHeaderBytes || n > math.MaxInt64 ||
			kind != Log && kind != Snapshot || kind == Snapshot && logSeen || kind == Snapshot && c.Cutoff == 0 {
			return c, bad("catalog entry")
		}
		logSeen = logSeen || kind == Log
		seen[id] = true
		c.Files = append(c.Files, File{ID: id, Bytes: int64(n), Kind: uint32(kind)})
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
	UUID                                                 [16]byte
	Sequence, Cutoff                                     uint64
	SourceCacheBytes, SourceRecordBytes, SourceFileBytes int64
	Files                                                []BackupFile
}

func EncodeBackup(b Backup) []byte {
	p := make([]byte, 64)
	copy(p, b.UUID[:])
	for i, n := range []uint64{b.Sequence, b.Cutoff, uint64(b.SourceCacheBytes), uint64(b.SourceRecordBytes), uint64(b.SourceFileBytes), uint64(len(b.Files))} {
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
	return envelope("TARPBAK1", p)
}

func DecodeBackup(b []byte) (Backup, error) {
	var result Backup
	p, err := payload(b, "TARPBAK1")
	if err != nil {
		return result, err
	}
	if len(p) < 64 || order.Uint64(p[56:]) > uint64((len(p)-64)/43) {
		return result, bad("backup count")
	}
	copy(result.UUID[:], p[:16])
	result.Sequence, result.Cutoff = order.Uint64(p[16:]), order.Uint64(p[24:])
	for i, target := range []*int64{&result.SourceCacheBytes, &result.SourceRecordBytes, &result.SourceFileBytes} {
		n := order.Uint64(p[32+8*i:])
		if n > math.MaxInt64 {
			return result, bad("backup source settings")
		}
		*target = int64(n)
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
