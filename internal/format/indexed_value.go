package format

import (
	"bytes"
	"hash/crc32"
)

// DecodeIndexedValue checks one complete indexed record from a borrowed read
// buffer and returns independently owned value bytes.
func DecodeIndexedValue(record, key []byte, sequence uint64) ([]byte, error) {
	if _, err := ValidateIndexedRecord(record, key, sequence); err != nil {
		return nil, err
	}
	return bytes.Clone(record[RecordHeaderBytes+len(key):]), nil
}

// ValidateIndexedRecord verifies a complete indexed Put and returns the
// checksum of its key and value without making a copy of either.
func ValidateIndexedRecord(record, key []byte, sequence uint64) (uint32, error) {
	if len(record) < RecordHeaderBytes {
		return 0, bad("incomplete committed record header")
	}
	r, err := decodeIndexedHeader(record[:RecordHeaderBytes], int64(len(record)), len(key), sequence)
	if err != nil {
		return 0, err
	}
	payload := record[RecordHeaderBytes:]
	if !bytes.Equal(payload[:len(key)], key) || crc32.Checksum(payload, table) != r.PayloadCRC {
		return 0, bad("indexed record payload")
	}
	return r.PayloadCRC, nil
}

func decodeIndexedHeader(header []byte, size int64, keyBytes int, sequence uint64) (RecordHeader, error) {
	r, err := DecodeRecordHeader(header, size)
	if err != nil {
		return RecordHeader{}, err
	}
	if r.EncodedRecordBytes() != size || r.Sequence != sequence || r.Kind != Put || r.KeyBytes != int64(keyBytes) {
		return RecordHeader{}, bad("indexed record identity")
	}
	return r, nil
}
