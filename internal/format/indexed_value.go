package format

import (
	"bytes"
	"hash/crc32"
)

// DecodeIndexedValue checks one complete indexed record from a borrowed read
// buffer and returns independently owned value bytes.
func DecodeIndexedValue(record, key []byte, sequence uint64) ([]byte, error) {
	if len(record) < RecordHeaderBytes {
		return nil, bad("incomplete committed record header")
	}
	r, err := decodeIndexedHeader(record[:RecordHeaderBytes], int64(len(record)), len(key), sequence)
	if err != nil {
		return nil, err
	}
	payload := record[RecordHeaderBytes:]
	if !bytes.Equal(payload[:len(key)], key) || crc32.Checksum(payload, table) != r.PayloadCRC {
		return nil, bad("indexed record payload")
	}
	return bytes.Clone(payload[len(key):]), nil
}

func decodeIndexedHeader(header []byte, size int64, keyBytes int, sequence uint64) (Record, error) {
	r, err := DecodeRecord(header, size)
	if err != nil {
		return Record{}, err
	}
	if r.Bytes() != size || r.Sequence != sequence || r.Kind != Put || r.KeyBytes != int64(keyBytes) {
		return Record{}, bad("indexed record identity")
	}
	return r, nil
}
