package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"

	"github.com/anaregdesign/tarp/internal/format"
)

// Replay runs and relocation spools carry a fixed-size header followed by one
// complete key. The last four header bytes authenticate both header and key
// against accidental corruption before a scratch record influences publication.
func writeScratchRecord(w *bufio.Writer, header []byte, key string, used int64) (int64, error) {
	if int64(len(key)) > math.MaxInt64-int64(len(header))-used {
		return used, errors.New("scratch file too large")
	}
	crcAt := len(header) - 4
	sum := crc32.Update(crc32.ChecksumIEEE(header[:crcAt]), crc32.IEEETable, []byte(key))
	binary.LittleEndian.PutUint32(header[crcAt:], sum)
	if _, err := w.Write(header); err != nil {
		return used, err
	}
	if _, err := w.WriteString(key); err != nil {
		return used, err
	}
	return used + int64(len(header)) + int64(len(key)), nil
}

// The header has already been read. Bound the key by both the remaining spool
// bytes and the largest key admitted to this spool before allocating.
func readScratchKey(r *bufio.Reader, header []byte, recordBytes, valueBytes, remaining int64, maxKey int) (string, error) {
	if recordBytes <= format.RecordHeaderBytes || valueBytes < 0 || valueBytes >= recordBytes-format.RecordHeaderBytes {
		return "", errors.New("invalid scratch record sizes")
	}
	n := recordBytes - format.RecordHeaderBytes - valueBytes
	if n > remaining || n > int64(maxKey) {
		return "", io.ErrUnexpectedEOF
	}
	key := make([]byte, int(n))
	if _, err := io.ReadFull(r, key); err != nil {
		return "", err
	}
	crcAt := len(header) - 4
	sum := crc32.Update(crc32.ChecksumIEEE(header[:crcAt]), crc32.IEEETable, key)
	if sum != binary.LittleEndian.Uint32(header[crcAt:]) {
		return "", errors.New("invalid scratch record checksum")
	}
	return string(key), nil
}
