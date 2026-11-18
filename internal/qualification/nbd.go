package qualification

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

const (
	nbdMagic      = uint64(0x4e42444d41474943)
	optionMagic   = uint64(0x49484156454f5054)
	optionReply   = uint64(0x0003e889045565a9)
	requestMagic  = uint32(0x25609513)
	replyMagic    = uint32(0x67446698)
	exportFlags   = uint16(1 | 4 | 8) // HAS_FLAGS, SEND_FLUSH, SEND_FUA
	maxRequest    = 32 << 20
	errorInvalid  = uint32(22)
	optionUnknown = uint32(0x80000001)
)

func writeFull(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func negotiate(conn net.Conn, disk *Disk) error {
	var greeting [18]byte
	binary.BigEndian.PutUint64(greeting[:8], nbdMagic)
	binary.BigEndian.PutUint64(greeting[8:16], optionMagic)
	binary.BigEndian.PutUint16(greeting[16:], 3) // FIXED_NEWSTYLE and NO_ZEROES
	if err := writeFull(conn, greeting[:]); err != nil {
		return err
	}
	var client [4]byte
	if _, err := io.ReadFull(conn, client[:]); err != nil {
		return err
	}
	flags := binary.BigEndian.Uint32(client[:])
	if flags&1 == 0 || flags&^uint32(3) != 0 {
		return errors.New("qualification: unsupported NBD client flags")
	}
	for {
		var header [16]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return err
		}
		option, length := binary.BigEndian.Uint32(header[8:12]), binary.BigEndian.Uint32(header[12:])
		if binary.BigEndian.Uint64(header[:8]) != optionMagic || length > 64<<10 {
			return errors.New("qualification: invalid NBD option")
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return err
		}
		if option == 1 {
			if string(payload) != "" && string(payload) != "tarp" {
				return errors.New("qualification: unknown NBD export")
			}
			size := 10
			if flags&2 == 0 {
				size += 124
			}
			export := make([]byte, size)
			binary.BigEndian.PutUint64(export[:8], uint64(disk.Size()))
			binary.BigEndian.PutUint16(export[8:10], exportFlags)
			return writeFull(conn, export)
		}
		var reply [20]byte
		binary.BigEndian.PutUint64(reply[:8], optionReply)
		binary.BigEndian.PutUint32(reply[8:12], option)
		kind := optionUnknown
		if option == 2 {
			kind = 1 // NBD_REP_ACK for an orderly negotiation abort.
		}
		binary.BigEndian.PutUint32(reply[12:16], kind)
		if err := writeFull(conn, reply[:]); err != nil {
			return err
		}
		if option == 2 {
			return io.EOF
		}
	}
}

// ServeNBD handles one fixed-newstyle client sequentially. Unsupported optional
// negotiation falls back to EXPORT_NAME. Disconnect never implies Flush.
func ServeNBD(ctx context.Context, conn net.Conn, disk *Disk) error {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if err := negotiate(conn, disk); err != nil {
		return err
	}
	for {
		var request [28]byte
		if _, err := io.ReadFull(conn, request[:]); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(request[:4]) != requestMagic {
			return errors.New("qualification: invalid NBD request magic")
		}
		flags := binary.BigEndian.Uint16(request[4:6])
		command := binary.BigEndian.Uint16(request[6:8])
		offset := binary.BigEndian.Uint64(request[16:24])
		length := binary.BigEndian.Uint32(request[24:])
		if length > maxRequest {
			return errors.New("qualification: NBD request exceeds 32 MiB")
		}
		var payload []byte
		if command == 1 {
			payload = make([]byte, length)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return err
			}
		}
		valid := offset <= uint64(disk.Size()) && uint64(length) <= uint64(disk.Size())-offset
		var result []byte
		var err error
		switch {
		case command == 0 && flags == 0 && valid:
			result = make([]byte, length)
			err = disk.ReadAt(result, int64(offset))
		case command == 1 && flags&^uint16(1) == 0 && valid:
			err = disk.WriteAt(payload, int64(offset), flags&1 != 0)
		case command == 2 && flags == 0 && offset == 0 && length == 0:
			return io.EOF
		case command == 3 && flags == 0 && offset == 0 && length == 0:
			err = disk.Flush()
		default:
			err = fmt.Errorf("qualification: invalid NBD command=%d flags=%d offset=%d length=%d", command, flags, offset, length)
		}
		var reply [16]byte
		binary.BigEndian.PutUint32(reply[:4], replyMagic)
		copy(reply[8:], request[8:16])
		if err != nil {
			binary.BigEndian.PutUint32(reply[4:8], errorInvalid)
		}
		if writeErr := writeFull(conn, reply[:]); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		if err != nil {
			return err
		}
		if len(result) != 0 {
			if err := writeFull(conn, result); err != nil {
				return err
			}
		}
	}
}
