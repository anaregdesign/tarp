package qualification

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

func testNBDClient(t *testing.T, disk *Disk) (net.Conn, <-chan error) {
	t.Helper()
	server, client := net.Pipe()
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		done <- ServeNBD(context.Background(), server, disk)
	}()
	t.Cleanup(func() { _ = client.Close() })
	var greeting [18]byte
	if _, err := io.ReadFull(client, greeting[:]); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint64(greeting[:8]) != nbdMagic {
		t.Fatal("invalid greeting")
	}
	if err := writeFull(client, []byte{0, 0, 0, 3}); err != nil {
		t.Fatal(err)
	}
	for _, option := range []uint32{8, 7, 1} {
		var request [16]byte
		binary.BigEndian.PutUint64(request[:8], optionMagic)
		binary.BigEndian.PutUint32(request[8:12], option)
		if err := writeFull(client, request[:]); err != nil {
			t.Fatal(err)
		}
		if option != 1 {
			var reply [20]byte
			if _, err := io.ReadFull(client, reply[:]); err != nil {
				t.Fatal(err)
			}
			if binary.BigEndian.Uint32(reply[12:16]) != optionUnknown {
				t.Fatal("unexpected optional-negotiation result")
			}
		}
	}
	var export [10]byte
	if _, err := io.ReadFull(client, export[:]); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint64(export[:8]) != uint64(disk.Size()) || binary.BigEndian.Uint16(export[8:]) != exportFlags {
		t.Fatal("device must advertise its real size, FLUSH and FUA")
	}
	return client, done
}

func nbdRequest(t *testing.T, conn net.Conn, command, flags uint16, offset uint64, payload []byte) {
	t.Helper()
	var request [28]byte
	binary.BigEndian.PutUint32(request[:4], requestMagic)
	binary.BigEndian.PutUint16(request[4:6], flags)
	binary.BigEndian.PutUint16(request[6:8], command)
	binary.BigEndian.PutUint64(request[8:16], 42)
	binary.BigEndian.PutUint64(request[16:24], offset)
	binary.BigEndian.PutUint32(request[24:], uint32(len(payload)))
	if err := writeFull(conn, request[:]); err != nil {
		t.Fatal(err)
	}
	if command == 1 {
		if err := writeFull(conn, payload); err != nil {
			t.Fatal(err)
		}
	}
	var reply [16]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(reply[:4]) != replyMagic || binary.BigEndian.Uint32(reply[4:8]) != 0 ||
		binary.BigEndian.Uint64(reply[8:]) != 42 {
		t.Fatalf("invalid NBD reply: %x", reply)
	}
}

func TestNBDFlushFUAAndDisconnectControls(t *testing.T) {
	for _, mode := range []string{"volatile", "flush", "fua"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "disk.raw")
			disk, err := CreateDisk(path, pageBytes)
			if err != nil {
				t.Fatal(err)
			}
			conn, done := testNBDClient(t, disk)
			payload := bytes.Repeat([]byte{0x58}, 512)
			flags := uint16(0)
			if mode == "fua" {
				flags = 1
				nbdRequest(t, conn, 1, 0, 1024, bytes.Repeat([]byte{0xa3}, 512))
			}
			nbdRequest(t, conn, 1, flags, 0, payload)
			if mode == "flush" {
				nbdRequest(t, conn, 3, 0, 0, nil)
			}
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, io.EOF) {
				t.Fatalf("unexpected server termination: %v", err)
			}
			disk.PowerCut()
			if err := disk.Close(); err != nil {
				t.Fatal(err)
			}
			persisted, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			expected := make([]byte, pageBytes)
			if mode != "volatile" {
				copy(expected, payload)
			}
			if !bytes.Equal(persisted, expected) {
				t.Fatalf("%s violated independent backing-file control", mode)
			}
		})
	}
}
