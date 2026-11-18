package qualification

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestAppendedInitramfsPreservesUbuntuAndExecutable(t *testing.T) {
	dir := t.TempDir()
	source, init, target := filepath.Join(dir, "ubuntu"), filepath.Join(dir, "init"), filepath.Join(dir, "combined")
	prefix, binary := []byte("pinned-original-initramfs"), []byte("static-init-payload")
	for path, data := range map[string][]byte{source: prefix, init: binary} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := AppendInitramfs(source, init, target); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(data, prefix) {
		t.Fatal("original Ubuntu initramfs changed")
	}
	gz, err := gzip.NewReader(bytes.NewReader(data[len(prefix):]))
	if err != nil {
		t.Fatal(err)
	}
	archive, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	field := func(begin int) uint64 {
		t.Helper()
		n, err := strconv.ParseUint(string(archive[begin:begin+8]), 16, 32)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if string(archive[:6]) != "070701" || field(14) != 0100755 ||
		field(54) != uint64(len(binary)) || field(94) != uint64(len("tarp-init")+1) {
		t.Fatal("invalid executable newc entry")
	}
	if string(archive[110:120]) != "tarp-init\x00" || !bytes.Equal(archive[120:120+len(binary)], binary) {
		t.Fatal("init entry name, alignment or payload changed")
	}
	if !bytes.Contains(archive[120+len(binary):], []byte("TRAILER!!!\x00")) {
		t.Fatal("missing archive terminator")
	}
	if err := AppendInitramfs(source, init, target); err == nil {
		t.Fatal("overwrote an existing boot image")
	}
}
