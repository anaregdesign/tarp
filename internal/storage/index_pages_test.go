package storage

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/index/btree"
)

func TestIndexHashBlocksVerifyMultipleLevelsAndRejectAlteredDigests(t *testing.T) {
	const pages = indexHashFanout + 1
	fd, err := os.Create(filepath.Join(t.TempDir(), "index"))
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	writer := &indexPageWriter{packed: &outputWriter{fd: fd}, fd: fd}
	var header [btree.PackedHeaderBytes]byte
	binary.LittleEndian.PutUint64(header[24:32], pages)
	if _, err := writer.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	var page [btree.PackedPageBytes]byte
	for i := range pages {
		page[0] = byte(i + 1)
		if _, err := writer.Write(page[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.finish(); err != nil {
		t.Fatal(err)
	}
	if len(writer.levels) != 2 || writer.levels[0].blocks != 2 || writer.levels[1].blocks != 1 {
		t.Fatalf("unexpected hash levels: %+v", writer.levels)
	}
	reader := verifiedIndexPages{
		r: fd, size: writer.size, root: writer.root, header: header[:],
		levels: writer.levels, cached: make([]indexHashCache, len(writer.levels)),
	}
	for _, i := range []int64{0, pages - 1, 1, pages - 2} {
		n, err := reader.ReadAt(page[:], btree.PackedHeaderBytes+i*btree.PackedPageBytes)
		if n != len(page) || page[0] != byte(i+1) || err != nil {
			t.Fatalf("page %d = %d bytes, marker %d, error %v", i, n, page[0], err)
		}
	}
	// Replacing a leaf with its valid digest in the bottom block does not
	// preserve the hash of that block recorded in its authenticated parent.
	offset := int64(btree.PackedHeaderBytes) + (pages-1)*btree.PackedPageBytes
	clear(page[:])
	page[0] = 0xff
	if _, err := fd.WriteAt(page[:], offset); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(page[:])
	leafOffset := writer.levels[0].offset + indexHashBlockSize
	if _, err := fd.WriteAt(digest[:], leafOffset); err != nil {
		t.Fatal(err)
	}
	clear(reader.cached)
	if _, err := reader.ReadAt(page[:], offset); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("altered leaf and digest read = %v, want integrity failure", err)
	}
}
