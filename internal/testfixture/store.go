// Package testfixture builds canonical on-disk images for engine and measurement tests.
package testfixture

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// WriteMixedStore writes a checkpoint, two cleaned intervals, a raw log and an active tail.
func WriteMixedStore(t testing.TB, prune uint64) string {
	t.Helper()
	dir := t.TempDir()
	uuid := [16]byte{0x67}
	write := func(name string, b []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	segment := func(id uint64, kind uint32, first, last uint64, records ...[]byte) format.File {
		b := format.EncodeSegment(format.Segment{ID: id, Kind: kind, UUID: uuid})
		for _, record := range records {
			b = append(b, record...)
		}
		write(storage.DataName(uuid, id), b)
		return format.File{ID: id, Kind: kind, Bytes: int64(len(b)), First: first, Last: last, Digest: sha256.Sum256(b)}
	}
	catalog := format.Catalog{
		UUID: uuid, Cutoff: 2, PrunedThrough: prune, ActiveID: 4,
		Files: []format.File{
			segment(1, format.Snapshot, 0, 0, format.EncodeRecord(format.Put, 2, []byte("checkpoint"), []byte("kept"))),
			{Kind: format.Cleaned, First: 3, Last: 4},
			segment(2, format.Cleaned, 5, 7, format.EncodeRecord(format.Put, 6, []byte("empty"), nil),
				format.EncodeRecord(format.Delete, 7, []byte("pruned-snapshot-key"), nil)),
			segment(3, format.Log, 8, 8, format.EncodeRecord(format.Delete, 8, []byte("pruned-put"), nil)),
		},
	}
	active := segment(4, format.Log, 9, 9, format.EncodeRecord(format.Put, 9, []byte("tail"), []byte("new")))
	manifest := format.EncodeCatalog(catalog)
	write(storage.ManifestName(uuid, 5), manifest)
	write("CURRENT", format.EncodeFrontier(format.Frontier{
		UUID: uuid, Generation: 13, ManifestID: 5, ManifestHash: sha256.Sum256(manifest),
		ActiveID: active.ID, ActiveEnd: active.Bytes, Sequence: 9,
	}))
	return dir
}
