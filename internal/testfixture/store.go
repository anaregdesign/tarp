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
	segment := func(id uint64, kind uint32, first, last uint64, records ...[]byte) format.CatalogEntry {
		b := format.EncodeSegmentHeader(format.SegmentHeader{ID: id, Kind: kind, UUID: uuid})
		for _, record := range records {
			b = append(b, record...)
		}
		write(storage.DataName(uuid, id), b)
		return format.CatalogEntry{ID: id, Kind: kind, Bytes: int64(len(b)), FirstSequence: first, LastSequence: last, DataFileDigest: sha256.Sum256(b)}
	}
	catalog := format.Catalog{
		UUID: uuid, CheckpointCutoffSequence: 2, PrunedThroughSequence: prune, ActiveID: 4,
		Entries: []format.CatalogEntry{
			segment(1, format.SnapshotKind, 0, 0, format.EncodeRecord(format.Put, 2, []byte("checkpoint"), []byte("kept"))),
			{Kind: format.CleanedLogKind, FirstSequence: 3, LastSequence: 4},
			segment(2, format.CleanedLogKind, 5, 7, format.EncodeRecord(format.Put, 6, []byte("empty"), nil),
				format.EncodeRecord(format.Delete, 7, []byte("pruned-snapshot-key"), nil)),
			segment(3, format.RawLogKind, 8, 8, format.EncodeRecord(format.Delete, 8, []byte("pruned-put"), nil)),
		},
	}
	active := segment(4, format.RawLogKind, 9, 9, format.EncodeRecord(format.Put, 9, []byte("tail"), []byte("new")))
	manifest := format.EncodeCatalog(catalog)
	write("LOCK", nil)
	write(storage.ManifestName(uuid, 5), manifest)
	write("CURRENT", format.EncodeFrontier(format.Frontier{
		UUID: uuid, PublicationGeneration: 13, ManifestID: 5, ManifestDigest: sha256.Sum256(manifest),
		ActiveID: active.ID, ActiveCommittedEndOffset: active.Bytes, LastUserSequence: 9,
	}))
	return dir
}
