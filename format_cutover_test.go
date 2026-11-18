package tarp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// This opaque backup was produced by commit 1eb703808a595d15df603553bb2f873dedb39396.
// The new engine treats the unsupported outer signatures as opaque as well.
//
//go:embed testdata/previous-release/*
var previousReleaseFS embed.FS

func TestPreviousReleaseRejectionPreservesInput(t *testing.T) {
	// The full suite also runs as a standalone binary inside qualification guests.
	fixture, err := fs.Sub(previousReleaseFS, "testdata/previous-release")
	if err != nil {
		t.Fatal(err)
	}
	previous := filepath.Join(t.TempDir(), "backup")
	if err := os.CopyFS(previous, fixture); err != nil {
		t.Fatal(err)
	}
	for _, lock := range []bool{false, true} {
		dir := filepath.Join(t.TempDir(), "old-db")
		if err := os.CopyFS(dir, os.DirFS(previous)); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(filepath.Join(dir, "BACKUP")); err != nil {
			t.Fatal(err)
		}
		if lock {
			if err := os.WriteFile(filepath.Join(dir, "LOCK"), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
		files, err := filepath.Glob(filepath.Join(dir, "DATA-*"))
		if err != nil || len(files) != 1 {
			t.Fatalf("opaque fixture: %v, %v", files, err)
		}
		f, err := os.OpenFile(files[0], os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString("uncommitted suffix")
		if err := errors.Join(err, f.Close()); err != nil {
			t.Fatal(err)
		}
		before := directoryBytes(t, dir)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		// A second rejection also checks release of ownership on failure.
		for i := 0; i < 2; i++ {
			if _, err := Open(dir, Options{}); !errors.Is(err, ErrUnsupportedFormat) {
				t.Fatalf("lock=%t open=%d: %v", lock, i, err)
			}
		}
		after, err := os.ReadDir(dir)
		if err != nil || len(after) != len(entries) || !reflect.DeepEqual(directoryBytes(t, dir), before) {
			t.Fatalf("opening a foreign directory changed it: %v", err)
		}
	}
	before := directoryBytes(t, previous)
	target := filepath.Join(t.TempDir(), "restored")
	if err := Restore(context.Background(), previous, target, Options{}); !errors.Is(err, ErrUnsupportedFormat) {
		t.Fatalf("old backup: %v", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) || !reflect.DeepEqual(directoryBytes(t, previous), before) {
		t.Fatalf("restore changed its source or published a directory: %v", err)
	}
}

func writeMixedStore(t *testing.T, prune uint64) string {
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
		return format.File{ID: id, Kind: kind, Bytes: int64(len(b)), First: first, Last: last}
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

func TestMixedStoreCheckpointAndBackup(t *testing.T) {
	if _, err := Open(writeMixedStore(t, 7), Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("redundant Delete beyond pruning boundary: %v", err)
	}
	dir := writeMixedStore(t, 8)
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := Restore(context.Background(), backup, target, Options{}); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(target, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restored.Close() })
	for _, image := range []*DB{db, restored} {
		if _, err := image.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}
		for key, value := range map[string]string{"checkpoint": "kept", "tail": "new", "empty": ""} {
			b, err := image.Get([]byte(key))
			if err != nil || !bytes.Equal(b, []byte(value)) {
				t.Fatalf("Get(%q) = %q, %v", key, b, err)
			}
		}
		for _, key := range []string{"pruned-snapshot-key", "pruned-put"} {
			if _, err := image.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get(%q): %v", key, err)
			}
		}
		mustPut(t, image, "next", "write")
		image.writer.Lock()
		seq, cutoff, prune := image.store.Current.Sequence, image.store.Catalog.Cutoff, image.store.Catalog.PrunedThrough
		image.writer.Unlock()
		if seq != 10 || cutoff != 9 || prune != cutoff {
			t.Fatalf("checkpoint sequence/cutoff/pruning %d/%d/%d", seq, cutoff, prune)
		}
	}
}
