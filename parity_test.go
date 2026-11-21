package tarp_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestRepairParityRecoversSealedDATAWhenOpenFails(t *testing.T) {
	dir := t.TempDir()
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("history-"), 1200)
	if err := db.Put([]byte("k"), value); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "db")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if err := tarp.RepairParity(t.Context(), alias); !errors.Is(err, tarp.ErrLocked) {
		t.Fatalf("repair under alias while Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	sidecars, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
	if err != nil || len(sidecars) != 1 {
		t.Fatalf("parity files %v: %v", sidecars, err)
	}
	path := filepath.Join(dir, "DATA-"+strings.TrimPrefix(filepath.Base(sidecars[0]), "PARITY-"))
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(original)
	bad[0] ^= 1
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{}); got != nil || !errors.Is(err, tarp.ErrCorrupt) {
		t.Fatalf("Open with corrupt sealed header: %v", err)
	}
	if err := tarp.RepairParity(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, original) {
		t.Fatalf("bytes after repair: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := tarp.RepairParity(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	again, err := os.Stat(path)
	if err != nil || !os.SameFile(info, again) {
		t.Fatalf("healthy DATA replaced: %v", err)
	}
	reopened, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get([]byte("k")); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("Get after offline repair: %v", err)
	}
}

func TestParityRebuildsAfterRestoreAndCompaction(t *testing.T) {
	ctx := t.Context()
	parityFiles := func(dir string) []string {
		t.Helper()
		files, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	onlyParity := func(dir string) string {
		t.Helper()
		files := parityFiles(dir)
		if len(files) != 1 {
			t.Fatalf("parity files in %s: %v", dir, files)
		}
		return files[0]
	}
	source := t.TempDir()
	db := openPublicDB(t, source, tarp.Options{})
	if err := db.Put([]byte("sealed"), []byte("protected")); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("active"), []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if indexes, err := filepath.Glob(filepath.Join(source, "INDEX-*")); err != nil || len(indexes) != 0 {
		t.Fatalf("explicit seal created an index: %v, %v", indexes, err)
	}
	originals := make(map[string][]byte)
	for _, path := range parityFiles(source) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		originals[filepath.Base(path)] = b
	}
	if len(originals) != 2 {
		t.Fatalf("explicit seals protected %d files, want two", len(originals))
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if files := parityFiles(backup); len(files) != 0 {
		t.Fatalf("logical backup copied parity: %v", files)
	}
	if info, err := tarp.VerifyBackup(ctx, backup); err != nil || info.LiveKeys != 2 {
		t.Fatalf("verify backup: %+v, %v", info, err)
	}

	restore := filepath.Join(t.TempDir(), "restored")
	if err := tarp.Restore(ctx, tarp.RestoreRequest{Source: backup, Destination: restore}); err != nil {
		t.Fatal(err)
	}
	if files := parityFiles(restore); len(files) != 0 {
		t.Fatalf("closed restored DB contains unexpected parity: %v", files)
	}
	restored := openPublicDB(t, restore, tarp.Options{})
	if err := restored.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	recreated := parityFiles(restore)
	if len(recreated) != len(originals) {
		t.Fatalf("regenerated %d parity files, want %d", len(recreated), len(originals))
	}
	for _, path := range recreated {
		want, found := originals[filepath.Base(path)]
		if got, err := os.ReadFile(path); err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("regenerated parity differs: %s, %v", path, err)
		}
	}

	result, err := restored.Compact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if result.PinnedRetiredDataBytes > 0 {
		// ProtectParity has now joined any work on the old selection.
		if _, err := restored.Compact(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, old := range recreated {
		data := "DATA-" + strings.TrimPrefix(filepath.Base(old), "PARITY-")
		for _, path := range []string{old, filepath.Join(restore, data)} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retired object %s: %v", path, err)
			}
		}
	}
	newSidecar := onlyParity(restore)
	if _, reused := originals[filepath.Base(newSidecar)]; reused {
		t.Fatal("compacted snapshot reused retired parity")
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(restore, "DATA-"+strings.TrimPrefix(filepath.Base(newSidecar), "PARITY-"))
	if err := os.Truncate(snapshot, 0); err != nil {
		t.Fatal(err)
	}
	if err := tarp.RepairParity(ctx, restore); err != nil {
		t.Fatal(err)
	}
	repaired := openPublicDB(t, restore, tarp.Options{})
	for k, v := range map[string]string{"sealed": "protected", "active": "tail"} {
		if got, err := repaired.Get([]byte(k)); err != nil || string(got) != v {
			t.Fatalf("Get(%q) after recovering compacted snapshot: %q, %v", k, got, err)
		}
	}
}
