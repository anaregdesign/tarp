package tarp_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestInspectKeyMatchesRecoveryWithoutChangingDirectory(t *testing.T) {
	for _, state := range []string{"closed", "unclean"} {
		t.Run(state, func(t *testing.T) {
			dir := t.TempDir()
			if state == "unclean" {
				runInspectKeyChild(t, dir, "write")
				// Recovery must ignore these bytes after the committed prefix.
				fd, err := os.OpenFile(inspectActiveDATA(t, dir), os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = fd.WriteString("uncommitted suffix")
				if err := errors.Join(err, fd.Close()); err != nil {
					t.Fatal(err)
				}
			} else {
				db := openPublicDB(t, dir, tarp.Options{})
				seedInspectKeys(t, db)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// Inspection needs only read permission on the stable ownership file.
			if err := os.Chmod(filepath.Join(dir, "LOCK"), 0400); err != nil {
				t.Fatal(err)
			}
			clone := t.TempDir()
			before := inspectInventory(t, dir, clone)
			recovered, err := tarp.OpenExisting(t.Context(), clone, tarp.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			for _, key := range []string{"manifest", "empty", "deleted", "missing"} {
				want, wantErr := recovered.Get([]byte(key))
				got, err := tarp.InspectKey(t.Context(), dir, []byte(key))
				if string(got) != string(want) || !errors.Is(err, wantErr) {
					t.Fatalf("InspectKey(%q) = %q, %v; recovered %q, %v", key, got, err, want, wantErr)
				}
			}
			value, err := tarp.InspectKey(t.Context(), dir, []byte("manifest"))
			if err != nil || string(value) != "latest" {
				t.Fatalf("manifest: %q, %v", value, err)
			}
			value[0] = 'X'
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if value, err := tarp.InspectKey(ctx, dir, []byte("manifest")); value != nil || !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled inspection: %q, %v", value, err)
			}
			if !maps.Equal(before, inspectInventory(t, dir, "")) {
				t.Fatal("inspection changed source contents or names")
			}
		})
	}
}

func TestInspectKeyRefusesLiveOwnerInSameAndOtherProcess(t *testing.T) {
	dir := t.TempDir()
	openPublicDB(t, dir, tarp.Options{})
	before := inspectInventory(t, dir, "")
	if value, err := tarp.InspectKey(t.Context(), dir, []byte("manifest")); value != nil || !errors.Is(err, tarp.ErrLocked) {
		t.Fatalf("inspect with a live owner: %q, %v", value, err)
	}
	runInspectKeyChild(t, dir, "locked")
	if !maps.Equal(before, inspectInventory(t, dir, "")) {
		t.Fatal("refused inspection changed source contents or names")
	}
}

func TestInspectKeyRejectsMissingLockAndCorruptCommittedData(t *testing.T) {
	key := []byte("manifest")
	root := t.TempDir()
	missing := filepath.Join(root, "missing")
	if value, err := tarp.InspectKey(t.Context(), missing, key); value != nil || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("inspect missing directory: %q, %v", value, err)
	}
	if entries, err := os.ReadDir(root); err != nil || len(entries) != 0 {
		t.Fatalf("missing inspection created entries: %v, %v", entries, err)
	}
	dir := t.TempDir()
	db := openPublicDB(t, dir, tarp.Options{IndexMode: tarp.IndexMemory})
	seedInspectKeys(t, db)
	backup, restored := filepath.Join(root, "image"), filepath.Join(root, "restored")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := tarp.Restore(t.Context(), tarp.RestoreRequest{Source: backup, Destination: restored}); err != nil {
		t.Fatal(err)
	}
	before := inspectInventory(t, restored, "")
	if value, err := tarp.InspectKey(t.Context(), restored, key); string(value) != "latest" || err != nil {
		t.Fatalf("inspect restored database before OpenExisting: %q, %v", value, err)
	}
	if !maps.Equal(before, inspectInventory(t, restored, "")) {
		t.Fatal("inspection changed the restored database")
	}
	if err := os.Remove(filepath.Join(restored, "LOCK")); err != nil {
		t.Fatal(err)
	}
	before = inspectInventory(t, restored, "")
	if value, err := tarp.InspectKey(t.Context(), restored, key); value != nil || !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("inspect without existing LOCK: %q, %v", value, err)
	}
	for _, open := range []func(context.Context, string, tarp.Options) (*tarp.DB, error){tarp.OpenContext, tarp.OpenExisting} {
		if owner, err := open(t.Context(), restored, tarp.Options{}); owner != nil || !errors.Is(err, tarp.ErrCorrupt) || !errors.Is(err, fs.ErrNotExist) {
			if owner != nil {
				owner.Close()
			}
			t.Fatalf("open without existing LOCK: %v, %v", owner, err)
		}
	}
	if !maps.Equal(before, inspectInventory(t, restored, "")) {
		t.Fatal("refusing a missing LOCK changed the restored database")
	}
	if value, err := tarp.InspectKey(t.Context(), backup, key); value != nil || !errors.Is(err, tarp.ErrBackupDirectory) {
		t.Fatalf("inspect backup: %q, %v", value, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := inspectActiveDATA(t, dir)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1 // Damage a record belonging to another key.
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	before = inspectInventory(t, dir, "")
	if value, err := tarp.InspectKey(t.Context(), dir, key); value != nil || !errors.Is(err, tarp.ErrCorrupt) || errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("inspect corrupt database: %q, %v", value, err)
	}
	if !maps.Equal(before, inspectInventory(t, dir, "")) {
		t.Fatal("corrupt inspection changed source contents or names")
	}
}

func runInspectKeyChild(t *testing.T, dir, mode string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestInspectKeyChild$")
	cmd.Env = append(os.Environ(), "TARP_TEST_INSPECT_DIRECTORY="+dir, "TARP_TEST_INSPECT_MODE="+mode)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("inspection subprocess (%s): %v\n%s", mode, err, output)
	}
}

func TestInspectKeyChild(t *testing.T) {
	dir := os.Getenv("TARP_TEST_INSPECT_DIRECTORY")
	switch os.Getenv("TARP_TEST_INSPECT_MODE") {
	case "write":
		db := openPublicDB(t, dir, tarp.Options{})
		seedInspectKeys(t, db)
		os.Exit(0) // Leave the acknowledged data and working files as-is.
	case "locked":
		if value, err := tarp.InspectKey(t.Context(), dir, []byte("manifest")); value != nil || !errors.Is(err, tarp.ErrLocked) {
			t.Fatalf("inspect with another process holding LOCK: %q, %v", value, err)
		}
	default:
		t.Skip("inspection subprocess")
	}
}

func seedInspectKeys(t *testing.T, db *tarp.DB) {
	t.Helper()
	for _, pair := range [][2]string{{"manifest", "old"}, {"empty", ""}, {"manifest", "latest"}, {"deleted", "old"}} {
		if err := db.Put([]byte(pair[0]), []byte(pair[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("deleted")); err != nil {
		t.Fatal(err)
	}
}

func inspectActiveDATA(t *testing.T, dir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := format.DecodeFrontier(raw)
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, storage.DataName(current.UUID, current.ActiveID))
}

func inspectInventory(t *testing.T, dir, clone string) map[string][sha256.Size]byte {
	t.Helper()
	files := make(map[string][sha256.Size]byte)
	err := fs.WalkDir(os.DirFS(dir), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if name == "." {
			return nil
		}
		if entry.IsDir() {
			files[name] = [sha256.Size]byte{}
			if clone != "" {
				return os.Mkdir(filepath.Join(clone, name), 0700)
			}
			return nil
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return err
		}
		files[name] = sha256.Sum256(data)
		if clone != "" {
			return os.WriteFile(filepath.Join(clone, name), data, 0600)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}
