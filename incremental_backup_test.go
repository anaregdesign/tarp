package tarp_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestIncrementalBackupReusesSealedDataAndRestoresEachGeneration(t *testing.T) {
	ctx := t.Context()
	db, err := tarp.Open(t.TempDir(), tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("old"), []byte("version one")); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	repository := t.TempDir()
	makeBackup := func(name string) tarp.IncrementalBackupResult {
		t.Helper()
		result, err := db.BackupIncremental(ctx, tarp.IncrementalBackupRequest{Repository: repository, Name: name})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := makeBackup("first")
	if first.LastUserSequence != 1 || first.ReusedDataBytes != 0 || first.CopiedDataBytes == 0 {
		t.Fatalf("first backup reported %+v", first)
	}
	if err := db.Put([]byte("new"), []byte("version two")); err != nil {
		t.Fatal(err)
	}
	second := makeBackup("second")
	if second.LastUserSequence != 2 || second.ReusedDataBytes == 0 || second.CopiedDataBytes == 0 {
		t.Fatalf("second backup reported %+v", second)
	}
	if _, err := db.BackupIncremental(ctx, tarp.IncrementalBackupRequest{Repository: repository, Name: "second"}); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("reusing a generation name returned %v", err)
	}
	if err := db.Delete([]byte("old")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if third := makeBackup("third"); third.LastUserSequence != 3 {
		t.Fatalf("third backup reported %+v", third)
	}
	restore := func(name string, sequence uint64, want map[string]string) {
		t.Helper()
		source := filepath.Join(repository, name)
		info, err := tarp.VerifyBackupWithScratch(ctx, source, t.TempDir())
		if err != nil || info.LastUserSequence != sequence || info.LiveKeys != uint64(len(want)) {
			t.Fatalf("VerifyBackupWithScratch(%s) = %+v, %v", name, info, err)
		}
		destination := filepath.Join(t.TempDir(), "restored")
		if err := tarp.Restore(ctx, tarp.RestoreRequest{Source: source, Destination: destination}); err != nil {
			t.Fatal(err)
		}
		restored, err := tarp.OpenExisting(ctx, destination, tarp.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer restored.Close()
		for _, key := range []string{"old", "new"} {
			value, err := restored.Get([]byte(key))
			if expected, found := want[key]; found {
				if err != nil || string(value) != expected {
					t.Fatalf("Get(%s) = %q, %v; want %q", key, value, err, expected)
				}
			} else if !errors.Is(err, tarp.ErrNotFound) {
				t.Fatalf("Get(%s) = %v, want ErrNotFound", key, err)
			}
		}
	}
	restore("first", 1, map[string]string{"old": "version one"})
	if err := os.RemoveAll(filepath.Join(repository, "first")); err != nil {
		t.Fatal(err)
	}
	if err := tarp.PruneIncrementalBackupObjects(ctx, repository); err != nil {
		t.Fatal(err)
	}
	restore("second", 2, map[string]string{"old": "version one", "new": "version two"})
	restore("third", 3, map[string]string{"new": "version two"})
}
