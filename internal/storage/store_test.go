package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenPreservesUnrecognizedTemporaryFilesBeforeCleanup(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Handles: 1}
	store, _, err := Open(dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	uuid := store.Current.UUID
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf(".tarp-current-%x-", uuid)
	owned, err := os.CreateTemp(dir, prefix+"*.tmp")
	if err != nil {
		t.Fatal(err)
	}
	if err := owned.Close(); err != nil {
		t.Fatal(err)
	}
	otherUUID := uuid
	otherUUID[0] ^= 1
	for _, name := range []string{prefix + ".tmp", prefix + "123", fmt.Sprintf(".tarp-current-%x-123.tmp", otherUUID)} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("preserve me"), 0600); err != nil {
			t.Fatal(err)
		}
		got, _, err := Open(dir, cfg, nil)
		if got != nil {
			got.Close()
		}
		if got != nil || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open with unrecognized name %q returned %v", name, err)
		}
		if b, err := os.ReadFile(path); err != nil || string(b) != "preserve me" {
			t.Fatalf("file %q after failed Open: %q, %v", name, b, err)
		}
		if _, err := os.Stat(owned.Name()); err != nil {
			t.Fatalf("cleanup ran before validation for %q: %v", name, err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	reopened, _, err := Open(dir, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := os.Stat(owned.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recognized temporary file after valid Open: %v", err)
	}
}
