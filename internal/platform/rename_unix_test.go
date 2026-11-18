//go:build (linux || darwin) && (amd64 || arm64)

package platform

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRenameNoReplace(t *testing.T) {
	parent := t.TempDir()
	a, b := filepath.Join(parent, "a"), filepath.Join(parent, "b")
	if err := os.Mkdir(a, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(b, 0700); err != nil {
		t.Fatal(err)
	}
	if err := RenameNoReplace(a, b); !errors.Is(err, os.ErrExist) {
		t.Fatalf("replaced existing empty destination: %v", err)
	}
	if err := os.Remove(b); err != nil {
		t.Fatal(err)
	}
	if err := RenameNoReplace(a, b); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(b); err != nil {
		t.Fatal(err)
	}
}

func TestRenameNoReplacePreservesSourceWhenParentIsMissing(t *testing.T) {
	parent := t.TempDir()
	source, target := filepath.Join(parent, "stage"), filepath.Join(parent, "missing", "target")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	err := RenameNoReplace(source, target)
	var link *os.LinkError
	if !errors.Is(err, os.ErrNotExist) || !errors.As(err, &link) || link.Old != source || link.New != target {
		t.Fatalf("rename from %q to %q: %v", source, target, err)
	}
	if _, err := os.Stat(source); err != nil {
		t.Fatalf("missing staging directory after failed publication: %v", err)
	}
}
