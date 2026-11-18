//go:build linux || darwin

package platform

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLockAndAlias(t *testing.T) {
	dir := t.TempDir()
	l, err := Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if other, err := Lock(alias); !errors.Is(err, ErrLocked) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("duplicate alias: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Lock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

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
