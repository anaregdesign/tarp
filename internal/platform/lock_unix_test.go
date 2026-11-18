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

func TestLockRejectsNonRegularLockFileAndCanRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "LOCK")
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	lock, err := Lock(dir)
	if err == nil || lock != nil || errors.Is(err, ErrLocked) {
		t.Fatalf("Lock with directory in place of LOCK: %v, %v", lock, err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		t.Fatalf("invalid LOCK was replaced or removed: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	lock, err = Lock(dir)
	if err != nil {
		t.Fatalf("retry after removing invalid LOCK: %v", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
}
