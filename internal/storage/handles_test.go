package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHandleLimitWaitsForEveryLeaseAndClosesEvictions(t *testing.T) {
	dir := t.TempDir()
	files := []*File{{Path: filepath.Join(dir, "a")}, {Path: filepath.Join(dir, "b")}}
	for _, file := range files {
		if err := os.WriteFile(file.Path, nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	pool := newHandles(1)
	first, err := pool.acquire(files[0])
	if err != nil {
		t.Fatal(err)
	}
	same, err := pool.acquire(files[0])
	if err != nil || same != first {
		t.Fatalf("shared positional-read handle: %v", err)
	}
	type result struct {
		handle *handle
		err    error
	}
	done := make(chan result, 1)
	go func() {
		next, err := pool.acquire(files[1])
		done <- result{next, err}
	}()
	pool.release(first)
	select {
	case got := <-done:
		t.Fatalf("evicted a still-leased descriptor: %v", got.err)
	case <-time.After(15 * time.Millisecond):
	}
	pool.release(same)
	next := <-done
	if next.err != nil {
		t.Fatal(next.err)
	}
	if _, err := first.fd.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("idle eviction leaked descriptor: %v", err)
	}
	pool.release(next.handle)
	if len(pool.items) != 1 {
		t.Fatalf("handle bound exceeded: %d", len(pool.items))
	}
	if err := pool.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := next.handle.fd.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("close leaked descriptor: %v", err)
	}
}

func TestInvalidationKeepsLeasedInodeUntilReleaseAndHonorsLimit(t *testing.T) {
	dir := t.TempDir()
	file := &File{Path: filepath.Join(dir, "DATA")}
	if err := os.WriteFile(file.Path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	pool := newHandles(1)
	old, err := pool.acquire(file)
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(dir, "replacement")
	if err := os.WriteFile(stage, []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(stage, file.Path); err != nil {
		t.Fatal(err)
	}
	if err := pool.invalidate(file); err != nil {
		t.Fatal(err)
	}
	if pool.items[file] != nil || pool.lru.Len() != 1 {
		t.Fatal("leased retired descriptor escaped the limit or remained visible")
	}
	type result struct {
		h   *handle
		err error
	}
	done := make(chan result, 1)
	go func() { h, err := pool.acquire(file); done <- result{h, err} }()
	var buf [3]byte
	if _, err := old.fd.ReadAt(buf[:], 0); err != nil || string(buf[:]) != "old" {
		t.Fatalf("old lease: %q, %v", buf, err)
	}
	select {
	case got := <-done:
		t.Fatalf("new descriptor exceeded the limit: %v", got.err)
	case <-time.After(15 * time.Millisecond):
	}
	pool.release(old)
	next := <-done
	if next.err != nil {
		t.Fatal(next.err)
	}
	if _, err := old.fd.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("old inode still open: %v", err)
	}
	if _, err := next.h.fd.ReadAt(buf[:], 0); err != nil || string(buf[:]) != "new" {
		t.Fatalf("fresh lease: %q, %v", buf, err)
	}
	pool.release(next.h)
	if err := pool.close(); err != nil {
		t.Fatal(err)
	}
}
