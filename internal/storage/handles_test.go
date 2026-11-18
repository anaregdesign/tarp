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
