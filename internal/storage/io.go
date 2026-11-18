// Package storage implements authoritative file I/O. Mutating Store methods
// require the owner's writer gate; Read and file references are concurrent.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// IO is a private testing seam. Hooks must themselves support concurrent calls.
// Point names end in :before/:after; an after fault may follow completed I/O.
type IO struct {
	// CleanerStart optionally gates the worker before its first pass in deterministic tests.
	CleanerStart <-chan struct{}
	Hook         func(string) error
	WriteAt      func(*os.File, []byte, int64) (int, error)
	// Wrote observes the bytes accepted by each write, including short writes.
	Wrote func(point string, bytes int)
	// PutCopied observes the key/value bytes copied for the index and cache.
	PutCopied func(keyBytes, valueBytes int)
}

func (i *IO) Check(point string) error {
	if i != nil && i.Hook != nil {
		return i.Hook(point)
	}
	return nil
}

func (i *IO) Do(point string, f func() error) error {
	if err := i.Check(point + ":before"); err != nil {
		return err
	}
	if err := f(); err != nil {
		return err
	}
	return i.Check(point + ":after")
}

func (i *IO) Write(f *os.File, b []byte, offset int64, point string) error {
	return i.Do(point, func() error {
		for len(b) != 0 {
			var n int
			var err error
			if i != nil && i.WriteAt != nil {
				n, err = i.WriteAt(f, b, offset)
			} else {
				n, err = f.WriteAt(b, offset)
			}
			if n < 0 || n > len(b) {
				return fmt.Errorf("storage: invalid write count %d", n)
			}
			if i != nil && i.Wrote != nil && n > 0 {
				i.Wrote(point, n)
			}
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			offset += int64(n)
			b = b[n:]
		}
		return nil
	})
}

func (i *IO) Sync(f *os.File, point string) error { return i.Do(point, f.Sync) }

func (i *IO) SyncDir(dir, point string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(i.Sync(f, point), f.Close())
}

func OpenRegular(name string) (*os.File, error) {
	st, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: not a regular file: %s", ErrCorrupt, filepath.Base(name))
	}
	return os.Open(name)
}

func ReadRegular(name string) ([]byte, error) {
	f, err := OpenRegular(name)
	if err != nil {
		return nil, err
	}
	b, readErr := io.ReadAll(f)
	return b, errors.Join(readErr, f.Close())
}

func (i *IO) NewFile(name string, b []byte, point string) error {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = i.Write(f, b, 0, point+".write")
	if err == nil {
		err = i.Sync(f, point+".sync")
	}
	return errors.Join(err, f.Close())
}

type Fault struct {
	Phase   string
	Outcome string
	Unsafe  bool
	Err     error
}

func (e *Fault) Error() string { return fmt.Sprintf("storage: %s (%s): %v", e.Phase, e.Outcome, e.Err) }
func (e *Fault) Unwrap() error { return e.Err }

func fault(phase, outcome string, unsafe bool, err error) error {
	return &Fault{Phase: phase, Outcome: outcome, Unsafe: unsafe, Err: err}
}
