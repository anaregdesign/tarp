// Package storage implements authoritative file I/O. Mutating Store methods
// require the owner's writer gate; Read and file references are concurrent.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/publication"
)

// IO is a private testing seam. Hooks must themselves support concurrent calls.
// Point names end in :before/:after; an after fault may follow completed I/O.
type IO struct {
	Hook    func(string) error
	WriteAt func(*os.File, []byte, int64) (int, error)
	// Wrote observes the bytes accepted by each write, including short writes.
	Wrote func(point string, bytes int)
	// OpenRead observes bytes returned by reads of selected DATA and INDEX
	// during Open. DATA distinguishes covered, tail and replay headers/payloads.
	OpenRead func(kind string, n int)
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

// A missing or truncated required file is evidence of corruption. Other I/O
// errors leave the contents unknown and retain their original cause.
func requiredFileError(name string, err error) error {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%s: %w: missing required file: %w", name, ErrCorrupt, err)
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return fmt.Errorf("%s: %w: truncated: %w", name, ErrCorrupt, err)
	default:
		return fmt.Errorf("%s: %w", name, err)
	}
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
	Outcome publication.Outcome
	Unsafe  bool
	Err     error
}

func (e *Fault) Error() string { return fmt.Sprintf("storage: %s (%s): %v", e.Phase, e.Outcome, e.Err) }
func (e *Fault) Unwrap() error { return e.Err }

func fault(phase string, outcome publication.Outcome, unsafe bool, err error) error {
	return &Fault{Phase: phase, Outcome: outcome, Unsafe: unsafe, Err: err}
}

// onlyCauses matches a wrapped or joined failure only if every leaf belongs to
// one of the expected categories. A separate I/O or cancellation error survives.
func onlyCauses(err, first error, equivalent ...error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		for _, cause := range causes {
			if !onlyCauses(cause, first, equivalent...) {
				return false
			}
		}
		return len(causes) > 0
	}
	if cause := errors.Unwrap(err); cause != nil {
		return onlyCauses(cause, first, equivalent...)
	}
	if errors.Is(err, first) {
		return true
	}
	for _, target := range equivalent {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
