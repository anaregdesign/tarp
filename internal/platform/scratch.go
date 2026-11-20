package platform

import (
	"errors"
	"os"
)

// OpenScratch opens a process-private file in dir and unlinks it before returning.
// On the supported Unix filesystems its storage is released when the last
// descriptor closes, including after process termination.
func OpenScratch(dir, pattern string) (*os.File, error) {
	if dir == "" {
		return nil, &os.PathError{Op: "open scratch", Path: dir, Err: os.ErrInvalid}
	}
	f, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return nil, err
	}
	if err := os.Remove(f.Name()); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}
