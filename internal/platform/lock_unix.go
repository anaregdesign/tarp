//go:build linux || darwin

// Package platform contains the standard-library-only OS-specific operations.
package platform

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

var (
	ErrLocked      = errors.New("platform: directory already owned")
	ErrUnsupported = errors.New("platform: unsupported OS or architecture")
	registry       struct {
		sync.Mutex
		locks map[*DirectoryLock]os.FileInfo
	}
)

type DirectoryLock struct {
	file *os.File
}

func Lock(dir string) (*DirectoryLock, error) {
	return lockDirectory(dir, os.O_CREATE|os.O_RDWR)
}

// LockExisting acquires the same exclusive ownership as Lock using a
// read-only descriptor. A missing LOCK is left missing.
func LockExisting(dir string) (*DirectoryLock, error) {
	return lockDirectory(dir, os.O_RDONLY)
}

func lockDirectory(dir string, flags int) (*DirectoryLock, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	registry.Lock()
	defer registry.Unlock()
	for _, held := range registry.locks {
		if os.SameFile(info, held) {
			return nil, ErrLocked
		}
	}
	name := filepath.Join(dir, "LOCK")
	if st, err := os.Lstat(name); err == nil && !st.Mode().IsRegular() {
		return nil, fmt.Errorf("platform: LOCK is not regular")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	f, err := os.OpenFile(name, flags|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	if st, err := f.Stat(); err != nil {
		return nil, errors.Join(err, f.Close())
	} else if !st.Mode().IsRegular() {
		return nil, errors.Join(fmt.Errorf("platform: LOCK is not regular"), f.Close())
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		closeErr := f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.Join(ErrLocked, err, closeErr)
		}
		return nil, errors.Join(err, closeErr)
	}
	l := &DirectoryLock{file: f}
	if registry.locks == nil {
		registry.locks = make(map[*DirectoryLock]os.FileInfo)
	}
	registry.locks[l] = info
	return l, nil
}

func (l *DirectoryLock) Sync() error { return l.file.Sync() }

func (l *DirectoryLock) Close() error {
	registry.Lock()
	defer registry.Unlock()
	err := errors.Join(syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN), l.file.Close())
	delete(registry.locks, l)
	return err
}
