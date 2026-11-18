//go:build !linux && !darwin

package platform

import "errors"

var (
	ErrLocked      = errors.New("platform: directory already owned")
	ErrUnsupported = errors.New("platform: unsupported OS or architecture")
)

type DirectoryLock struct{}

func Lock(string) (*DirectoryLock, error)  { return nil, ErrUnsupported }
func (*DirectoryLock) Sync() error         { return ErrUnsupported }
func (*DirectoryLock) Close() error        { return ErrUnsupported }
func RenameNoReplace(string, string) error { return ErrUnsupported }
