//go:build (linux || darwin) && (amd64 || arm64)

package platform

import (
	"os"
	"runtime"
	"syscall"
	"unsafe"
)

// RenameNoReplace atomically publishes a new directory without replacing one.
func RenameNoReplace(old, new string) error {
	a, err := syscall.BytePtrFromString(old)
	if err != nil {
		return err
	}
	b, err := syscall.BytePtrFromString(new)
	if err != nil {
		return err
	}
	at := atFDCWD
	_, _, errno := syscall.Syscall6(renameTrap, uintptr(at), uintptr(unsafe.Pointer(a)),
		uintptr(at), uintptr(unsafe.Pointer(b)), renameExclusive, 0)
	runtime.KeepAlive(a)
	runtime.KeepAlive(b)
	if errno != 0 {
		return &os.LinkError{Op: "rename-noreplace", Old: old, New: new, Err: errno}
	}
	return nil
}
