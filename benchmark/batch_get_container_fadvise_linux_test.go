//go:build linux && (amd64 || arm64 || riscv64 || loong64 || ppc64 || ppc64le || mips64 || mips64le)

package benchmark

import "syscall"

// These architectures use fadvise64(fd, offset, length, advice). A zero
// offset and length advise the whole file, including its final partial page.
func adviseContainerBatchDontNeed(fd uintptr) error {
	const posixFadvDontNeed = 4
	_, _, errno := syscall.Syscall6(syscall.SYS_FADVISE64, fd, 0, 0, posixFadvDontNeed, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}
