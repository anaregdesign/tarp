//go:build linux && !(amd64 || arm64 || riscv64 || loong64 || ppc64 || ppc64le || mips64 || mips64le)

package benchmark

import "errors"

// The Linux syscall ABI for these architectures needs a separate wrapper.
// The driver can still report warm measurements and the explicit cold status.
func adviseContainerBatchDontNeed(fd uintptr) error {
	return errors.ErrUnsupported
}
