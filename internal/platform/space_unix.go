//go:build darwin || linux

package platform

import (
	"math"
	"syscall"
)

// AvailableBytes reports advisory free space available to an unprivileged process.
func AvailableBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	if st.Bavail <= 0 || st.Bsize <= 0 {
		return 0, nil
	}
	blocks, size := uint64(st.Bavail), uint64(st.Bsize)
	if blocks > math.MaxUint64/size {
		return math.MaxUint64, nil
	}
	return blocks * size, nil
}
