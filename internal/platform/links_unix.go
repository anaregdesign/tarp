//go:build darwin || linux

package platform

import (
	"errors"
	"os"
	"syscall"
)

// FileLinkCount returns the number of hard links to the file described by info.
func FileLinkCount(info os.FileInfo) (uint64, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Nlink == 0 {
		return 0, errors.New("platform: missing file link count")
	}
	return uint64(st.Nlink), nil
}
