//go:build !darwin && !linux

package platform

import "os"

func FileLinkCount(os.FileInfo) (uint64, error) { return 0, ErrUnsupported }
