//go:build (linux && !amd64 && !arm64) || (darwin && !amd64 && !arm64)

package platform

func RenameNoReplace(old, new string) error { return ErrUnsupported }
