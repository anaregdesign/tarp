//go:build (linux && !amd64 && !arm64) || (darwin && !amd64 && !arm64)

package platform

import (
	"errors"
	"testing"
)

func TestRenameNoReplaceUnsupportedArchitecture(t *testing.T) {
	if err := RenameNoReplace("stage", "target"); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("RenameNoReplace returned %v, want ErrUnsupported", err)
	}
}
