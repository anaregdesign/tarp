package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestVMCommandsRefuseNonHostedExecutionBeforeMutation(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	for _, mode := range []string{"-block-controls", "-campaign", "-runtime-suite"} {
		t.Run(mode, func(t *testing.T) {
			work := filepath.Join(t.TempDir(), "must-not-be-created")
			if err := run([]string{mode, work, "-confirm", "disposable-vm-only"}, io.Discard); err == nil {
				t.Fatal("accepted VM commands outside the guarded disposable runner")
			}
			if _, err := os.Lstat(work); !os.IsNotExist(err) {
				t.Fatalf("refused command mutated its work path: %v", err)
			}
		})
	}
}
