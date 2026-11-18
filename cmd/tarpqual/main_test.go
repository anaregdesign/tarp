package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestBlockControlsRefuseNonHostedExecutionBeforeMutation(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	work := filepath.Join(t.TempDir(), "must-not-be-created")
	if err := run([]string{"-block-controls", work, "-confirm", "disposable-vm-only"}, io.Discard); err == nil {
		t.Fatal("accepted block controls outside the guarded disposable runner")
	}
	if _, err := os.Lstat(work); !os.IsNotExist(err) {
		t.Fatalf("refused controls mutated their work path: %v", err)
	}
}
