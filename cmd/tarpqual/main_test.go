package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestVMCommandsRefuseNonHostedExecutionBeforeMutation(t *testing.T) {
	t.Setenv("GITHUB_ACTIONS", "")
	for _, mode := range []string{"-block-controls", "-campaign", "-runtime-suite", "-measurements"} {
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

func TestMeasurementAckPublishesOnceWithoutOverwriting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "device-000-begin")
	if err := measurementAck(dir, "device-000-begin"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("caller-owned"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := measurementAck(dir, "device-000-begin"); err == nil {
		t.Fatal("reused an external measurement handshake slot")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "caller-owned" {
		t.Fatalf("reused handshake mutated its prior contents: %q %v", data, err)
	}
}
