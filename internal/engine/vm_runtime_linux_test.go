package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp/benchmark/guesttest"
	"github.com/anaregdesign/tarp/benchmark/qualification"
)

type vmRuntimeOutput struct{ bytes.Buffer }

func (w *vmRuntimeOutput) Write(data []byte) (int, error) {
	if len(data) > (2<<20)-w.Len() {
		return 0, errors.New("runtime child output exceeded its 2 MiB bound")
	}
	return w.Buffer.Write(data)
}

func TestVMRuntimeGuest(t *testing.T) {
	if os.Getenv("TARP_VM_RUNTIME_CONFIRM") != "disposable-vm-only" {
		t.Skip("requires explicit disposable guest runtime-suite opt-in")
	}
	guesttest.MountTarget(t, true, "full-race-and-fuzz", 0x5eed)
	manifest, err := os.Open("/mnt/tarp-input/runtime.tsv")
	if err != nil {
		t.Fatal(err)
	}
	packages, readErr := qualification.RuntimeManifest(manifest)
	if err := errors.Join(readErr, manifest.Close()); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/mnt/tarp-data/tmp", "/mnt/tarp-data/fuzzcache"} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	var environment []string
	for _, variable := range os.Environ() {
		if !strings.HasPrefix(variable, "TARP_") && !strings.HasPrefix(variable, "TMPDIR=") &&
			!strings.HasPrefix(variable, "GOMAXPROCS=") {
			environment = append(environment, variable)
		}
	}
	environment = append(environment, "TMPDIR=/mnt/tarp-data/tmp", "GOMAXPROCS=2")
	run := func(binary string, args ...string) (time.Duration, string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, args...)
		var captured vmRuntimeOutput
		stream := io.MultiWriter(os.Stdout, &captured)
		cmd.Env, cmd.Stdout, cmd.Stderr, cmd.Dir = environment, stream, stream, "/mnt/tarp-data"
		start := time.Now()
		if err := cmd.Run(); err != nil {
			t.Fatalf("selected-target child %s %v: %v", binary, args, err)
		}
		return time.Since(start), captured.String()
	}
	var formatBinary string
	for i := range packages {
		entry := &packages[i]
		binary := filepath.Join("/mnt/tarp-input", entry.Binary)
		fd, err := os.Open(binary)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.New()
		_, readErr := io.Copy(digest, fd)
		if err := errors.Join(readErr, fd.Close()); err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(digest.Sum(nil)) != entry.SHA256 {
			t.Fatal("runtime binary does not match the external build manifest")
		}
		fmt.Printf("selected target full race package: %s sha256=%s\n", entry.Package, entry.SHA256)
		elapsed, _ := run(binary, "-test.v", "-test.count=1", "-test.timeout=150s")
		entry.ElapsedNS = elapsed.Nanoseconds()
		if entry.Package == "github.com/anaregdesign/tarp/internal/format" {
			formatBinary = binary
		}
	}
	_, fuzzOutput := run(formatBinary, "-test.run=^$", "-test.fuzz=^FuzzDecoders$", "-test.fuzztime=15s",
		"-test.parallel=2", "-test.fuzzcachedir=/mnt/tarp-data/fuzzcache", "-test.timeout=150s")
	counts := regexp.MustCompile(`execs: ([1-9][0-9]*)`).FindAllStringSubmatch(fuzzOutput, -1)
	if !strings.Contains(fuzzOutput, "gathering baseline coverage") || !strings.Contains(fuzzOutput, "now fuzzing with") ||
		strings.Contains(fuzzOutput, "without coverage guidance") || strings.Contains(fuzzOutput, "not built with coverage") || len(counts) == 0 {
		t.Fatal("fuzz campaign did not demonstrate baseline completion and coverage-guided executions")
	}
	executions, err := strconv.ParseUint(counts[len(counts)-1][1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(qualification.RuntimeVerdict{
		Packages: packages, FuzzTarget: "FuzzDecoders", FuzzSeconds: 15,
		FuzzCoverageGuide: true, FuzzExecutions: executions,
	})
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("TARP_RUNTIME_PASS %s\n", data)
}
