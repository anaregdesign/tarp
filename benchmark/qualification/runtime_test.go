package qualification

import (
	"fmt"
	"path"
	"strings"
	"testing"
)

func TestRuntimeFuzzRequiresGuidanceAndCompletedExecutionBudget(t *testing.T) {
	baseline := "fuzz: elapsed: 0s, gathering baseline coverage: 8/8 completed, now fuzzing with 2 workers\n"
	progress := baseline + "fuzz: elapsed: 3s, execs: 5000 (1666/sec), new interesting: 2 (total: 10)\n"
	complete := progress + "fuzz: elapsed: 6s, execs: 10000 (1666/sec), new interesting: 3 (total: 11)\nPASS\n"
	if got, err := RuntimeFuzzExecutions(complete); err != nil || got != 10000 {
		t.Fatalf("completed guided fuzz = %d, %v", got, err)
	}
	for _, invalid := range []string{
		baseline,
		progress,
		strings.ReplaceAll(complete, "gathering baseline coverage", "testing seed corpus"),
		complete + "warning: cannot fuzz without coverage guidance",
		complete + "warning: binary not built with coverage",
		strings.Replace(complete, "execs: 10000", "execs: 18446744073709551616", 1),
	} {
		if _, err := RuntimeFuzzExecutions(invalid); err == nil {
			t.Fatalf("accepted incomplete or unguided fuzz output: %s", invalid)
		}
	}
}

func TestRuntimeManifestRejectsMissingPackagesAndExecutableSubstitution(t *testing.T) {
	var rows []string
	for i, name := range []string{".", "benchmark", "benchmark/cmd/tarporacle", "benchmark/cmd/tarpqual",
		"benchmark/qualification", "internal/cache", "internal/composite", "internal/engine", "internal/entrysize", "internal/format",
		"internal/index", "internal/index/btree", "internal/index/memory", "internal/platform", "internal/storage"} {
		rows = append(rows, fmt.Sprintf("%s\truntime-%d.test\t%s\n", path.Join("github.com/anaregdesign/tarp", name), i, strings.Repeat("a", 64)))
	}
	valid := strings.Join(rows, "")
	if _, err := RuntimeManifest(strings.NewReader(valid)); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{
		strings.Join(rows[:len(rows)-1], ""),
		strings.Replace(valid, "runtime-0.test", "../other.test", 1),
		strings.Replace(valid, "runtime-0.test", "runtime-1.test", 1),
		strings.Replace(valid, strings.Repeat("a", 64), "not-a-hash", 1),
		strings.Replace(valid, "/internal/format", "/internal/different", 1),
		strings.Replace(valid, "tarp\truntime-0", "tarp/examples\truntime-0", 1),
		strings.Replace(valid, "/internal/composite", "/internal/different", 1),
		strings.Replace(valid, "/internal/entrysize", "/internal/different", 1),
		strings.Replace(valid, "/internal/index\t", "/internal/different\t", 1),
		strings.Replace(valid, "/internal/index/btree", "/internal/different", 1),
		strings.Replace(valid, "/internal/index/memory", "/internal/different", 1),
	} {
		if _, err := RuntimeManifest(strings.NewReader(invalid)); err == nil {
			t.Fatal("accepted an incomplete or substituted full-suite manifest")
		}
	}
}
