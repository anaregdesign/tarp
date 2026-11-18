package qualification

import (
	"fmt"
	"strings"
	"testing"
)

func TestRuntimeManifestRejectsMissingPackagesAndExecutableSubstitution(t *testing.T) {
	var rows []string
	for i, name := range []string{"benchmark", "benchmark/cmd/tarporacle", "benchmark/cmd/tarpqual",
		"benchmark/qualification", "examples", "internal/cache", "internal/engine", "internal/format",
		"internal/platform", "internal/storage"} {
		rows = append(rows, fmt.Sprintf("github.com/anaregdesign/tarp/%s\truntime-%d.test\t%s\n", name, i, strings.Repeat("a", 64)))
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
	} {
		if _, err := RuntimeManifest(strings.NewReader(invalid)); err == nil {
			t.Fatal("accepted an incomplete or substituted full-suite manifest")
		}
	}
}
