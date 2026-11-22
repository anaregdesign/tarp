package qualification

import (
	"fmt"
	"path"
	"strings"
	"testing"
)

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
