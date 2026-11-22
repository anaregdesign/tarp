package qualification

import (
	"bufio"
	"encoding/hex"
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
)

type RuntimePackage struct {
	Package   string `json:"package"`
	Binary    string `json:"binary"`
	SHA256    string `json:"sha256"`
	ElapsedNS int64  `json:"elapsed_ns"`
}

type RuntimeVerdict struct {
	Packages          []RuntimePackage `json:"packages"`
	FuzzTarget        string           `json:"fuzz_target"`
	FuzzSeconds       int              `json:"fuzz_seconds"`
	FuzzCoverageGuide bool             `json:"fuzz_coverage_guided"`
	FuzzExecutions    uint64           `json:"fuzz_executions"`
}

func RuntimeManifest(input io.Reader) ([]RuntimePackage, error) {
	const module = "github.com/anaregdesign/tarp"
	scanner := bufio.NewScanner(io.LimitReader(input, 65537))
	var packages []RuntimePackage
	seen := map[string]bool{}
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) != 3 || len(packages) == 32 {
			return nil, errors.New("qualification: invalid bounded runtime manifest")
		}
		sum, err := hex.DecodeString(fields[2])
		if err != nil || len(sum) != 32 || seen[fields[0]] ||
			fields[1] != "runtime-"+strconv.Itoa(len(packages))+".test" ||
			fields[0] != module && !strings.HasPrefix(fields[0], module+"/") {
			return nil, errors.New("qualification: invalid runtime package/hash/binary")
		}
		seen[fields[0]] = true
		packages = append(packages, RuntimePackage{Package: fields[0], Binary: fields[1], SHA256: fields[2]})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	for _, suite := range []string{
		".", "benchmark", "benchmark/cmd/tarporacle", "benchmark/cmd/tarpqual", "benchmark/qualification",
		"internal/cache", "internal/composite", "internal/engine", "internal/entrysize", "internal/format",
		"internal/index", "internal/index/btree", "internal/index/memory", "internal/platform", "internal/storage",
	} {
		pkg := path.Join(module, suite)
		if !seen[pkg] {
			return nil, errors.New("qualification: runtime manifest missing " + pkg)
		}
	}
	return packages, nil
}
