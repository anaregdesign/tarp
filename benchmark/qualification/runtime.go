package qualification

import (
	"bufio"
	"encoding/hex"
	"errors"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
)

const RuntimeFuzzIterations uint64 = 10000

var runtimeFuzzCounts = regexp.MustCompile(`execs: ([0-9]+)`)

type RuntimePackage struct {
	Package   string `json:"package"`
	Binary    string `json:"binary"`
	SHA256    string `json:"sha256"`
	ElapsedNS int64  `json:"elapsed_ns"`
}

type RuntimeVerdict struct {
	Packages          []RuntimePackage `json:"packages"`
	FuzzTarget        string           `json:"fuzz_target"`
	FuzzIterations    uint64           `json:"fuzz_iterations"`
	FuzzElapsedNS     int64            `json:"fuzz_elapsed_ns"`
	FuzzCoverageGuide bool             `json:"fuzz_coverage_guided"`
	FuzzExecutions    uint64           `json:"fuzz_executions"`
}

func RuntimeFuzzExecutions(output string) (uint64, error) {
	if !strings.Contains(output, "gathering baseline coverage") || !strings.Contains(output, "now fuzzing with") ||
		strings.Contains(output, "without coverage guidance") || strings.Contains(output, "not built with coverage") {
		return 0, errors.New("qualification: runtime fuzz did not demonstrate coverage guidance")
	}
	counts := runtimeFuzzCounts.FindAllStringSubmatch(output, -1)
	if len(counts) == 0 {
		return 0, errors.New("qualification: runtime fuzz has no execution count")
	}
	executions, err := strconv.ParseUint(counts[len(counts)-1][1], 10, 64)
	if err != nil {
		return 0, err
	}
	if executions < RuntimeFuzzIterations {
		return 0, errors.New("qualification: runtime fuzz did not complete its execution budget")
	}
	return executions, nil
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
