package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/anaregdesign/tarp/benchmark/scaleprobe"
)

func storageScaleRound(t *testing.T) int {
	t.Helper()
	s := os.Getenv("TARP_STORAGE_SCALE_ROUND")
	if s == "" {
		t.Skip("opt-in storage scale campaign")
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || !scaleprobe.Enabled {
		t.Fatal("run the instrumented campaign using benchmark/scripts/container-storage-scale.sh")
	}
	return n
}

func storageScaleRow(t *testing.T, round int, scenario, arm string, metrics map[string]any) {
	t.Helper()
	row := struct {
		Round    int            `json:"round"`
		Scenario string         `json:"scenario"`
		Arm      string         `json:"arm"`
		Metrics  map[string]any `json:"metrics"`
	}{round, scenario, arm, metrics}
	b, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("TARP_SCALE_RESULT", string(b))
}
