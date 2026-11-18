package benchmark

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type repackEvent struct {
	Point string `json:"point"`
	AtNS  int64  `json:"at_ns"`
}

type repackPutSpan struct {
	StartNS int64 `json:"start_ns"`
	EndNS   int64 `json:"end_ns"`
}

// All offsets use one monotonic clock. Reads/writes in both arms run the same
// hooks. Only this separate diagnostic records events; the primary rows do not.
type repackTrace struct {
	mu        sync.Mutex
	origin    time.Time
	StartNS   int64           `json:"start_ns"`
	RetiredNS int64           `json:"retired_ns,omitempty"`
	Events    []repackEvent   `json:"events"`
	Puts      []repackPutSpan `json:"puts"`
}

func (t *repackTrace) writerWait(acquired bool) {
	point := "writer.wait:before"
	if acquired {
		point = "writer.wait:after"
	}
	t.event(point)
}

func (t *repackTrace) record(point string) {
	stage, _, _ := strings.Cut(point, ":")
	switch stage {
	case "data.sync", "current.sync", "current.dirsync",
		"clean.output.sync", "clean.catalog.write", "clean.catalog.sync", "clean.catalog.dirsync",
		"clean.current.write", "clean.current.sync", "clean.current.rename", "clean.current.dirsync",
		"clean.remap", "clean.retire.unlink", "clean.retire.dirsync":
		t.event(point)
	}
}

func (t *repackTrace) event(point string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Events = append(t.Events, repackEvent{point, time.Since(t.origin).Nanoseconds()})
}

func (t *repackTrace) addCalls(start time.Time, writes []repackCall, retired time.Time) {
	t.StartNS = start.Sub(t.origin).Nanoseconds()
	if !retired.IsZero() {
		t.RetiredNS = retired.Sub(t.origin).Nanoseconds()
	}
	for _, put := range writes {
		t.Puts = append(t.Puts, repackPutSpan{put.begin.Sub(t.origin).Nanoseconds(), put.end.Sub(t.origin).Nanoseconds()})
	}
}

// TARP_REPACK_TRACE selects an opt-in five-pair stage trace for the 64-byte workload.
func TestRepackStageTrace(t *testing.T) {
	path := os.Getenv("TARP_REPACK_TRACE")
	if path == "" {
		t.Skip("set TARP_REPACK_TRACE to a new JSONL output file")
	}
	out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := out.Close(); err != nil {
			t.Error(err)
		}
	})
	enc := json.NewEncoder(out)
	fixture := prepareRepackFixture(t, 64)
	for rep := 1; rep <= 5; rep++ {
		for _, mode := range repackModesFor(rep) {
			trace := &repackTrace{origin: time.Now(), Events: make([]repackEvent, 0, 1500)}
			result := fixture.measureTraced(t, mode, repackPuts, trace)
			if err := enc.Encode(struct {
				Repetition  int               `json:"repetition"`
				Measurement repackMeasurement `json:"measurement"`
				Trace       *repackTrace      `json:"trace"`
			}{rep, result, trace}); err != nil {
				t.Fatal(err)
			}
		}
	}
}
