// Package compareclock collects wall times in diagnostic Compact binaries.
// Uninstrumented builds run the same workload in separate processes.
package compareclock

import (
	"slices"
	"sync/atomic"
	"time"
)

var epoch = time.Now()
var windowStart, windowEnd atomic.Int64
var ForegroundOwnsWriter atomic.Bool

type Timer struct {
	ns, calls atomic.Int64
}

var Replay, Emit, Copy, RecordCopy, Relocate, OtherWriterWait, ForegroundWait, ForegroundSync Timer

var RelocateWait, RelocateHold, RelocateIndexWait, RelocateIndexHold relocationTimer

// Only the Compact goroutine records these intervals. It reports its samples
// after the timed operation, including gates that it never needed to acquire.
type relocationTimer struct {
	ns, maxNS int64
	samples   []int64
}

func (h *relocationTimer) Add(start int64) {
	elapsed := Now() - start
	h.ns += elapsed
	h.maxNS = max(h.maxNS, elapsed)
	h.samples = append(h.samples, elapsed)
}

func Now() int64 { return time.Since(epoch).Nanoseconds() }

func BeginCompact() {
	windowEnd.Store(0)
	windowStart.Store(Now())
}

func EndCompact() { windowEnd.Store(Now()) }

// Add intersects the interval with the Compact call. A foreground sync may
// have begun before the call or may complete after it returns.
func (t *Timer) Add(start int64) {
	end := Now()
	started := windowStart.Load()
	if started == 0 {
		return
	}
	start = max(start, started)
	if stopped := windowEnd.Load(); stopped != 0 {
		end = min(end, stopped)
	}
	if end > start {
		t.ns.Add(end - start)
		t.calls.Add(1)
	}
}

func Report() map[string]int64 {
	return map[string]int64{
		"compact_ns":                        windowEnd.Load() - windowStart.Load(),
		"replay_including_emit_ns":          Replay.ns.Load(),
		"emit_ns":                           Emit.ns.Load(),
		"checkpoint_copy_inclusive_ns":      Copy.ns.Load(),
		"record_copy_ns":                    RecordCopy.ns.Load(),
		"record_copy_calls":                 RecordCopy.calls.Load(),
		"relocation_ns":                     Relocate.ns.Load(),
		"relocation_writer_wait_ns":         RelocateWait.ns,
		"relocation_writer_acquires":        int64(len(RelocateWait.samples)),
		"relocation_writer_hold_calls":      int64(len(RelocateHold.samples)),
		"relocation_writer_hold_ns":         RelocateHold.ns,
		"relocation_writer_max_hold_ns":     RelocateHold.maxNS,
		"relocation_index_gate_acquires":    int64(len(RelocateIndexWait.samples)),
		"relocation_index_gate_hold_calls":  int64(len(RelocateIndexHold.samples)),
		"relocation_index_gate_wait_ns":     RelocateIndexWait.ns,
		"relocation_index_gate_hold_ns":     RelocateIndexHold.ns,
		"relocation_index_gate_max_hold_ns": RelocateIndexHold.maxNS,
		"other_compact_writer_wait_ns":      OtherWriterWait.ns.Load(),
		"foreground_writer_wait_ns":         ForegroundWait.ns.Load(),
		"foreground_sync_ns":                ForegroundSync.ns.Load(),
		"foreground_sync_calls":             ForegroundSync.calls.Load(),
	}
}

func GateSamples() map[string][]int64 {
	samples := map[string][]int64{
		"relocation_writer_wait_ns":     RelocateWait.samples,
		"relocation_writer_hold_ns":     RelocateHold.samples,
		"relocation_index_gate_wait_ns": RelocateIndexWait.samples,
		"relocation_index_gate_hold_ns": RelocateIndexHold.samples,
	}
	for event, ns := range samples {
		if ns == nil {
			samples[event] = []int64{}
		}
		slices.Sort(ns)
	}
	return samples
}
