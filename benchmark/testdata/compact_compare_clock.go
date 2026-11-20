// Package compareclock collects wall times in the diagnostic binaries for #102.
// The uninstrumented release builds run the same workload in separate processes.
package compareclock

import (
	"sync/atomic"
	"time"
)

var epoch = time.Now()
var windowStart, windowEnd atomic.Int64
var ForegroundOwnsWriter atomic.Bool

type Timer struct {
	ns, calls atomic.Int64
}

var Replay, Emit, Copy, RecordCopy, Relocate, RelocateWait, OtherWriterWait, ForegroundWait, ForegroundSync Timer

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

func Report() any {
	return map[string]int64{
		"compact_ns":                   windowEnd.Load() - windowStart.Load(),
		"replay_including_emit_ns":     Replay.ns.Load(),
		"emit_ns":                      Emit.ns.Load(),
		"checkpoint_copy_inclusive_ns": Copy.ns.Load(),
		"record_copy_ns":               RecordCopy.ns.Load(),
		"record_copy_calls":            RecordCopy.calls.Load(),
		"relocation_ns":                Relocate.ns.Load(),
		"relocation_writer_wait_ns":    RelocateWait.ns.Load(),
		"relocation_writer_acquires":   RelocateWait.calls.Load(),
		"other_compact_writer_wait_ns": OtherWriterWait.ns.Load(),
		"foreground_writer_wait_ns":    ForegroundWait.ns.Load(),
		"foreground_sync_ns":           ForegroundSync.ns.Load(),
		"foreground_sync_calls":        ForegroundSync.calls.Load(),
	}
}
