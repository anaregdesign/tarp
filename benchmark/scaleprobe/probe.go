// Package scaleprobe counts events in the instrumented storage-scale test binary.
package scaleprobe

import (
	"os"
	"sync/atomic"
	"time"
)

// The campaign overlay enables these probes without changing the library build.
const Enabled = false

var (
	DirectoryReads      atomic.Uint64
	UnlinkedDataBytes   atomic.Uint64
	RelocationConflicts atomic.Uint64
	ReplayPasses        atomic.Uint64
	ReplayBytes         atomic.Uint64
	// IdleLastUserSequence records the last user sequence when the cleaner
	// found no candidate or pending retirement. Each arm has one DB.
	IdleLastUserSequence atomic.Uint64
	IdleAtNS             atomic.Int64
	ClockStart           = time.Now()
)

func ReadDir(path string) ([]os.DirEntry, error) {
	DirectoryReads.Add(1)
	return os.ReadDir(path)
}

// CleanerIdle records the first quiet observation of a frontier. The campaign
// samples its timestamp after observing that last user sequence.
func CleanerIdle(lastUserSequence uint64) {
	if IdleLastUserSequence.Load() != lastUserSequence {
		IdleAtNS.Store(time.Since(ClockStart).Nanoseconds())
		IdleLastUserSequence.Store(lastUserSequence)
	}
}
