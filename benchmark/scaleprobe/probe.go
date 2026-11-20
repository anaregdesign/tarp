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
	RetiredDataBytes    atomic.Uint64
	RelocationConflicts atomic.Uint64
	ReplayPasses        atomic.Uint64
	ReplayBytes         atomic.Uint64
	// IdleSequence is the last frontier a cleaner found with neither a
	// candidate nor pending retirement. The harness runs one DB per arm.
	IdleSequence atomic.Uint64
	IdleAtNS     atomic.Int64
	ClockStart   = time.Now()
)

func ReadDir(path string) ([]os.DirEntry, error) {
	DirectoryReads.Add(1)
	return os.ReadDir(path)
}

// CleanerIdle records the first quiet observation of a frontier. The campaign
// uses one active cleaner, and samples its timestamp after seeing the sequence.
func CleanerIdle(sequence uint64) {
	if IdleSequence.Load() != sequence {
		IdleAtNS.Store(time.Since(ClockStart).Nanoseconds())
		IdleSequence.Store(sequence)
	}
}
