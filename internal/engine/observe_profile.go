//go:build tarpprofile

package engine

import "slices"

// LockStats contains all aggregate observations and the retained raw prefix.
type LockStats struct {
	Count, TotalNS uint64
	MaximumNS      int64
	Samples        []int64
}

// ProfileLocks controls recording in the diagnostic build.
func ProfileLocks(enabled bool) { profileLocks.Store(enabled) }

func (o Observer) ResetLockProfile() {
	for _, m := range []*engineMutex{&o.db.writer, &o.db.state, &o.db.life} {
		m.metrics.Lock()
		m.samples, m.count, m.sum, m.maximum = nil, 0, 0, 0
		m.metrics.Unlock()
	}
}

func (o Observer) LockProfile() map[string]LockStats {
	stats := make(map[string]LockStats, 3)
	for name, m := range map[string]*engineMutex{"writer": &o.db.writer, "state": &o.db.state, "lifecycle": &o.db.life} {
		m.metrics.Lock()
		stats[name] = LockStats{
			Count: m.count, TotalNS: m.sum, MaximumNS: m.maximum, Samples: slices.Clone(m.samples),
		}
		m.metrics.Unlock()
	}
	return stats
}

// HoldWriter lets a diagnostic probe queue foreground writes after their input
// buffers have been copied. The caller releases the gate using the result.
func (o Observer) HoldWriter() func() {
	o.db.writer.Lock()
	return o.db.writer.Unlock
}

func (o Observer) WaitingWriters() int64 { return o.db.cleaner.writers.Load() }
