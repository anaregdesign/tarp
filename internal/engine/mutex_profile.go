//go:build tarpprofile

package engine

import (
	"sync"
	"sync/atomic"
	"time"
)

var profileLocks atomic.Bool

// This diagnostic-only build records the acquired-lock-to-Unlock-entry span.
// Recording itself extends the actual hold and is not included in that span.
type engineMutex struct {
	raw     sync.Mutex
	waiters atomic.Int64
	start   time.Time
	metrics sync.Mutex
	samples []int64
	count   uint64
	sum     uint64
	maximum int64
}

func (m *engineMutex) Lock() {
	m.waiters.Add(1)
	m.raw.Lock()
	m.waiters.Add(-1)
	m.start = time.Time{}
	if profileLocks.Load() {
		m.start = time.Now()
	}
}

func (m *engineMutex) Unlock() {
	if !m.start.IsZero() {
		elapsed := time.Since(m.start).Nanoseconds()
		m.metrics.Lock()
		m.count++
		m.sum += uint64(elapsed)
		m.maximum = max(m.maximum, elapsed)
		if len(m.samples) < 65536 {
			m.samples = append(m.samples, elapsed)
		}
		m.metrics.Unlock()
	}
	m.raw.Unlock()
}
