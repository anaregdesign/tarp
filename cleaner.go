// Automatic incremental compaction is coordinated per database.
package tarp

import (
	"context"
	"errors"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/storage"
)

// MaintenanceStatus is a snapshot of automatic maintenance since this Open.
// UnlinkedBytes counts logical file lengths, including obsolete manifests.
type MaintenanceStatus struct {
	DeferredReason string
	PinnedBytes    int64
	UnlinkedBytes  uint64
	LastError      error
}

type cleaner struct {
	wake    chan struct{}
	resume  chan struct{} // foreground work released or manual checkpoint requested
	gate    chan struct{} // one shared maintenance slot for the worker and Compact
	cancel  context.CancelFunc
	done    chan struct{}
	writers atomic.Int64
	pinned  atomic.Bool
	mu      sync.Mutex
	status  MaintenanceStatus
}

// MaintenanceStatus returns the latest observed worker status, including after Close.
func (db *DB) MaintenanceStatus() MaintenanceStatus {
	w := &db.cleaner
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.status
}

func (db *DB) startCleaner() {
	ctx, cancel := context.WithCancel(context.Background())
	w := &db.cleaner
	w.wake = make(chan struct{}, 1)
	w.resume = make(chan struct{}, 1)
	w.gate = make(chan struct{}, 1)
	w.gate <- struct{}{}
	w.cancel = cancel
	w.done = make(chan struct{})
	var start <-chan struct{}
	if db.store.IO != nil {
		start = db.store.IO.CleanerStart
	}
	go func() {
		defer close(w.done)
		if start != nil {
			select {
			case <-start:
			case <-ctx.Done():
				return
			}
		}
		db.runCleaner(ctx)
	}()
	db.notifyCleaner()
}

func (db *DB) notifyCleaner() {
	select {
	case db.cleaner.wake <- struct{}{}:
	default:
	}
}

func (db *DB) resumeCleaner() {
	select {
	case db.cleaner.resume <- struct{}{}:
	default:
	}
}

func (db *DB) deferCleaning(reason string, err error) {
	w := &db.cleaner
	w.mu.Lock()
	w.status.DeferredReason, w.status.LastError = reason, err
	w.mu.Unlock()
}

var errManualPriority = errors.New("tarp: manual checkpoint requested")

// Called between bounded I/O chunks, outside of DB locks.
func (db *DB) cleanYield(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := db.health(); err != nil {
			return err
		}
		if db.compact.Load() {
			return errManualPriority
		}
		switch {
		case db.backup.Load():
			db.deferCleaning("backup", nil)
		case db.cleaner.writers.Load() > 0:
			db.deferCleaning("writer", nil)
		default:
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-db.cleaner.resume:
		}
	}
}

// nextCandidate inspects committed per-file accounting under the writer gate.
// With source bytes S and estimated output bytes W, (S-W)/(S+W) >= 1/3
// simplifies to W <= S/2. Ranking this ratio is equivalent to minimizing W/S.
func nextCandidate(s *storage.Store) (best format.File, staging uint64) {
	metadata := uint64(len(s.Manifest)) + format.FrontierBytes
	for _, entry := range s.Catalog.Files {
		if entry.ID == 0 {
			continue
		}
		f := s.Files[entry.ID]
		output := uint64(f.LiveBytes.Load()) + uint64(f.DeleteBytes.Load())
		if output > uint64(entry.Bytes) {
			continue
		}
		if output != 0 {
			output += format.SegmentHeaderBytes
		}
		// Reject before adding overhead so large signed file limits cannot overflow.
		half := uint64(entry.Bytes) / 2
		if output > half || metadata > half-output {
			continue
		}
		output += metadata
		if best.ID != 0 {
			a, b := bits.Mul64(output, uint64(best.Bytes))
			c, d := bits.Mul64(staging, uint64(entry.Bytes))
			if a > c || a == c && (b > d || b == d && (entry.First > best.First || entry.First == best.First && entry.ID >= best.ID)) {
				continue
			}
		}
		best, staging = entry, output
	}
	return best, staging
}

// Each pass yields the slot to manual maintenance before considering another file.
func (db *DB) runCleaner(ctx context.Context) {
	delay := time.Duration(0)
	for {
		if delay > 0 {
			timer := time.NewTimer(delay)
			var released <-chan struct{}
			if db.MaintenanceStatus().DeferredReason == "pinned" {
				// A reader or backup can release the last pin while the DB is idle.
				released = db.cleaner.wake
			}
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-released:
				timer.Stop()
			case <-timer.C:
			}
		} else {
			select {
			case <-ctx.Done():
				return
			case <-db.cleaner.wake:
			}
		}
		if ctx.Err() != nil || db.health() != nil {
			return
		}
		again, retry := db.cleanPass(ctx)
		switch {
		case retry:
			delay = min(max(100*time.Millisecond, 2*delay), 10*time.Second)
		case again:
			delay = 0
			db.notifyCleaner()
		default:
			delay = 0
		}
	}
}

func (db *DB) cleanPass(ctx context.Context) (again, retry bool) {
	w := &db.cleaner
	if db.compact.Load() {
		db.deferCleaning("manual checkpoint", nil)
		return false, false
	}
	select {
	case <-w.gate:
		defer func() { w.gate <- struct{}{} }()
	default:
		return false, false
	}
	if err := db.cleanYield(ctx); err != nil {
		return false, false
	}
	db.writer.Lock()
	if db.health() != nil {
		db.writer.Unlock()
		return false, false
	}
	removed, pinned, pending, err := db.store.RetireCleaned()
	var victim format.File
	var staging uint64
	if err == nil {
		victim, staging = nextCandidate(db.store)
	}
	db.writer.Unlock()
	w.mu.Lock()
	w.status.PinnedBytes = pinned
	if uint64(removed) > math.MaxUint64-w.status.UnlinkedBytes {
		w.status.UnlinkedBytes = math.MaxUint64
	} else {
		w.status.UnlinkedBytes += uint64(removed)
	}
	w.mu.Unlock()
	w.pinned.Store(pinned > 0)
	if err != nil {
		db.deferCleaning("retirement", compactCleanupError(err))
		return false, true
	}
	if victim.ID == 0 {
		if pinned != 0 {
			db.deferCleaning("pinned", nil)
		} else {
			db.deferCleaning("", nil)
		}
		return pending && removed > 0, pending && removed == 0
	}
	err = db.store.IO.Check("clean.headroom:before")
	if err == nil {
		var available uint64
		available, err = platform.AvailableBytes(db.store.Dir)
		// Allow additional filesystem allocation blocks for catalog and CURRENT.
		if err == nil && available < staging+3*4096 {
			err = ErrNoSpace
		}
	}
	if err == nil {
		err = db.cleanSegment(ctx, victim)
	}
	switch {
	case err == nil:
		db.deferCleaning("", nil)
		return true, false
	case errors.Is(err, errManualPriority), errors.Is(err, context.Canceled):
		return false, false
	case errors.Is(publicCause(err), ErrNoSpace) && db.health() == nil:
		db.deferCleaning("space", publicCause(err))
	case db.health() != nil:
		db.deferCleaning("unhealthy", err)
		return false, false
	default:
		db.deferCleaning("maintenance error", err)
	}
	return false, true
}
