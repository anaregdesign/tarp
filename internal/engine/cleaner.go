// Automatic incremental compaction is coordinated per database.
package engine

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

// AutoCompactionStatus describes the automatic compaction worker since this Open.
type AutoCompactionStatus struct {
	DeferredReason         string
	PinnedRetiredDataBytes int64  // Retired DATA bytes held by leases at the last retirement pass.
	UnlinkedBytes          uint64 // Cumulative logical lengths of files unlinked by the worker, including obsolete manifests.
	LastError              error
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
	status  AutoCompactionStatus
}

// AutoCompactionStatus returns the latest observed worker status, including after Close.
func (db *DB) AutoCompactionStatus() AutoCompactionStatus {
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
	if db.hooks != nil {
		start = db.hooks.CleanerStart
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
func nextCandidate(s *storage.Store, target int64) (inputs []format.File, staging uint64) {
	metadata := uint64(len(s.Manifest)) + format.FrontierBytes
	var best format.File
	for i, entry := range s.Catalog.Files {
		if !entry.HasPhysicalFile() {
			continue
		}
		records := retainedBytes(s.Files[entry.ID])
		half := uint64(entry.Bytes) / 2
		if records > half {
			continue
		}
		output := replacementBytes(records)
		if output > half || metadata > half-output {
			if len(inputs) == 0 && entry.Kind != format.Snapshot && entry.Bytes <= target {
				// Save the oldest eligible group as a fallback. Any qualifying
				// single file, including later in the catalog, takes priority.
				inputs, staging = obsoleteGroupAt(s, s.Catalog.Files[i:], uint64(target), metadata)
			}
			continue
		}
		output += metadata
		if best.HasPhysicalFile() {
			a, b := bits.Mul64(output, uint64(best.Bytes))
			c, d := bits.Mul64(staging, uint64(entry.Bytes))
			if a > c || a == c && (b > d || b == d && (entry.First > best.First || entry.First == best.First && entry.ID >= best.ID)) {
				continue
			}
		}
		best, staging = entry, output
		inputs = s.Catalog.Files[i : i+1]
	}
	return inputs, staging
}

func retainedBytes(f *storage.File) uint64 {
	return uint64(f.LiveBytes.Load()) + uint64(f.DeleteBytes.Load())
}

func replacementBytes(records uint64) uint64 {
	if records == 0 {
		return 0
	}
	return format.SegmentHeaderBytes + records
}

// obsoleteGroupAt shares the cost of one publication across adjacent sealed
// logs. Bound both the catalog lookahead under the writer gate and the amount
// of DATA scanned off the gate: at most eight entries, each at most one target.
// Sealed live bytes only decrease as writers advance. Include every tombstone
// and keep the resulting DATA file within one target even at its estimate.
func obsoleteGroupAt(s *storage.Store, files []format.File, target, metadata uint64) (group []format.File, staging uint64) {
	const maxEntries = 8
	var source, retained uint64
	var count int
	for i, entry := range files[:min(len(files), maxEntries)] {
		if entry.Kind == format.Snapshot || uint64(entry.Bytes) > target {
			break
		}
		if !entry.HasPhysicalFile() {
			continue // Preserve intermediate metadata-only sequence coverage.
		}
		size := uint64(entry.Bytes)
		records := retainedBytes(s.Files[entry.ID])
		if records > size/2 || records > target-format.SegmentHeaderBytes-retained || size > math.MaxInt64-source {
			break
		}
		count++
		source += size
		retained += records
		output := replacementBytes(retained)
		if count >= 2 && output <= source/2 && metadata <= source/2-output {
			group, staging = files[:i+1], output+metadata
		}
	}
	return group, staging
}

// repackGroup picks the oldest eligible run from the immutable catalog. Try at
// most eight physical files per start. Empty log intervals contribute coverage
// without requiring a file read. Snapshots and logs have different replay rules.
func repackGroup(files []format.File, target int64) ([]format.File, int64) {
	const minFiles, maxFiles = 4, 8
	for start, first := range files {
		if !first.HasPhysicalFile() || first.Bytes > target/4 {
			continue
		}
		var group []format.File
		var total, largest, groupBytes int64
		count := 0
		for i := start; i < len(files) && count < maxFiles; i++ {
			entry := files[i]
			if (entry.Kind == format.Snapshot) != (first.Kind == format.Snapshot) {
				break
			}
			if !entry.HasPhysicalFile() {
				continue
			}
			if entry.Bytes > target/4 || entry.Bytes > target-total {
				break
			}
			count++
			total += entry.Bytes
			largest = max(largest, entry.Bytes)
			// Avoid copying a previously packed file for a few small additions.
			if count >= minFiles && largest <= total-largest {
				group, groupBytes = files[start:i+1], total
			}
		}
		if len(group) != 0 {
			return group, groupBytes
		}
	}
	return nil, 0
}

// Each pass yields the slot to manual maintenance before selecting more work.
func (db *DB) runCleaner(ctx context.Context) {
	delay := time.Duration(0)
	for {
		if delay > 0 {
			timer := time.NewTimer(delay)
			var released <-chan struct{}
			if db.AutoCompactionStatus().DeferredReason == "pinned" {
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
	var inputs []format.File
	var staging uint64
	if err == nil {
		inputs, staging = nextCandidate(db.store, db.opts.TargetSegmentBytes)
	}
	files, manifestBytes := db.store.Catalog.Files, len(db.store.Manifest)
	db.writer.Unlock()
	w.mu.Lock()
	w.status.PinnedRetiredDataBytes = pinned
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
	if len(inputs) == 0 {
		// Foreground rotations publish a new catalog slice. Inspect this one
		// outside the writer gate; BeginClean rechecks the selection.
		var inputBytes int64
		inputs, inputBytes = repackGroup(files, db.opts.TargetSegmentBytes)
		staging = uint64(inputBytes) + uint64(manifestBytes) + format.FrontierBytes
	}
	if len(inputs) == 0 {
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
		err = db.cleanSegment(ctx, inputs...)
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
