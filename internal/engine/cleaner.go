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
	DroppedKeys            uint64 // Cumulative unrecoverable live versions discarded by the worker.
	PinnedRetiredDataBytes int64  // Retired DATA bytes retained by segment pins or live index references at the last retirement pass.
	UnlinkedBytes          uint64 // Cumulative logical lengths of files unlinked by the worker, including obsolete manifests.
	LastError              error
}

type cleaner struct {
	wake    chan struct{}
	resume  chan struct{} // foreground work released or manual checkpoint requested
	gate    chan struct{} // one maintenance slot for the worker, Compact and CheckpointIndex
	cancel  context.CancelFunc
	done    chan struct{}
	writers atomic.Int64
	pinned  atomic.Bool
	mu      sync.Mutex
	status  AutoCompactionStatus

	recoverPending      atomic.Bool
	cleanBeforeRecovery bool               // gate owner lets a healthy clean reclaim space before retrying
	recoverCancel       context.CancelFunc // mu guards preemption by manual Compact
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
	db.store.DamageChanged = func() {
		w.recoverPending.Store(true)
		db.notifyCleaner()
	}
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

var errManualCheckpointPriority = errors.New("tarp: manual checkpoint requested")

// Called between bounded I/O chunks, outside of DB locks.
func (db *DB) cleanYield(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := db.health(); err != nil {
			return err
		}
		if db.manualCheckpoint.Load() {
			return errManualCheckpointPriority
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

// selectCleaningInputs inspects committed per-file accounting under the writer gate.
// With source bytes S and estimated output bytes W, (S-W)/(S+W) >= 1/3
// simplifies to W <= S/2. Ranking this ratio is equivalent to minimizing W/S.
func selectCleaningInputs(s *storage.Store, target int64) (inputs []format.CatalogEntry, staging uint64, blocked map[uint64]*storage.UnavailableDataError) {
	metadata := uint64(len(s.Manifest)) + format.FrontierBytes
	var best format.CatalogEntry
	for i, entry := range s.Catalog.Entries {
		if !entry.HasPhysicalFile() {
			continue
		}
		f := s.Segments[entry.ID]
		if err := f.UnavailableData(); err != nil {
			if blocked == nil {
				blocked = make(map[uint64]*storage.UnavailableDataError)
			}
			blocked[entry.ID] = err
			continue
		}
		records := retainedRecordBytes(f)
		half := uint64(entry.Bytes) / 2
		if records > half {
			continue
		}
		output := replacementBytes(records)
		if output > half || metadata > half-output {
			if len(inputs) == 0 && entry.Kind != format.SnapshotKind && entry.Bytes <= target {
				// Save the oldest eligible group as a fallback. Any qualifying
				// single file, including later in the catalog, takes priority.
				inputs, staging = groupedLogCleaningAt(s, s.Catalog.Entries[i:], uint64(target), metadata)
			}
			continue
		}
		output += metadata
		if best.HasPhysicalFile() {
			a, b := bits.Mul64(output, uint64(best.Bytes))
			c, d := bits.Mul64(staging, uint64(entry.Bytes))
			if a > c || a == c && (b > d || b == d && (entry.FirstSequence > best.FirstSequence || entry.FirstSequence == best.FirstSequence && entry.ID >= best.ID)) {
				continue
			}
		}
		best, staging = entry, output
		inputs = s.Catalog.Entries[i : i+1]
	}
	return inputs, staging, blocked
}

func retainedRecordBytes(f *storage.DataSegment) uint64 {
	return uint64(f.LiveBytes.Load()) + uint64(f.DeleteBytes.Load())
}

func replacementBytes(records uint64) uint64 {
	if records == 0 {
		return 0
	}
	return format.SegmentHeaderBytes + records
}

// groupedLogCleaningAt shares one publication across consecutive log entries
// in the catalog. Bound both the catalog lookahead under the writer gate and the amount
// of DATA scanned off the gate: at most eight entries, each at most one target.
// Sealed live bytes only decrease as writers advance. Include every tombstone
// and keep the resulting DATA file within one target even at its estimate.
func groupedLogCleaningAt(s *storage.Store, entries []format.CatalogEntry, target, metadata uint64) (group []format.CatalogEntry, staging uint64) {
	const maxEntries = 8
	var source, retained uint64
	var count int
	for i, entry := range entries[:min(len(entries), maxEntries)] {
		if entry.Kind == format.SnapshotKind || uint64(entry.Bytes) > target {
			break
		}
		if !entry.HasPhysicalFile() {
			continue // Preserve intermediate metadata-only sequence coverage.
		}
		f := s.Segments[entry.ID]
		if f.UnavailableData() != nil {
			break
		}
		size := uint64(entry.Bytes)
		records := retainedRecordBytes(f)
		if records > size/2 || records > target-format.SegmentHeaderBytes-retained || size > math.MaxInt64-source {
			break
		}
		count++
		source += size
		retained += records
		output := replacementBytes(retained)
		if count >= 2 && output <= source/2 && metadata <= source/2-output {
			group, staging = entries[:i+1], output+metadata
		}
	}
	return group, staging
}

// smallSegmentRepackGroup finds 4–8 small DATA segments in consecutive
// catalog entries, even when all records are live. Empty log intervals add
// sequence coverage without a physical file. Each group contains only snapshot
// segments or only log segments.
func smallSegmentRepackGroup(entries []format.CatalogEntry, target int64, blocked map[uint64]*storage.UnavailableDataError) ([]format.CatalogEntry, int64) {
	const minFiles, maxFiles = 4, 8
	for start, first := range entries {
		if !first.HasPhysicalFile() || first.Bytes > target/4 {
			continue
		}
		var group []format.CatalogEntry
		var total, largest, groupBytes int64
		count := 0
		for i := start; i < len(entries) && count < maxFiles; i++ {
			entry := entries[i]
			if (entry.Kind == format.SnapshotKind) != (first.Kind == format.SnapshotKind) {
				break
			}
			if !entry.HasPhysicalFile() {
				continue
			}
			if blocked[entry.ID] != nil || entry.Bytes > target/4 || entry.Bytes > target-total {
				break
			}
			count++
			total += entry.Bytes
			largest = max(largest, entry.Bytes)
			// Avoid copying a previously packed file for a few small additions.
			if count >= minFiles && largest <= total-largest {
				group, groupBytes = entries[start:i+1], total
			}
		}
		if len(group) != 0 {
			return group, groupBytes
		}
	}
	return nil, 0
}

// The automatic recovery checkpoint owns the same slot as manual Compact.
// A manual request can cancel private work. Once CURRENT has been installed
// relocation and cleanup still finish before the slot changes hands.
func (db *DB) cancelAutoRecovery() {
	w := &db.cleaner
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.recoverCancel != nil {
		w.recoverCancel()
	}
}

func (db *DB) recoverDamaged(ctx context.Context) error {
	w := &db.cleaner
	ctx, cancel := context.WithCancel(ctx)
	w.mu.Lock()
	w.recoverCancel = cancel
	if db.manualCheckpoint.Load() || db.backup.Load() {
		cancel()
	}
	w.mu.Unlock()
	defer func() {
		cancel()
		w.mu.Lock()
		w.recoverCancel = nil
		w.mu.Unlock()
	}()
	result, err := db.compactCheckpoint(ctx, true)
	w.mu.Lock()
	w.status.DroppedKeys += min(result.DroppedKeys, math.MaxUint64-w.status.DroppedKeys)
	w.status.UnlinkedBytes += min(uint64(result.UnlinkedBytes), math.MaxUint64-w.status.UnlinkedBytes)
	w.status.PinnedRetiredDataBytes = result.PinnedRetiredDataBytes
	w.mu.Unlock()
	if result.PinnedRetiredDataBytes != 0 {
		w.pinned.Store(true)
	}
	return err
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
	if db.manualCheckpoint.Load() {
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
	var inputs []format.CatalogEntry
	var staging uint64
	var blocked map[uint64]*storage.UnavailableDataError
	if err == nil {
		inputs, staging, blocked = selectCleaningInputs(db.store, db.opts.TargetSegmentBytes)
	}
	entries, manifestBytes := db.store.Catalog.Entries, len(db.store.Manifest)
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
		inputs, inputBytes = smallSegmentRepackGroup(entries, db.opts.TargetSegmentBytes, blocked)
		staging = uint64(inputBytes) + uint64(manifestBytes) + format.FrontierBytes
	}
	if len(blocked) != 0 && w.recoverPending.Load() && (!w.cleanBeforeRecovery || len(inputs) == 0) {
		// Incremental cleaning retains tombstones. Full DATA checkpoint recovery
		// must validate every selected file, so a known missing sidecar or
		// staging failure would prevent publication for this cutoff.
		eligible := true
		for _, unavailable := range blocked {
			if !errors.Is(unavailable.Err, format.ErrUnrecoverableDataBlocks) {
				eligible = false
				break
			}
		}
		if eligible && w.recoverPending.Swap(false) {
			err := db.recoverDamaged(ctx)
			switch {
			case err == nil:
				db.deferCleaning("", nil)
				return true, false
			case errors.Is(err, context.Canceled):
				w.recoverPending.Store(true)
				return false, false
			case errors.Is(err, ErrNoSpace) && db.health() == nil:
				w.recoverPending.Store(true)
				w.cleanBeforeRecovery = true
				db.deferCleaning("space", err)
				return len(inputs) > 0, len(inputs) == 0
			default:
				// A missing/invalid proof does not improve on another pass.
				// Further damage-state transitions or manual Compact can retry.
				db.deferCleaning("recovery checkpoint", err)
				return len(inputs) > 0 || pending && removed > 0, pending && removed == 0
			}
		}
	}
	if len(inputs) == 0 {
		if pinned != 0 {
			db.deferCleaning("pinned", nil)
		} else if len(blocked) != 0 {
			for _, entry := range entries {
				if unavailable := blocked[entry.ID]; unavailable != nil {
					db.deferCleaning("unavailable DATA", publicCause(unavailable))
					break
				}
			}
		} else {
			db.deferCleaning("", nil)
		}
		return pending && removed > 0, pending && removed == 0
	}
	w.cleanBeforeRecovery = false
	err = db.store.IO.Check("clean.headroom:before")
	if err == nil {
		var available uint64
		available, err = platform.AvailableBytes(db.store.Dir)
		// Reserve space for the sidecar and allocation blocks alongside DATA,
		// the catalog and CURRENT. Staging already includes catalog metadata.
		outputBytes := staging - uint64(manifestBytes) - format.FrontierBytes
		if err == nil && available < staging+format.ParitySidecarBytes(outputBytes)+4*4096 {
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
	case storage.IsDataUnavailable(err):
		// The next pass can reclaim other inputs without retrying the damaged file.
		db.deferCleaning("unavailable DATA", err)
		return true, false
	case errors.Is(err, errManualCheckpointPriority), errors.Is(err, context.Canceled):
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
