// Automatic incremental compaction is coordinated per database.
package engine

import (
	"cmp"
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
	DeferredReason          string
	PinnedRetiredDataBytes  int64  // Retired DATA bytes observed with pins or live index references during the last completed retirement sweep.
	UnlinkedBytes           uint64 // Cumulative logical lengths of files unlinked by the worker, including obsolete manifests.
	PendingReclaimDataBytes uint64 // Estimated DATA savings from obsolete-heavy segments before metadata, PARITY and pin checks.
	PinnedReclaimDataBytes  uint64 // Estimated DATA savings deferred by current retention pins.
	AvailableBytes          uint64 // Filesystem headroom at the last successful sample.
	WriteReserveBytes       uint64 // Headroom reserved for cleaner work on a large DB.
	LastError               error
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

	candidates cleanerCandidates // writer gate
	runUntil   time.Time         // cleaner maintenance slot
	space      cleanerSpace      // writer gate
	pressure   atomic.Bool
	pinChange  atomic.Uint64
	damageMu   sync.Mutex
	damage     map[uint64]*storage.DataSegment
	blocked    map[uint64]*storage.UnavailableDataError // cleaner maintenance slot
	editBytes  uint64                                   // cleaner maintenance slot

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
	db.store.DamageChanged = func(segment *storage.DataSegment) {
		w.damageMu.Lock()
		if w.damage == nil {
			w.damage = make(map[uint64]*storage.DataSegment)
		}
		w.damage[segment.ID] = segment
		w.damageMu.Unlock()
		db.notifyCleaner()
	}
	db.resetCleanerInventory()
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

func (db *DB) cleanerPinsReleased() {
	db.cleaner.pinChange.Add(1)
	db.notifyCleaner()
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
	w := &db.cleaner
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
		backup := db.backup.Load()
		if backup && !w.pressure.Load() {
			w.runUntil = time.Time{}
			db.deferCleaning("backup", nil)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-w.resume:
			}
			continue
		}
		if !backup && w.writers.Load() == 0 {
			w.runUntil = time.Time{}
			return nil
		}
		if time.Now().Before(w.runUntil) {
			return nil
		}
		reason := "writer"
		if backup {
			reason = "backup"
		}
		db.deferCleaning(reason, nil)
		// Pace by time window, since one chunk may contain a single small
		// record. Sustained traffic still leaves the cleaner some work time.
		timer := time.NewTimer(8 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-w.resume:
			timer.Stop()
		case <-timer.C:
			w.runUntil = time.Now().Add(2 * time.Millisecond)
		}
	}
}

// With source bytes S and estimated output bytes W, (S-W)/(S+W) >= 1/3
// simplifies to W <= S/2. Rank the eligible candidates by estimated immediately
// reclaimable bytes per byte of work, including the replacement's protection.
type cleanRank struct {
	entry             format.CatalogEntry
	stage, gain, work uint64
}

func rankCleanCandidate(s *storage.Store, entry format.CatalogEntry, metadata uint64) (cleanRank, bool) {
	f := s.Segments[entry.ID]
	if f == nil || f.UnavailableData() != nil {
		return cleanRank{}, false
	}
	bytes := uint64(entry.Bytes)
	half := bytes / 2
	records := retainedRecordBytes(f)
	if records > half {
		return cleanRank{}, false
	}
	output := replacementBytes(records)
	if output > half || metadata > half-output {
		return cleanRank{}, false
	}
	stage := output + metadata
	protectedStage := stage + format.ParitySidecarBytes(output)
	// Read the source, write the output and metadata, and then read the output
	// to build PARITY. Retiring a sidecar does not read its bytes.
	work := bytes + output
	work += min(protectedStage, math.MaxUint64-work)
	var gain uint64
	if !s.HasDataRetentionPin(f) {
		unlinkable := bytes
		if f.ParityReady() {
			unlinkable += format.ParitySidecarBytes(bytes)
		}
		gain = unlinkable - min(unlinkable, protectedStage)
	}
	return cleanRank{entry: entry, stage: stage, gain: gain, work: work}, true
}

func betterCleanRank(a, b cleanRank) bool {
	if !b.entry.HasPhysicalFile() {
		return true
	}
	order := compareProducts(a.gain, b.work, b.gain, a.work)
	if order == 0 {
		// Prefer a smaller rewrite when both sources retain long-lived pins.
		order = compareProducts(b.stage, uint64(a.entry.Bytes), a.stage, uint64(b.entry.Bytes))
	}
	if order != 0 {
		return order > 0
	}
	if a.entry.FirstSequence != b.entry.FirstSequence {
		return a.entry.FirstSequence < b.entry.FirstSequence
	}
	return a.entry.ID < b.entry.ID
}

func compareProducts(a, b, c, d uint64) int {
	ah, al := bits.Mul64(a, b)
	bh, bl := bits.Mul64(c, d)
	if ah != bh {
		return cmp.Compare(ah, bh)
	}
	return cmp.Compare(al, bl)
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

// smallSegmentRepackAt finds 4–8 small DATA segments in consecutive
// catalog entries, even when all records are live. Empty log intervals add
// sequence coverage without a physical file. Each group contains only snapshot
// segments or only log segments.
func smallSegmentRepackAt(entries []format.CatalogEntry, target int64, blocked map[uint64]*storage.UnavailableDataError) ([]format.CatalogEntry, int64) {
	const minFiles, maxFiles = 4, 8
	first := entries[0]
	if !first.HasPhysicalFile() || first.Bytes > target/4 {
		return nil, 0
	}
	var group []format.CatalogEntry
	var total, largest, groupBytes int64
	count := 0
	for i, entry := range entries {
		if count == maxFiles || (entry.Kind == format.SnapshotKind) != (first.Kind == format.SnapshotKind) {
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
			group, groupBytes = entries[:i+1], total
		}
	}
	return group, groupBytes
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
	result, err := db.compactCheckpoint(ctx, sourceRecovery)
	w.mu.Lock()
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
	if ctx.Err() != nil || db.health() != nil {
		db.writer.Unlock()
		return false, false
	}
	removed, pinned, pending, err := db.store.RetireCleaned()
	retiring := db.store.RetirementSweepPending()
	if removed != 0 {
		w.space.checked = time.Time{}
	}
	metadata := db.store.CatalogEditBytes()
	if metadata < w.editBytes && w.candidates.ready {
		// A base rollover can make previously rejected candidates affordable.
		db.resetCleanerInventory()
	}
	w.editBytes = metadata
	inventoryReady := db.seedCleanerInventory()
	q := &w.candidates
	pinGeneration, pressure := w.pinChange.Load(), w.pressure.Load()
	q.resumePins(pinGeneration, pressure)
	db.observeCleanerDamage()
	var best cleanRank
	var files []*cleanerCandidate
	if err == nil && inventoryReady {
		files = q.take()
		for i, file := range files {
			segment := db.store.Segments[file.entry.ID]
			if segment == nil {
				q.remove(file.entry.ID)
				files[i] = nil
				continue
			}
			if unavailable := segment.UnavailableData(); unavailable != nil {
				w.rememberDamage(segment.ID, unavailable)
				files[i] = nil
				continue
			}
			if pressure && db.store.HasDataRetentionPin(segment) {
				q.deferPin(file, pinGeneration)
				files[i] = nil
				continue
			}
			if rank, ok := rankCleanCandidate(db.store, file.entry, metadata); ok && betterCleanRank(rank, best) {
				best = rank
			}
		}
	}
	blocked := w.blocked
	awaitPins := q.pinned.Len() != 0
	w.pinned.Store(pinned > 0 || awaitPins)
	more := retiring || !inventoryReady || q.pending.Len() != 0 || q.pinsReady(w.pinChange.Load(), pressure)
	debt, pinnedDebt := q.debt, q.pinnedDebt
	db.writer.Unlock()
	w.mu.Lock()
	w.status.PinnedRetiredDataBytes = pinned
	w.status.PendingReclaimDataBytes = debt
	w.status.PinnedReclaimDataBytes = pinnedDebt
	w.status.UnlinkedBytes += min(uint64(removed), math.MaxUint64-w.status.UnlinkedBytes)
	w.mu.Unlock()
	if err != nil {
		db.deferCleaning("retirement", compactCleanupError(err))
		return false, true
	}
	if !inventoryReady {
		return true, false
	}
	var inputs []format.CatalogEntry
	var staging uint64
	if best.entry.HasPhysicalFile() {
		inputs, staging = []format.CatalogEntry{best.entry}, best.stage
	} else if len(files) != 0 {
		windows := db.candidateWindows(files)
		db.writer.Lock()
		inputs, staging = chooseCandidateGroups(db.store, windows, db.opts.TargetSegmentBytes, metadata, blocked, pressure)
		db.writer.Unlock()
	}
	if len(inputs) != 0 {
		// Retain the candidates that lost this pass. Installing a successful
		// publication removes its input IDs from the queue.
		db.writer.Lock()
		for _, file := range files {
			q.enqueue(file)
		}
		db.writer.Unlock()
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
				return more || len(inputs) > 0, !more && len(inputs) == 0
			default:
				// A missing/invalid proof does not improve on another pass.
				// Further damage-state transitions or manual Compact can retry.
				db.deferCleaning("recovery checkpoint", err)
				return more || len(inputs) > 0 || pending && removed > 0, !more && pending && removed == 0
			}
		}
	}
	if len(inputs) == 0 {
		if pinned != 0 || awaitPins {
			db.deferCleaning("pinned", nil)
		} else if len(blocked) != 0 {
			var first uint64 = math.MaxUint64
			for id := range blocked {
				first = min(first, id)
			}
			db.deferCleaning("unavailable DATA", publicCause(blocked[first]))
		} else {
			db.deferCleaning("", nil)
		}
		return more || pending && removed > 0, !more && pending && removed == 0
	}
	w.cleanBeforeRecovery = false
	err = db.store.IO.Check("clean.headroom:before")
	if err == nil {
		var available uint64
		available, err = platform.AvailableBytes(db.store.Dir)
		// Reserve space for the sidecar and allocation blocks alongside DATA,
		// the catalog and CURRENT. Staging already includes catalog metadata.
		outputBytes := staging - metadata
		if err == nil {
			db.recordCleanerSpace(available)
			if available < staging+format.ParitySidecarBytes(outputBytes)+4*4096 {
				err = ErrNoSpace
			}
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
