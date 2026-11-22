package engine

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

type parityWorker struct {
	wake   chan struct{}
	gate   chan struct{}
	cancel context.CancelFunc
	done   chan struct{}

	// layoutRevision advances under the writer gate when maintenance replaces a catalog prefix.
	layoutRevision uint64
	// The gate serializes automatic, explicit and Close builders. Appending a
	// sealed entry extends the cursor without rechecking the covered catalog entries.
	scannedLayoutRevision uint64
	next                  int
}

func (db *DB) startMaintenance() {
	ctx, cancel := context.WithCancel(context.Background())
	w := &db.parity
	w.wake, w.gate, w.done = make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{})
	w.cancel = cancel
	var start <-chan struct{}
	if db.hooks != nil {
		start = db.hooks.ParityStart
	}
	// Each worker can notify the other. Initialize both sets of channels
	// before either worker can send to an uninitialized receiver.
	db.startCleaner()
	go func() {
		defer close(w.done)
		if start != nil {
			select {
			case <-ctx.Done():
				return
			case <-start:
			}
		}
		db.runParity(ctx)
	}()
	db.notifyParity()
}

func (db *DB) notifyParity() {
	select {
	case db.parity.wake <- struct{}{}:
	default:
	}
}

// resetParity is called under writer after cleaning or full compaction.
func (db *DB) resetParity() {
	db.parity.layoutRevision++
	db.notifyParity()
}

func (db *DB) runParity(ctx context.Context) {
	var delay time.Duration
	for {
		if delay == 0 {
			select {
			case <-ctx.Done():
				return
			case <-db.parity.wake:
			}
		} else {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		err := db.protectSealed(ctx, nil)
		if ctx.Err() != nil || db.health() != nil {
			return
		}
		if err != nil {
			delay = min(max(100*time.Millisecond, 2*delay), 10*time.Second)
		} else {
			delay = 0
		}
	}
}

// ProtectParity seals the current committed tail and protects the captured
// DATA. New writes can proceed while this call verifies and builds sidecars.
// Physical replacements of the captured layout are recaptured.
func (db *DB) ProtectParity(ctx context.Context) error {
	const op = "protect parity"
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if ctx == nil {
		return validContextError(op)
	}
	if err := ctx.Err(); err != nil {
		return operationError(op, "context", err)
	}
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return operationError(op, "capture", err)
	}
	if db.store.Current.ActiveCommittedEndOffset != format.SegmentHeaderBytes {
		if err := db.sealActive(); err != nil {
			db.storageFailure(err)
			db.writer.Unlock()
			failed := operationError(op, "rotation", err)
			failed.Outcome = NotApplied
			return failed
		}
	}
	cut := db.parityCut()
	db.writer.Unlock()
	if err := db.protectSealed(ctx, &cut); err != nil {
		db.notifyParity()
		return operationError(op, "sealed DATA", err)
	}
	return nil
}

type parityCut struct {
	entries        []format.CatalogEntry
	layoutRevision uint64
	uuid           [16]byte
}

// The caller holds writer. Published catalog slices are immutable.
func (db *DB) parityCut() parityCut {
	return parityCut{db.store.Catalog.Entries, db.parity.layoutRevision, db.store.Catalog.UUID}
}

func (db *DB) protectSealed(ctx context.Context, captured *parityCut) error {
	w := &db.parity
	recheck := captured != nil
	var yield func() error
	if !recheck {
		yield = func() error {
			if db.cleaner.writers.Load() == 0 && !db.backup.Load() {
				return nil
			}
			timer := time.NewTimer(time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}
	}
	select {
	case w.gate <- struct{}{}:
		defer func() { <-w.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var cut parityCut
		if captured != nil {
			cut, captured = *captured, nil
		} else {
			db.writer.Lock()
			cut = db.parityCut()
			db.writer.Unlock()
		}
		entries, layoutRevision, uuid := cut.entries, cut.layoutRevision, cut.uuid
		if recheck || layoutRevision != w.scannedLayoutRevision {
			w.next = 0
		}
		w.scannedLayoutRevision = layoutRevision
		next := len(entries)
		var firstErr, unavailable error
		for i := w.next; i < len(entries); i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := db.health(); err != nil {
				return err
			}
			entry := entries[i]
			if !entry.HasPhysicalFile() {
				continue
			}
			db.writer.Lock()
			if w.layoutRevision != layoutRevision {
				db.writer.Unlock()
				break
			}
			f := db.store.Segments[entry.ID]
			f.Pin()
			db.writer.Unlock()

			err := db.store.EnsureParitySidecar(ctx, uuid, storage.CapturedSegment{Segment: f, Entry: entry}, recheck, yield)
			f.Unpin()
			if db.cleaner.pinned.Load() {
				db.notifyCleaner()
			}
			if err != nil {
				if storage.IsDataUnavailable(err) {
					// Keep protecting subsequent files. Explicit calls report
					// the gap; the worker advances past it until a recheck.
					if recheck && unavailable == nil {
						unavailable = err
					}
					continue
				}
				// A failed optional output cannot invalidate an acknowledged
				// write. Confirm DATA damage still belongs to the selected
				// layout before reporting it as a live engine failure.
				var source *storage.SourceError
				if errors.As(err, &source) {
					db.writer.Lock()
					if w.layoutRevision == layoutRevision || slices.Contains(db.store.Catalog.Entries, entry) {
						db.poison(err)
					}
					db.writer.Unlock()
				}
				if firstErr == nil {
					firstErr, next = err, i
				}
			}
		}
		db.writer.Lock()
		changed := w.layoutRevision != layoutRevision
		db.writer.Unlock()
		if changed {
			continue
		}
		w.next = next
		return errors.Join(ctx.Err(), unavailable, firstErr, db.health())
	}
}
