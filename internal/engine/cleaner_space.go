package engine

import (
	"math"
	"time"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

const (
	cleanerReserveActivationBytes = 1 << 30
	cleanerSpaceSampleInterval    = 100 * time.Millisecond
	cleanerSpaceSampleBytes       = 8 << 20
)

// An advisory filesystem sample amortized over a bounded number of added DATA
// bytes. The writer gate protects both the sample and its debit.
type cleanerSpace struct {
	remaining uint64
	debit     uint64
	checked   time.Time
}

func spaceAdd(a, b uint64) uint64 { return a + min(b, math.MaxUint64-a) }

// One full cleaner output and its matching PARITY remain possible after a
// write. Tiny DBs can continue to use filesystems smaller than a segment target.
func (db *DB) cleanerWriteReserve() uint64 {
	if db.stats.SelectedDataBytes < cleanerReserveActivationBytes {
		return 0
	}
	segment := uint64(db.opts.TargetSegmentBytes)
	reserve := spaceAdd(segment, format.ParitySidecarBytes(segment))
	return spaceAdd(reserve, spaceAdd(db.store.CatalogEditBytes(), 4*4096))
}

func (db *DB) reportCleanerSpace(available, reserve uint64) {
	w := &db.cleaner
	w.mu.Lock()
	w.status.AvailableBytes = available
	w.status.WriteReserveBytes = reserve
	w.mu.Unlock()
	pressure := reserve != 0 && available < spaceAdd(reserve, reserve)
	if w.pressure.Swap(pressure) != pressure {
		db.resumeCleaner()
		db.notifyCleaner()
	}
}

func (db *DB) recordCleanerSpace(available uint64) {
	db.writer.Lock()
	defer db.writer.Unlock()
	reserve := db.cleanerWriteReserve()
	db.reportCleanerSpace(available, reserve)
}

// Called under the writer gate after input validation, before appending any
// committed bytes. An admitted operation still handles ENOSPC from the actual
// write, since another filesystem user can consume space after this check.
func (db *DB) checkWriteReserve(operation string, charge uint64) error {
	reserve := db.cleanerWriteReserve()
	if reserve == 0 {
		return nil
	}
	check := &db.cleaner.space
	now := time.Now()
	charge = spaceAdd(charge, 4096)
	if room := max(0, db.opts.TargetSegmentBytes-db.store.Current.ActiveCommittedEndOffset); charge > uint64(room) {
		charge = spaceAdd(charge, db.store.CatalogEditBytes())
	}
	expired := now.Sub(check.checked) >= cleanerSpaceSampleInterval
	if expired || check.debit != 0 && (spaceAdd(check.debit, charge) >= cleanerSpaceSampleBytes || check.remaining < spaceAdd(reserve, charge)) {
		available, err := platform.AvailableBytes(db.store.Dir)
		if err != nil {
			return operationError(operation, "space reserve", err)
		}
		*check = cleanerSpace{remaining: available, checked: now}
		db.reportCleanerSpace(available, reserve)
	}
	if check.remaining < spaceAdd(reserve, charge) {
		db.notifyCleaner()
		return operationError(operation, "space reserve", ErrNoSpace)
	}
	check.remaining -= charge
	check.debit = spaceAdd(check.debit, charge)
	return nil
}
