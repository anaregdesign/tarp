package engine

import (
	"context"
	"errors"
	"iter"
	"os"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Entry is a snapshot key and value. Both byte slices belong to the caller.
type Entry struct {
	Key   []byte
	Value []byte
}

// Scan captures a new point-in-time view each time iteration begins.
func (db *DB) Scan(ctx context.Context) iter.Seq2[Entry, error] {
	return db.ScanPrefix(ctx, nil)
}

// ScanPrefix reads matching records from the same index and DATA snapshot.
func (db *DB) ScanPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[Entry, error] {
	return db.scanEntries(ctx, nil, index.PrefixScan(encodedPrefix))
}

// ScanRange reads records in [lower, upper) in strictly ascending or descending
// bytewise lexicographic key order from a single index and DATA snapshot.
func (db *DB) ScanRange(ctx context.Context, lower, upper []byte, reverse bool) iter.Seq2[Entry, error] {
	return db.scanEntries(ctx, nil, index.OrderedScan(lower, upper, reverse))
}

func (db *DB) scanEntries(ctx context.Context, snapshot *Snapshot, query index.Scan) iter.Seq2[Entry, error] {
	return func(yield func(Entry, error) bool) {
		err := db.walkSnapshot(ctx, snapshot, query, "scan", func(k, value []byte) bool {
			return yield(Entry{Key: k, Value: value}, nil)
		})
		if err != nil {
			yield(Entry{}, err)
		}
	}
}

// The index and the DATA segment pins must describe the same point in time.
func (db *DB) captureScan(ctx context.Context, snapshot *Snapshot, query index.Scan, op string) (index.ScanView, *scanSegmentSet, error) {
	if err := db.admit(); err != nil {
		return index.ScanView{}, nil, err
	}
	defer db.done()
	if ctx == nil {
		return index.ScanView{}, nil, validContextError(op)
	}
	if err := ctx.Err(); err != nil {
		return index.ScanView{}, nil, operationError(op, "context", err)
	}
	if query.Upper != "" && query.Lower >= query.Upper {
		return index.ScanView{}, nil, db.health()
	}
	if snapshot != nil {
		return index.ScanView{Snapshot: snapshot.view, Scan: query}, snapshot.segments, nil
	}
	db.indexGate.RLock()
	defer db.indexGate.RUnlock()
	view, err := query.Capture(ctx, db.keys)
	if err != nil {
		return index.ScanView{}, nil, operationError(op, "context", err)
	}
	var segments *scanSegmentSet
	if op == "scan" {
		segments = db.pinScanSegments()
	}
	return view, segments, nil
}

// walkSnapshot uses an existing snapshot or captures a fresh view for the
// iteration. No DB lock or admission is held while yielding to the caller.
func (db *DB) walkSnapshot(ctx context.Context, snapshot *Snapshot, query index.Scan, op string, yield func([]byte, []byte) bool) error {
	if snapshot != nil {
		if err := snapshot.acquire(); err != nil {
			return err
		}
		defer snapshot.release()
	}
	view, segments, err := db.captureScan(ctx, snapshot, query, op)
	if err != nil || view.Snapshot == nil {
		return err
	}
	if snapshot == nil {
		defer view.Close()
		defer db.unpinScanSegments(segments)
	}

	if err := db.scanStatus(ctx, snapshot, op); err != nil {
		return err
	}
	readValues := op == "scan"
	var result error
	var stopped bool
	var copies *prefixKeyCopies
	if query.Bounds != (index.Bounds{}) {
		copies = new(prefixKeyCopies)
	}
	visit := func(k string, a index.Address) bool {
		if result = db.scanStatus(ctx, snapshot, op); result != nil {
			return false
		}
		var value []byte
		if readValues {
			value, result = db.readScanValue(ctx, segments.files[a.FileID], a, k)
			if result != nil {
				return false
			}
			if result = db.scanStatus(ctx, snapshot, op); result != nil {
				return false
			}
		}
		var key []byte
		if copies == nil {
			key = []byte(k)
		} else {
			key = copies.copyKey(k)
		}
		stopped = !yield(key, value)
		return !stopped
	}
	err = view.Walk(visit)
	if result != nil {
		return result
	}
	if err != nil {
		if errors.Is(err, os.ErrClosed) || err == ctx.Err() {
			if lifeErr := db.scanStatus(ctx, snapshot, op); lifeErr != nil {
				return lifeErr
			}
		}
		db.poison(err)
		return operationError(op, "index", err)
	}
	if stopped {
		return nil
	}
	// Bounded and ordered scans report cancellation at the end of the range.
	// A full scan has already delivered its final entry, but a retained
	// snapshot still reports Close from that entry's callback.
	if query != (index.Scan{}) {
		return db.scanStatus(ctx, snapshot, op)
	}
	if snapshot != nil {
		if db.scanClosed(snapshot) {
			return ErrClosed
		}
		return db.health()
	}
	return nil
}

func (db *DB) scanClosed(snapshot *Snapshot) bool {
	if snapshot != nil && snapshot.isClosing() {
		return true
	}
	db.life.Lock()
	closing := db.closing
	db.life.Unlock()
	return closing
}

func (db *DB) scanStatus(ctx context.Context, snapshot *Snapshot, op string) error {
	if db.scanClosed(snapshot) {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return operationError(op, "context", err)
	}
	return db.health()
}

func (db *DB) readScanValue(ctx context.Context, segment *storage.DataSegment, address index.Address, key string) ([]byte, error) {
	if segment == nil {
		err := missingDataSegment(address.FileID)
		db.poison(err)
		return nil, operationError("scan", "index", err)
	}
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()
	value, err := db.store.ReadContext(ctx, storage.At(segment, address), key)
	if err != nil {
		if err == ctx.Err() {
			return nil, operationError("scan", "context", err)
		}
		db.readFailure(err)
		return nil, operationError("scan", "read", err)
	}
	return value, nil
}

func (db *DB) notifyCleanerIfUnpinned(segment *storage.DataSegment) {
	if !db.store.DataReferenced(segment) {
		db.notifyCleaner()
	}
}
