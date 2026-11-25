package engine

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"os"
	"sync"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Snapshot provides an immutable, multi-operation point-in-time read view of a DB.
// Safe for concurrent read and scan calls across goroutines until Close.
type Snapshot struct {
	db                *DB
	mu                sync.Mutex
	refs              int  // 1 while open + 1 per in-flight operation/iterator
	closing           bool // true once Close has been called
	view              index.View
	segments          map[uint64]*storage.DataSegment
	seq               uint64
	revision          uint64
	maxSeenValueBytes int64
}

// Snapshot captures the current index and pins its referenced DATA segments.
// Writers and compaction can continue while the returned view is open.
func (db *DB) Snapshot() (*Snapshot, error) {
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()

	db.indexGate.RLock()
	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		db.indexGate.RUnlock()
		return nil, err
	}
	view := db.keys.View()
	segments := make(map[uint64]*storage.DataSegment, len(db.segments))
	for id, f := range db.segments {
		f.PinRetained()
		segments[id] = f
	}
	snap := &Snapshot{
		db:                db,
		refs:              1,
		view:              view,
		segments:          segments,
		seq:               db.publishedSeq,
		revision:          db.revision,
		maxSeenValueBytes: db.maxSeenValueBytes,
	}
	db.state.Unlock()
	db.indexGate.RUnlock()
	return snap, nil
}

// Sequence returns the last user sequence captured by the snapshot.
func (s *Snapshot) Sequence() uint64 {
	return s.seq
}

func (s *Snapshot) acquire() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrClosed
	}
	s.refs++
	s.mu.Unlock()
	return nil
}

func (s *Snapshot) release() {
	s.mu.Lock()
	s.refs--
	last := s.refs == 0
	view := s.view
	segments := s.segments
	if last {
		s.view = nil
		s.segments = nil
	}
	s.mu.Unlock()
	if last {
		s.freeResources(view, segments)
	}
}

func (s *Snapshot) isClosing() bool {
	s.mu.Lock()
	c := s.closing
	s.mu.Unlock()
	return c
}

func (s *Snapshot) freeResources(view index.View, segments map[uint64]*storage.DataSegment) {
	if view != nil {
		view.Close()
	}
	for _, f := range segments {
		f.UnpinRetained()
		s.db.notifyCleanerIfUnpinned(f)
	}
}

// Close rejects new snapshot reads. The last active operation releases the
// index view and segment pins if an operation is still using them.
func (s *Snapshot) Close() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.mu.Unlock()
	s.release()
	return nil
}

// Get returns an independently owned value at s.Sequence(), or ErrNotFound.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	db := s.db
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()

	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		return nil, err
	}
	sameRevision := db.revision == s.revision
	if sameRevision {
		if value, ok := db.cache.Get(key); ok {
			countLookup(&db.stats.ValueCache.Hits)
			db.state.Unlock()
			return bytes.Clone(value), nil
		}
		if int64(len(key)) > db.opts.MaxRecordBytes-format.RecordHeaderBytes {
			countLookup(&db.stats.MissingKeyLookups)
			db.state.Unlock()
			return nil, ErrNotFound
		}
	}
	db.state.Unlock()

	if int64(len(key)) > db.opts.MaxRecordBytes-format.RecordHeaderBytes {
		return nil, ErrNotFound
	}

	k := string(key)
	addr, found, err := s.view.Lookup(k)
	if err != nil {
		db.poison(err)
		return nil, operationError("get", "index", err)
	}
	if !found {
		if sameRevision {
			db.state.Lock()
			if db.revision == s.revision {
				countLookup(&db.stats.MissingKeyLookups)
			}
			db.state.Unlock()
		}
		return nil, ErrNotFound
	}
	if sameRevision {
		db.state.Lock()
		if db.revision == s.revision {
			countLookup(&db.stats.ValueCache.Misses)
		}
		db.state.Unlock()
	}

	segment := s.segments[addr.FileID]
	if segment == nil {
		err := missingDataSegment(addr.FileID)
		db.poison(err)
		return nil, operationError("get", "index", err)
	}

	value, err := db.store.Read(storage.At(segment, addr), k)
	if err != nil {
		db.readFailure(err)
		return nil, operationError("get", "read", err)
	}

	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		return nil, err
	}
	if db.revision == s.revision {
		db.cache.Put(k, value)
		db.state.Unlock()
		return bytes.Clone(value), nil
	}
	db.state.Unlock()
	return value, nil
}

// BatchGet reads keys from this snapshot in input order.
func (s *Snapshot) BatchGet(keys [][]byte) ([]BatchResult, error) {
	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	db := s.db
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()

	if err := CheckBatchCount(len(keys)); err != nil {
		return nil, err
	}
	for i, key := range keys {
		if err := CheckBatchKey("BatchGet", i, key); err != nil {
			return nil, err
		}
	}
	return s.readBatch(keys)
}

// BatchGetPrepared reads pre-validated encoded keys from this snapshot.
func (s *Snapshot) BatchGetPrepared(keys [][]byte) ([]BatchResult, error) {
	if err := s.acquire(); err != nil {
		return nil, err
	}
	defer s.release()

	db := s.db
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()

	return s.readBatch(keys)
}

func (s *Snapshot) readBatch(keys [][]byte) ([]BatchResult, error) {
	db := s.db
	results := make([]BatchResult, len(keys))
	if len(keys) == 0 {
		return db.finishBatchRead(keys, results, nil)
	}

	db.state.Lock()
	if err := db.health(); err != nil {
		db.state.Unlock()
		return nil, err
	}
	var hits, uncached int
	sameRevision := db.revision == s.revision
	if sameRevision {
		hits, uncached = db.peekBatch(keys, results, false)
		if uncached == 0 {
			err := checkBatchGetValueBytes(len(keys), db.opts.MaxBatchGetValueBytes, s.maxSeenValueBytes, func(i int) int64 {
				return int64(len(results[i].Value))
			})
			if err == nil {
				db.batchCounts(hits, 0, 0, len(keys)-hits)
			}
			db.state.Unlock()
			if err != nil {
				return nil, err
			}
			return db.finishBatchRead(keys, results, nil)
		}
	} else {
		uncached = len(keys)
	}
	db.state.Unlock()

	plan, err := db.planIndexedBatch(s.view, s.segments, keys, results, uncached, s.maxSeenValueBytes)
	if err != nil {
		return nil, err
	}
	if sameRevision {
		db.state.Lock()
		if db.revision == s.revision {
			db.batchCounts(hits, plan.misses, plan.distinct-plan.misses, len(keys)-plan.distinct-hits)
		}
		db.state.Unlock()
	}
	return db.finishBatchRead(keys, results, plan.reads)
}

// Scan yields all entries in this snapshot.
func (s *Snapshot) Scan(ctx context.Context) iter.Seq2[Entry, error] {
	return s.ScanPrefix(ctx, nil)
}

// ScanPrefix yields entries in this snapshot whose encoded key starts with encodedPrefix.
func (s *Snapshot) ScanPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[Entry, error] {
	bounds := index.PrefixBounds(encodedPrefix)
	return func(yield func(Entry, error) bool) {
		err := s.walk(ctx, true, bounds, false, false, func(k, v []byte) bool {
			return yield(Entry{Key: k, Value: v}, nil)
		})
		if err != nil {
			yield(Entry{}, err)
		}
	}
}

// ScanRange yields entries in [lower, upper) in strictly ascending or descending
// bytewise lexicographic order from this snapshot.
func (s *Snapshot) ScanRange(ctx context.Context, lower, upper []byte, reverse bool) iter.Seq2[Entry, error] {
	bounds := index.Bounds{Lower: string(lower), Upper: string(upper)}
	return func(yield func(Entry, error) bool) {
		err := s.walk(ctx, true, bounds, true, reverse, func(k, v []byte) bool {
			return yield(Entry{Key: k, Value: v}, nil)
		})
		if err != nil {
			yield(Entry{}, err)
		}
	}
}

// Keys yields all keys in this snapshot without reading DATA segments.
func (s *Snapshot) Keys(ctx context.Context) iter.Seq2[[]byte, error] {
	return s.KeysPrefix(ctx, nil)
}

// KeysPrefix yields keys in this snapshot matching encodedPrefix without reading DATA segments.
func (s *Snapshot) KeysPrefix(ctx context.Context, encodedPrefix []byte) iter.Seq2[[]byte, error] {
	bounds := index.PrefixBounds(encodedPrefix)
	return func(yield func([]byte, error) bool) {
		err := s.walk(ctx, false, bounds, false, false, func(k, _ []byte) bool {
			return yield(k, nil)
		})
		if err != nil {
			yield(nil, err)
		}
	}
}

// KeysRange yields keys in [lower, upper) in strictly ascending or descending
// bytewise lexicographic order from this snapshot without reading DATA segments.
func (s *Snapshot) KeysRange(ctx context.Context, lower, upper []byte, reverse bool) iter.Seq2[[]byte, error] {
	bounds := index.Bounds{Lower: string(lower), Upper: string(upper)}
	return func(yield func([]byte, error) bool) {
		err := s.walk(ctx, false, bounds, true, reverse, func(k, _ []byte) bool {
			return yield(k, nil)
		})
		if err != nil {
			yield(nil, err)
		}
	}
}

func (s *Snapshot) walk(ctx context.Context, values bool, bounds index.Bounds, ordered, reverse bool, yield func([]byte, []byte) bool) error {
	op := "keys"
	if values {
		op = "scan"
	}
	if err := s.acquire(); err != nil {
		return err
	}
	defer s.release()

	db := s.db
	if err := db.admit(); err != nil {
		return err
	}
	if ctx == nil {
		db.done()
		return validContextError(op)
	}
	if err := ctx.Err(); err != nil {
		db.done()
		return operationError(op, "context", err)
	}
	if ordered && bounds.Upper != "" && bounds.Lower >= bounds.Upper {
		err := db.health()
		db.done()
		return err
	}
	db.done()

	check := func() error {
		if s.isClosing() {
			return ErrClosed
		}
		db.life.Lock()
		closing := db.closing
		db.life.Unlock()
		if closing {
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			return operationError(op, "context", err)
		}
		return db.health()
	}
	if err := check(); err != nil {
		return err
	}

	var result error
	var stopped bool
	var copies *prefixKeyCopies
	if bounds.Lower != "" || bounds.Upper != "" {
		copies = new(prefixKeyCopies)
	}
	visit := func(k string, a index.Address) bool {
		if result = check(); result != nil {
			return false
		}
		var value []byte
		if values {
			f := s.segments[a.FileID]
			if f == nil {
				err := missingDataSegment(a.FileID)
				db.poison(err)
				result = operationError(op, "index", err)
				return false
			}
			if result = db.admit(); result != nil {
				return false
			}
			value, result = db.store.ReadContext(ctx, storage.At(f, a), k)
			if result != nil {
				if result == ctx.Err() {
					db.done()
					result = operationError(op, "context", result)
					return false
				}
				db.readFailure(result)
				db.done()
				result = operationError(op, "read", result)
				return false
			}
			db.done()
			if result = check(); result != nil {
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

	var err error
	switch {
	case ordered && reverse:
		rv, ok := s.view.(index.ReverseRangeView)
		if !ok {
			err = errors.New("index: reverse range unsupported")
		} else {
			err = rv.RangeDesc(bounds.Lower, bounds.Upper, visit)
		}
	case ordered || bounds.Lower != "" || bounds.Upper != "":
		rv, ok := s.view.(index.RangeView)
		if !ok {
			err = s.view.All(func(k string, a index.Address) bool {
				if !bounds.Contains(k) {
					return true
				}
				return visit(k, a)
			})
		} else {
			err = rv.Range(bounds.Lower, bounds.Upper, visit)
		}
	default:
		err = s.view.All(visit)
	}

	if result != nil {
		return result
	}
	if err != nil {
		if errors.Is(err, os.ErrClosed) || err == ctx.Err() {
			if lifeErr := check(); lifeErr != nil {
				return lifeErr
			}
		}
		db.poison(err)
		return operationError(op, "index", err)
	}
	if !stopped {
		if s.isClosing() {
			return ErrClosed
		}
		db.life.Lock()
		closing := db.closing
		db.life.Unlock()
		if closing {
			return ErrClosed
		}
		if ordered || bounds.Lower != "" || bounds.Upper != "" {
			return check()
		}
		return db.health()
	}
	return nil
}
