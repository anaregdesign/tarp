package engine

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/storage"
)

type failure struct{ err error }

// DB owns its directory until Close finishes. Do not copy a DB.
type DB struct {
	cleaner cleaner
	opts    Options
	store   *storage.Store
	writer  engineMutex
	state   engineMutex
	index   map[string]storage.Location
	cache   *cache.Cache
	failed  atomic.Pointer[failure]

	life     engineMutex
	cond     *sync.Cond
	active   int
	closing  bool
	closed   chan struct{}
	closeErr error
	backup   atomic.Bool
	compact  atomic.Bool
}

// Open creates or reopens a database and starts automatic incremental compaction.
// The parent directory must exist.
// Incompatible smaller limits do not truncate or clean existing data.
func Open(dir string, opts Options) (*DB, error) { return OpenWithIO(dir, opts, nil) }

// OpenWithIO attaches storage event hooks before recovery and worker startup.
func OpenWithIO(dir string, opts Options, fs *storage.IO) (*DB, error) {
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	s, index, err := storage.Open(dir, storage.Config{CacheBytes: opts.CacheBytes, RecordBytes: opts.MaxRecordBytes, TargetSegmentBytes: opts.TargetSegmentBytes, Handles: opts.ReadFileHandles}, fs)
	if err != nil {
		return nil, operationError("open", "recovery", err)
	}
	db := &DB{opts: opts, store: s, index: index, cache: cache.New(opts.CacheBytes), closed: make(chan struct{})}
	db.cond = sync.NewCond(&db.life)
	for _, loc := range index {
		loc.File.Pin()
		loc.File.LiveBytes.Add(loc.Bytes)
	}
	db.startCleaner()
	return db, nil
}

func (db *DB) health() error {
	if f := db.failed.Load(); f != nil {
		return errors.Join(ErrUnhealthy, f.err)
	}
	return nil
}

func (db *DB) poison(err error) { db.failed.CompareAndSwap(nil, &failure{err: publicCause(err)}) }

func (db *DB) admit() error {
	db.life.Lock()
	defer db.life.Unlock()
	if db.closing {
		return ErrClosed
	}
	if err := db.health(); err != nil {
		return err
	}
	db.active++
	return nil
}

func (db *DB) done() {
	db.life.Lock()
	db.active--
	if db.active == 0 {
		db.cond.Broadcast()
	}
	db.life.Unlock()
}

// Get returns an independently owned value, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := db.admit(); err != nil {
		return nil, err
	}
	defer db.done()
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}
	var k string
	for {
		db.state.Lock()
		if err := db.health(); err != nil {
			db.state.Unlock()
			return nil, err
		}
		if value, ok := db.cache.Get(key); ok {
			db.state.Unlock()
			return bytes.Clone(value), nil
		}
		loc, ok := db.index[string(key)]
		if !ok {
			db.state.Unlock()
			return nil, ErrNotFound
		}
		loc.File.Pin()
		db.state.Unlock()
		if k == "" {
			k = string(key)
		}
		value, err := db.store.Read(loc, k)
		loc.File.Unpin()
		if db.cleaner.pinned.Load() {
			db.notifyCleaner()
		}
		if err != nil {
			db.poison(err)
			return nil, operationError("get", "read", err)
		}
		db.state.Lock()
		if err := db.health(); err != nil {
			db.state.Unlock()
			return nil, err
		}
		current, exists := db.index[k]
		if !exists || current.Sequence != loc.Sequence {
			db.state.Unlock()
			continue
		}
		db.cache.Put(k, value)
		db.state.Unlock()
		return bytes.Clone(value), nil
	}
}

// Put durably upserts one value. A single oversized entry is rejected before
// changing files, the old value or cache recency. Full caches evict exact LRU.
func (db *DB) Put(key, value []byte) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if err := db.opts.checkSize(uint64(len(key)), uint64(len(value))); err != nil {
		return err
	}
	return db.mutate(format.Put, key, value)
}

// Delete durably removes one key. Missing keys return ErrNotFound without a write.
func (db *DB) Delete(key []byte) error {
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if len(key) == 0 {
		return ErrEmptyKey
	}
	return db.mutate(format.Delete, key, nil)
}

func (db *DB) mutate(kind uint8, key, value []byte) error {
	op := "put"
	if kind == format.Delete {
		op = "delete"
	}
	db.cleaner.writers.Add(1)
	fs := db.store.IO
	if fs != nil && fs.WriterWait != nil {
		fs.WriterWait(false)
	}
	db.writer.Lock()
	if fs != nil && fs.WriterWait != nil {
		fs.WriterWait(true)
	}
	previousActive := db.store.Current.ActiveID
	invalidatedSealed := false
	defer func() {
		rotated := previousActive != db.store.Current.ActiveID
		db.writer.Unlock()
		if db.cleaner.writers.Add(-1) == 0 {
			db.resumeCleaner()
		}
		if rotated || invalidatedSealed {
			db.notifyCleaner()
		}
	}()
	if err := db.health(); err != nil {
		return err
	}
	if kind == format.Delete {
		db.state.Lock()
		_, exists := db.index[string(key)]
		db.state.Unlock()
		if !exists {
			return ErrNotFound
		}
	}
	loc, err := db.store.Append(kind, key, value, db.health)
	if err != nil {
		var fault *storage.Fault
		if errors.As(err, &fault) && fault.Unsafe {
			db.poison(err)
		}
		return operationError(op, "commit", err)
	}
	// The caller keeps inputs stable through return. Own retained data only
	// after the append succeeds, outside the state lock used by readers.
	k := string(key)
	if kind == format.Put {
		value = bytes.Clone(value)
		if fs := db.store.IO; fs != nil && fs.PutCopied != nil {
			fs.PutCopied(len(k), len(value))
		}
	}
	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		return &OpError{Operation: op, Phase: "memory-publication", Outcome: Committed, Err: err}
	}
	if old, exists := db.index[k]; exists {
		invalidatedSealed = old.File != loc.File
		old.File.LiveBytes.Add(-old.Bytes)
		old.File.Unpin()
	}
	if kind == format.Delete {
		loc.File.DeleteBytes.Add(loc.Bytes)
		delete(db.index, k)
		db.cache.Delete(k)
	} else {
		loc.File.Pin()
		loc.File.LiveBytes.Add(loc.Bytes)
		db.index[k] = loc
		db.cache.Put(k, value)
	}
	return nil
}

// Close rejects new calls, waits for admitted calls and releases ownership last.
// Repeated/concurrent calls return the same cached result. It can wait for a
// slow maintenance destination; give Backup/Compact cancellable contexts.
func (db *DB) Close() error {
	db.life.Lock()
	if db.closing {
		done := db.closed
		db.life.Unlock()
		<-done
		return db.closeErr
	}
	db.closing = true
	db.cleaner.cancel()
	for db.active != 0 {
		db.cond.Wait()
	}
	db.life.Unlock()
	<-db.cleaner.done
	for _, loc := range db.index {
		loc.File.Unpin()
	}
	db.index, db.cache = nil, nil
	err := publicCause(db.store.Close())
	if f := db.failed.Load(); f != nil {
		err = errors.Join(f.err, err)
	}
	db.life.Lock()
	db.closeErr = err
	close(db.closed)
	db.life.Unlock()
	return err
}

func publicCause(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		err = errors.Join(ErrNoSpace, err)
	}
	var incompatible *storage.CompatibilityError
	if errors.As(err, &incompatible) {
		return errors.Join(&CompatibilityError{RequiredCacheBytes: incompatible.CacheBytes, RequiredRecordBytes: incompatible.RecordBytes}, err)
	}
	switch {
	case errors.Is(err, format.ErrCorrupt):
		return errors.Join(ErrCorrupt, err)
	case errors.Is(err, format.ErrUnsupported):
		return errors.Join(ErrUnsupportedFormat, err)
	case errors.Is(err, platform.ErrLocked):
		return errors.Join(ErrLocked, err)
	case errors.Is(err, platform.ErrUnsupported):
		return errors.Join(ErrUnsupportedPlatform, err)
	default:
		return err
	}
}

func operationError(op, phase string, err error) *OpError {
	outcome := NotApplied
	var fault *storage.Fault
	if errors.As(err, &fault) {
		phase, outcome = fault.Phase, Outcome(fault.Outcome)
	}
	return &OpError{Operation: op, Phase: phase, Outcome: outcome, Err: publicCause(err)}
}

func validContextError(op string) error {
	return &OpError{Operation: op, Phase: "context", Outcome: NotApplied, Err: fmt.Errorf("tarp: nil context")}
}
