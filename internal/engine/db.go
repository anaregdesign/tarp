package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/storage"
)

type failure struct{ err error }

// Hooks observes engine events for deterministic tests and measurement drivers.
// Set hooks before starting the operations that use them.
type Hooks struct {
	// CleanerStart gates the worker before its first pass until the channel closes.
	CleanerStart <-chan struct{}
	// PutCopied counts key and value bytes copied for the index and cache.
	PutCopied func(keyBytes, valueBytes int)
	// WriterWait observes a mutation before (false) and after (true) acquiring the writer gate.
	WriterWait func(acquired bool)
}

// DB owns its directory until Close finishes. Do not copy a DB.
type DB struct {
	cleaner cleaner
	opts    Options
	store   *storage.Store
	writer  engineMutex
	state   engineMutex
	keys    index.Backend
	// indexGate coordinates point/batch lookup and DATA lease acquisition with publication.
	// Neither DATA reads nor caller callbacks run under this lock.
	indexGate sync.RWMutex
	files     map[uint64]*storage.File
	revision  uint64 // protected by state
	cache     *cache.Cache
	failed    atomic.Pointer[failure]

	life     engineMutex
	cond     *sync.Cond
	active   int
	closing  bool
	closed   chan struct{}
	closeErr error
	backup   atomic.Bool
	compact  atomic.Bool

	// Keep the snapshot after the fields shared by Get and lifecycle admission.
	// Its placement affects parallel cache-hit throughput.
	stats Stats // guarded by state

	resident            index.ResidentLookup // usable under state, which excludes index publication
	batchReadSlots      chan struct{}        // shared by BatchGet calls on this DB
	fastSmallBatchReads atomic.Bool          // last sampled small DATA reads completed quickly
	hooks               *Hooks

	// Upper bound on live encoded value lengths, guarded by state. Deleting or
	// shrinking a value can leave it high until the next Open.
	maxSeenValueBytes int64

	// Only Compact sets or clears this under writer while it owns the cleaner
	// gate. indexGate guards the staged keys during that interval.
	memoryRemap *memoryCheckpointRemap
}

// Open creates or reopens a database and starts automatic incremental compaction.
// The parent directory must exist.
// Incompatible smaller limits do not truncate or clean existing data.
func Open(dir string, opts Options) (*DB, error) {
	return OpenContext(context.Background(), dir, opts, false)
}

// OpenContext optionally requires an initialized database.
func OpenContext(ctx context.Context, dir string, opts Options, existingOnly bool) (*DB, error) {
	return openDB(ctx, dir, opts, existingOnly, nil, nil)
}

// OpenWithIO attaches storage event hooks before recovery and worker startup.
func OpenWithIO(dir string, opts Options, fs *storage.IO) (*DB, error) {
	return OpenWithHooks(dir, opts, fs, nil)
}

// OpenWithHooks attaches engine and storage hooks before the first worker pass.
func OpenWithHooks(dir string, opts Options, fs *storage.IO, hooks *Hooks) (*DB, error) {
	return openDB(context.Background(), dir, opts, false, fs, hooks)
}

func openDB(ctx context.Context, dir string, opts Options, existingOnly bool, fs *storage.IO, hooks *Hooks) (*DB, error) {
	if ctx == nil {
		return nil, validContextError("open")
	}
	if err := ctx.Err(); err != nil {
		return nil, operationError("open", "context", err)
	}
	opts, err := opts.normalized()
	if err != nil {
		return nil, err
	}
	cfg := storage.Config{IndexedOpen: opts.OpenValidation == Indexed, ValueCacheBudgetBytes: opts.ValueCacheBudgetBytes, RecordBytes: opts.MaxRecordBytes, TargetSegmentBytes: opts.TargetSegmentBytes, Handles: opts.ReadFileHandles}
	s, keys, err := storage.OpenIndexed(ctx, dir, cfg, existingOnly, fs, func(s *storage.Store) (index.Backend, error) {
		return openIndex(s, opts.IndexMode, opts.BTreeMemoryBudgetBytes/2)
	}, opts.importCheckpoint)
	if err != nil {
		return nil, operationError("open", "recovery", err)
	}
	db := &DB{opts: opts, store: s, keys: keys, files: make(map[uint64]*storage.File), cache: cache.New(opts.ValueCacheBudgetBytes), closed: make(chan struct{}), hooks: hooks}
	db.resident, _ = keys.(index.ResidentLookup)
	db.batchReadSlots = make(chan struct{}, opts.BatchGetReadConcurrency)
	db.cond = sync.NewCond(&db.life)
	for id, f := range s.Files {
		if live := f.LiveBytes.Load(); live != 0 {
			db.files[id] = f
			db.stats.LiveRecordBytes += uint64(live)
		}
	}
	db.maxSeenValueBytes = s.MaxValueBytes
	db.stats.LiveKeys = uint64(keys.Count())
	db.initDataStats()
	// Finish opening a fresh, empty database once initialization has published
	// CURRENT. Only recovery has a cancellable index to hand off to the worker.
	if err := ctx.Err(); err != nil && !s.Created {
		return nil, operationError("open", "recovery", errors.Join(err, keys.Close(), s.Close()))
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

// countLookup records one classified key probe while the caller holds state.
func countLookup(counter *uint64) {
	if *counter < math.MaxUint64 {
		*counter++
	}
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
	for first := true; ; first = false {
		db.state.Lock()
		if err := db.health(); err != nil {
			db.state.Unlock()
			return nil, err
		}
		if value, ok := db.cache.Get(key); ok {
			if first {
				countLookup(&db.stats.ValueCache.Hits)
			}
			db.state.Unlock()
			return bytes.Clone(value), nil
		}
		if int64(len(key)) > db.opts.MaxRecordBytes-format.RecordHeaderBytes {
			if first {
				countLookup(&db.stats.MissingKeyLookups)
			}
			db.state.Unlock()
			return nil, ErrNotFound
		}
		var loc storage.Location
		var found bool
		if db.resident != nil {
			addr, ok := db.resident.LookupResident(key)
			if ok {
				file := db.files[addr.FileID]
				if file == nil {
					err := missingIndexFile(addr.FileID)
					db.poison(err)
					db.state.Unlock()
					return nil, operationError("get", "index", err)
				}
				loc = storage.At(file, addr)
			}
			found = ok
		} else {
			db.state.Unlock()
			db.indexGate.RLock()
			var err error
			loc, found, err = db.locationLocked(string(key))
			if err != nil {
				db.indexGate.RUnlock()
				return nil, err
			}
			db.state.Lock()
			db.indexGate.RUnlock()
		}
		if first {
			if found {
				countLookup(&db.stats.ValueCache.Misses)
			} else {
				countLookup(&db.stats.MissingKeyLookups)
			}
		}
		revision := db.revision
		if found {
			loc.File.Pin()
		}
		db.state.Unlock()
		if !found {
			return nil, ErrNotFound
		}
		k := string(key)
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
		if db.revision == revision {
			db.cache.Put(k, value)
			db.state.Unlock()
			return bytes.Clone(value), nil
		}
		db.state.Unlock()
		db.indexGate.RLock()
		current, found, err := db.locationLocked(k)
		if err != nil {
			db.indexGate.RUnlock()
			return nil, err
		}
		if !found || current.Sequence != loc.Sequence {
			db.indexGate.RUnlock()
			continue
		}
		db.state.Lock()
		err = db.health()
		if err == nil {
			db.cache.Put(k, value)
		}
		db.state.Unlock()
		db.indexGate.RUnlock()
		if err != nil {
			return nil, err
		}
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

// lockMutation notifies automatic maintenance of queued writers and returns the
// active file ID used to detect a rotation when releasing the writer gate.
func (db *DB) lockMutation() uint64 {
	db.cleaner.writers.Add(1)
	hooks := db.hooks
	if hooks != nil && hooks.WriterWait != nil {
		hooks.WriterWait(false)
	}
	db.writer.Lock()
	if hooks != nil && hooks.WriterWait != nil {
		hooks.WriterWait(true)
	}
	return db.store.Current.ActiveID
}

func (db *DB) unlockMutation(previousActive uint64, invalidatedSealed bool) {
	rotated := previousActive != db.store.Current.ActiveID
	if db.memoryRemap != nil {
		db.indexGate.Lock()
		clear(db.memoryRemap.staged)
		db.indexGate.Unlock()
	}
	db.writer.Unlock()
	if db.cleaner.writers.Add(-1) == 0 {
		db.resumeCleaner()
	}
	if rotated || invalidatedSealed {
		db.notifyCleaner()
	}
}

func (db *DB) mutate(kind uint8, key, value []byte) error {
	op := "put"
	if kind == format.Delete {
		op = "delete"
	}
	previousActive := db.lockMutation()
	invalidatedSealed := false
	defer func() { db.unlockMutation(previousActive, invalidatedSealed) }()
	if err := db.health(); err != nil {
		return err
	}
	tx := db.keys.Begin()
	finished := false
	defer func() {
		if !finished {
			tx.Abort()
		}
	}()
	entry := ownedMutation{key: string(key)}
	var old storage.Location
	prepare := func(loc storage.Location) error {
		var err error
		old, err = db.stage(tx, kind, entry.key, loc)
		return err
	}
	if kind == format.Delete {
		if err := prepare(storage.Location{}); err != nil {
			if errors.Is(err, ErrNotFound) {
				return ErrNotFound
			}
			return operationError(op, "index-prepare", err)
		}
		prepare = nil
	}
	loc, err := db.store.AppendPrepared(kind, key, value, db.health, prepare)
	if err != nil {
		db.storageFailure(err)
		return operationError(op, "commit", err)
	}
	db.copyPutValue(kind, &entry, value)
	db.indexGate.Lock()
	defer db.indexGate.Unlock()
	db.state.Lock()
	defer db.state.Unlock()
	if err := db.health(); err != nil {
		return &OpError{Operation: op, Phase: "memory-publication", Outcome: Committed, Err: err}
	}
	tx.Commit()
	finished = true
	invalidatedSealed = db.applyMutation(kind, entry, loc, old)
	db.countWriteCommit(previousActive)
	return nil
}

// countWriteCommit runs once per published single or batch write under state.
func (db *DB) countWriteCommit(previousActive uint64) {
	db.revision++
	db.stats.WriteCommits++
	if db.store.Current.ActiveID != previousActive {
		db.stats.SelectedSegments++
		db.stats.SelectedDataBytes += format.SegmentHeaderBytes
	}
}

type ownedMutation struct {
	key   string
	value []byte
}

// The index and Value LRU share the owned key. Copy the value after the append.
func (db *DB) copyPutValue(kind uint8, entry *ownedMutation, value []byte) {
	if kind == format.Put {
		entry.value = bytes.Clone(value)
		if hooks := db.hooks; hooks != nil && hooks.PutCopied != nil {
			hooks.PutCopied(len(entry.key), len(value))
		}
	}
}

// applyMutation requires the state lock and reports obsolete data in a sealed file.
func (db *DB) applyMutation(kind uint8, entry ownedMutation, loc, old storage.Location) bool {
	invalidatedSealed := false
	if old.File != nil {
		invalidatedSealed = old.File != loc.File
		old.File.LiveBytes.Add(-old.Bytes)
		db.stats.LiveRecordBytes -= uint64(old.Bytes)
		db.dropLocation(old.File)
	}
	if kind == format.Delete {
		loc.File.DeleteBytes.Add(loc.Bytes)
		db.stats.TombstoneBytes += uint64(loc.Bytes)
		db.stats.LiveKeys--
		db.cache.Delete(entry.key)
	} else {
		if old.File == nil {
			db.stats.LiveKeys++
		}
		db.addLocation(loc.File)
		loc.File.LiveBytes.Add(loc.Bytes)
		db.stats.LiveRecordBytes += uint64(loc.Bytes)
		if loc.ValueBytes > db.maxSeenValueBytes {
			db.maxSeenValueBytes = loc.ValueBytes
		}
		db.cache.Put(entry.key, entry.value)
	}
	db.stats.WrittenRecords++
	db.stats.SelectedDataBytes += uint64(loc.Bytes)
	return invalidatedSealed
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
	var checkpointErr error
	if db.opts.IndexMode == IndexBTree && db.health() == nil {
		checkpointErr = db.checkpointIndex(context.Background(), true)
	}
	for _, file := range db.files {
		file.LiveKeys.Store(0)
	}
	db.state.Lock()
	db.readStatsLocked(&db.stats)
	db.files, db.cache = nil, nil
	db.state.Unlock()
	err := publicCause(errors.Join(checkpointErr, db.keys.Close(), db.store.Close()))
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
		return errors.Join(&CompatibilityError{RequiredValueCacheBudgetBytes: incompatible.ValueCacheBudgetBytes, RequiredRecordBytes: incompatible.RecordBytes}, err)
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
		phase, outcome = fault.Phase, fault.Outcome
	}
	return &OpError{Operation: op, Phase: phase, Outcome: outcome, Err: publicCause(err)}
}

func validContextError(op string) error {
	return &OpError{Operation: op, Phase: "context", Outcome: NotApplied, Err: fmt.Errorf("tarp: nil context")}
}
