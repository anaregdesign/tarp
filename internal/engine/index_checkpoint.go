package engine

import (
	"context"
	"errors"
	"io"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/index/memory"
	"github.com/anaregdesign/tarp/internal/storage"
)

func (o Options) fastCheckpointImporter() storage.FastIndexImporter {
	if o.IndexMode != IndexBTree || o.OpenValidation != Indexed {
		return nil
	}
	return func(ctx context.Context, s *storage.Store, source io.ReaderAt, closer io.Closer, size int64, layer *btree.OverlayLayout) (index.Backend, error) {
		dir, err := s.WorkspaceDir()
		if err != nil {
			return nil, err
		}
		if layer != nil {
			return btree.OpenOverlay(ctx, source, closer, *layer, dir, o.BTreeMemoryBudgetBytes/2)
		}
		return btree.OpenPacked(ctx, source, closer, size, dir, o.BTreeMemoryBudgetBytes/2)
	}
}

// Memory mode borrows a bounded, private pager to verify the packed topology
// and consumes each leaf during validation.
func (o Options) importCheckpoint(ctx context.Context, s *storage.Store, src io.Reader, size int64, visit func(string, index.Address) error) (index.Backend, error) {
	dir, err := s.WorkspaceDir()
	if err != nil {
		return nil, err
	}
	budget := o.BTreeMemoryBudgetBytes / 2
	var mem index.Backend
	var build index.InitialBuild
	if o.IndexMode == IndexMemory {
		budget = defaultBTreeMemoryBudgetBytes / 4
		mem = memory.New()
		build, err = mem.(index.InitialBuilder).BeginBuild()
		if err != nil {
			return nil, errors.Join(err, mem.Close())
		}
		validate := visit
		visit = func(key string, a index.Address) error {
			if err := validate(key, a); err != nil {
				return err
			}
			return build.Put(key, a)
		}
	}
	tree, err := btree.ImportPacked(ctx, src, size, dir, budget, visit)
	if storage.IsIndexCheckpointUnusable(err, btree.ErrInvalidPacked) {
		err = storage.ErrIndexCheckpointUnusable
	}
	if err != nil {
		if build != nil {
			build.Abort()
			err = errors.Join(err, mem.Close())
		}
		return nil, err
	}
	if build == nil {
		return tree, nil
	}
	if err := tree.Close(); err != nil {
		build.Abort()
		return nil, errors.Join(err, mem.Close())
	}
	if err := build.Finish(nil); err != nil {
		return nil, errors.Join(err, mem.Close())
	}
	return mem, nil
}

// CheckpointIndex shares the compaction workspace and leaves foreground
// mutations free to append and rotate while the immutable prefix is packed.
func (db *DB) CheckpointIndex(ctx context.Context) error {
	return db.runIndexCheckpoint(ctx, "checkpoint index", false)
}

func (db *DB) RefreshIndex(ctx context.Context) error {
	return db.runIndexCheckpoint(ctx, "refresh index", true)
}

func (db *DB) runIndexCheckpoint(ctx context.Context, op string, incremental bool) error {
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
	if !db.manualCheckpoint.CompareAndSwap(false, true) {
		return ErrBusy
	}
	db.cancelAutoRecovery()
	db.resumeCleaner()
	defer func() { db.manualCheckpoint.Store(false); db.notifyCleaner() }()
	select {
	case <-db.cleaner.gate:
		defer func() { db.cleaner.gate <- struct{}{} }()
	case <-ctx.Done():
		return operationError(op, "wait", ctx.Err())
	}
	return db.checkpointIndex(ctx, op, incremental, incremental)
}

func (db *DB) checkpointIndex(ctx context.Context, op string, incremental, skipCurrent bool) (result error) {
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return operationError(op, "capture", err)
	}
	if skipCurrent && db.store.IndexCheckpointValidated && db.store.CatalogMatchesIndexCheckpoint() {
		db.writer.Unlock()
		return nil
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
	work, err := db.store.BeginIndexCheckpoint()
	db.writer.Unlock()
	if err != nil {
		return operationError(op, "capture", err)
	}
	defer func() {
		if err := work.Release(); err != nil {
			if result == nil {
				result = &OpError{Operation: op, Phase: "cleanup", Outcome: Committed, Err: publicCause(err)}
			} else {
				result = errors.Join(result, err)
			}
		}
	}()
	budget := db.opts.BTreeMemoryBudgetBytes / 4
	if db.opts.IndexMode == IndexMemory {
		budget = defaultBTreeMemoryBudgetBytes / 4
	}
	var tree index.Backend
	if !incremental {
		tree, err = openIndex(db.store, IndexBTree, budget)
		if err != nil {
			return operationError(op, "workspace", err)
		}
	}
	err = db.store.RetryMaintenance(ctx, work.Sources(), func() error {
		if incremental {
			err := work.WriteIncremental(ctx, budget)
			if !storage.IndexRefreshNeedsBase(err) {
				return err
			}
			incremental = false
		}
		if tree == nil {
			var err error
			tree, err = openIndex(db.store, IndexBTree, budget)
			if err != nil {
				return err
			}
		}
		err := errors.Join(work.Write(ctx, tree, btree.ExportPacked), tree.Close())
		tree = nil
		return err
	}, nil)
	if err != nil {
		var source *storage.SourceError
		if errors.As(err, &source) {
			db.readFailure(err)
		}
		return operationError(op, "build", err)
	}
	if err := ctx.Err(); err != nil {
		return operationError(op, "pre-publication", err)
	}
	if err := work.PublishFile(); err != nil {
		return operationError(op, "index file", err)
	}
	db.writer.Lock()
	defer db.writer.Unlock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		return operationError(op, "pre-publication", err)
	}
	if err := work.Install(); err != nil {
		if errors.Is(err, storage.ErrCoveredCatalogPrefixChanged) {
			err = errors.Join(ErrBusy, err)
		}
		var fault *storage.Fault
		if errors.As(err, &fault) && fault.Outcome == Unknown {
			db.poison(err)
		}
		return operationError(op, "publication", err)
	}
	return nil
}
