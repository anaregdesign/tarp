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
	if storage.IsIndexMiss(err, btree.ErrInvalidPacked) {
		err = storage.ErrIndexMiss
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
	if err := db.admit(); err != nil {
		return err
	}
	defer db.done()
	if ctx == nil {
		return validContextError("checkpoint index")
	}
	if err := ctx.Err(); err != nil {
		return operationError("checkpoint index", "context", err)
	}
	if !db.compact.CompareAndSwap(false, true) {
		return ErrBusy
	}
	db.resumeCleaner()
	defer func() { db.compact.Store(false); db.notifyCleaner() }()
	select {
	case <-db.cleaner.gate:
		defer func() { db.cleaner.gate <- struct{}{} }()
	case <-ctx.Done():
		return operationError("checkpoint index", "wait", ctx.Err())
	}
	return db.checkpointIndex(ctx, false)
}

func (db *DB) checkpointIndex(ctx context.Context, closing bool) (result error) {
	const op = "checkpoint index"
	db.writer.Lock()
	if err := errors.Join(ctx.Err(), db.health()); err != nil {
		db.writer.Unlock()
		return operationError(op, "capture", err)
	}
	cp := db.store.Catalog.Index
	if closing && db.store.IndexCheckpointHit && cp.ID != 0 && cp.Sequence == db.store.Current.Sequence && cp.Entries == uint64(len(db.store.Catalog.Files)) {
		db.writer.Unlock()
		return nil
	}
	if db.store.Current.ActiveEnd != format.SegmentHeaderBytes {
		if err := db.sealActive(); err != nil {
			db.storageFailure(err)
			db.writer.Unlock()
			failed := operationError(op, "rotation", err)
			failed.Outcome = NotApplied
			return failed
		}
	}
	work, err := db.store.BeginIndex()
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
	tree, err := openIndex(db.store, IndexBTree, budget)
	if err != nil {
		return operationError(op, "workspace", err)
	}
	err = errors.Join(work.Write(ctx, tree, btree.ExportPacked), tree.Close())
	if err != nil {
		var source *storage.SourceError
		if errors.As(err, &source) {
			db.poison(err)
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
		if errors.Is(err, storage.ErrIndexLayoutChanged) {
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
