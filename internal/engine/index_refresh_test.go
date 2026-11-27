package engine

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/storage"
)

func openRefreshFixture(t *testing.T, source *DB) *DB {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "LOCK"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for name, data := range directoryBytes(t, source.store.Dir) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	opts := source.opts
	opts.OpenValidation = Indexed
	reopened, err := OpenWithHooks(dir, opts, nil, parkedMaintenance())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	return reopened
}

func TestRefreshIndexFailureBeforeSelectorKeepsPreviousCheckpoint(t *testing.T) {
	ctx := t.Context()
	var armed atomic.Bool
	fs := &storage.IO{Hook: func(point string) error {
		if point == "index.current.write:before" && armed.CompareAndSwap(true, false) {
			return syscall.ENOSPC
		}
		return nil
	}}
	db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
	mustPut(t, db, "first", "one")
	if err := db.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	prior := db.store.Catalog.Index
	mustPut(t, db, "second", "two")
	sealForCleaning(t, db)
	before := db.store.Current
	armed.Store(true)
	err := db.RefreshIndex(ctx)
	var op *OpError
	if armed.Load() || !errors.Is(err, ErrNoSpace) || !errors.As(err, &op) || op.Outcome != NotApplied {
		t.Fatalf("selector fault returned %v", err)
	}
	if db.store.Current != before || db.store.Catalog.Index != prior || db.health() != nil {
		t.Fatalf("unpublished refresh changed the selected frontier or health: %+v, %v", db.store.Current, db.health())
	}
	reopened := openRefreshFixture(t, db)
	if info := reopened.RecoveryInfo(); info.Path != RecoveryFastCheckpointReplay ||
		info.CheckpointID != prior.ID || info.CoveredSequence != 1 || info.LastUserSequence != 2 {
		t.Fatalf("fallback to previous INDEX did not preserve the committed tail: %+v", info)
	}
	wantValue(t, reopened, "first", "one")
	wantValue(t, reopened, "second", "two")

	if err := db.RefreshIndex(ctx); err != nil {
		t.Fatalf("retry refresh: %v", err)
	}
	if next := db.store.Catalog.Index; next.ID == prior.ID || next.CoveredSequence != 2 {
		t.Fatalf("retry did not publish a refreshed index: %+v", next)
	}
}

func TestRefreshIndexUncertainSelectorPreservesCandidateAndParent(t *testing.T) {
	ctx := t.Context()
	var armed atomic.Bool
	fs := &storage.IO{Hook: func(point string) error {
		if point == "index.current.rename:after" && armed.CompareAndSwap(true, false) {
			return syscall.ENOSPC
		}
		return nil
	}}
	db := newTestDBWithHooks(t, Options{}, fs, parkedMaintenance())
	mustPut(t, db, "first", "one")
	if err := db.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	prior := db.store.Catalog.Index
	mustPut(t, db, "second", "two")
	sealForCleaning(t, db)
	armed.Store(true)
	err := db.RefreshIndex(ctx)
	var op *OpError
	if armed.Load() || !errors.Is(err, ErrCommitUncertain) || !errors.As(err, &op) ||
		op.Outcome != Unknown || !errors.Is(db.health(), ErrUnhealthy) {
		t.Fatalf("selector fault returned %v, health %v", err, db.health())
	}
	reopened := openRefreshFixture(t, db)
	info := reopened.RecoveryInfo()
	if info.Path != RecoveryFastCheckpointReplay || info.CheckpointID == 0 || info.CheckpointID == prior.ID ||
		info.CoveredSequence != 2 || info.LastUserSequence != 2 {
		t.Fatalf("reopen did not select the newly named INDEX: %+v", info)
	}
	parent := filepath.Join(reopened.store.Dir, storage.IndexName(reopened.store.Current.UUID, prior.ID))
	if _, err := os.Stat(parent); err != nil {
		t.Fatalf("recovery removed the required parent INDEX: %v", err)
	}
	wantValue(t, reopened, "first", "one")
	wantValue(t, reopened, "second", "two")
}

func TestRefreshIndexIncludesCleanerRelocationAtSameUserSequence(t *testing.T) {
	ctx := t.Context()
	db := newTestDBWithHooks(t, Options{}, nil, parkedMaintenance())
	mustPut(t, db, "hot", strings.Repeat("obsolete", 1024))
	mustPut(t, db, "stable", "still live")
	if err := db.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	original := db.store.CatalogEntries().At(0)
	path := db.store.Segments[original.ID].Path

	mustPut(t, db, "hot", "updated")
	if err := db.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	prior := db.store.Catalog.Index
	if err := db.cleanSegment(ctx, original); err != nil {
		t.Fatal(err)
	}
	db.writer.Lock()
	_, _, err := db.store.Cleanup()
	db.writer.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("original DATA is still available after relocation: %v", err)
	}
	if seq := db.store.Current.LastUserSequence; seq != prior.CoveredSequence {
		t.Fatalf("cleaner advanced the sequence from %d to %d", prior.CoveredSequence, seq)
	}
	layout, err := db.IndexCheckpointStatus(ctx)
	changed := uint64(db.store.Current.ActiveCommittedEndOffset) + uint64(db.store.CatalogEntries().At(0).Bytes)
	if err != nil || layout.CheckpointID != prior.ID || layout.CoveredSequence != prior.CoveredSequence ||
		!layout.ChangedDataBytesExact || layout.ChangedDataBytes != changed {
		t.Fatalf("relocation status = %+v, %v; want %d changed bytes", layout, err, changed)
	}
	if err := db.RefreshIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if next := db.store.Catalog.Index; next.ID == prior.ID || next.CoveredSequence != prior.CoveredSequence {
		t.Fatalf("cleaner layout changes were not reflected in the new index: %+v, prior %+v", next, prior)
	}
	reopened := openRefreshFixture(t, db)
	if info := reopened.RecoveryInfo(); info.Path != RecoveryFastCheckpointReplay ||
		info.CheckpointID != db.store.Catalog.Index.ID ||
		info.CoveredSequence != prior.CoveredSequence || !info.ChangedDataBytesExact ||
		info.ChangedDataBytes != uint64(db.store.Current.ActiveCommittedEndOffset) {
		t.Fatalf("relocated DATA required additional replay after refresh: %+v", info)
	}
	wantValue(t, reopened, "hot", "updated")
	wantValue(t, reopened, "stable", "still live")
}
