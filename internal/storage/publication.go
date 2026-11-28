package storage

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
)

func (s *Store) publishCatalog(catalog format.Catalog, candidate format.Frontier) error {
	return s.publishCatalogFor(catalog, candidate, "", nil)
}

var errCatalogChanged = errors.New("storage: catalog changed while preparing cleaner manifest")

func (s *Store) publishCatalogFor(catalog format.Catalog, candidate format.Frontier, actor string, offGate func(func() error) error) (result error) {
	entries := CatalogEntries{catalogFromSlice(catalog.Entries)}
	catalog.Entries = nil
	return s.publishCatalogBase(catalog, entries, candidate, actor, offGate)
}

const rebaseCatalogAt = 128
const maxCatalogEdits = 256

func (s *Store) CatalogEditBytes() uint64 {
	if len(s.catalogChain) > maxCatalogEdits {
		return format.CatalogEncodedBytes(s.catalogEntries.Len()+1) + format.FrontierBytes
	}
	return format.MaxCatalogEditBytes + format.FrontierBytes
}

// StagedCatalogBytes bounds metadata for a staged append at the current cut.
func (s *Store) StagedCatalogBytes() uint64 {
	if len(s.catalogChain) > maxCatalogEdits {
		return format.CatalogEncodedBytes(s.catalogEntries.Len()+format.MaxStagedCatalogEntries) + format.FrontierBytes
	}
	return format.MaxStagedCatalogEditBytes + format.FrontierBytes
}

func (s *Store) NeedsCatalogRebase() bool { return len(s.catalogChain) > rebaseCatalogAt }

// RebaseCatalog checkpoints an immutable catalog view while callers continue
// to append DATA. A foreground rotation can supersede the prepared base.
func (s *Store) RebaseCatalog(offGate func(func() error) error) error {
	if !s.NeedsCatalogRebase() || s.Current.PublicationGeneration == math.MaxUint64 {
		return nil
	}
	candidate := s.Current
	candidate.PublicationGeneration++
	err := s.publishCatalogBase(s.Catalog, s.catalogEntries, candidate, "catalog-rebase.", offGate)
	if err == errCatalogChanged {
		return nil
	}
	return err
}

func (s *Store) publishCatalogBase(catalog format.Catalog, entries CatalogEntries, candidate format.Frontier, actor string, offGate func(func() error) error) error {
	return s.publishCatalogObject(catalog, entries, candidate, actor, offGate, false, func() []byte {
		materialized := catalog
		materialized.Entries = entries.Slice(0, entries.Len())
		return format.EncodeCatalog(materialized)
	})
}

func (s *Store) publishCatalogEdit(catalog format.Catalog, start, remove int, inserted []format.CatalogEntry, candidate format.Frontier, actor string, offGate func(func() error) error) error {
	catalog.Entries = nil
	entries := s.catalogEntries.splice(start, remove, inserted)
	// Keep recovery bounded even if foreground rotations continually supersede
	// background rebasing or the background worker could not write its base.
	if len(s.catalogChain) > maxCatalogEdits {
		return s.publishCatalogBase(catalog, entries, candidate, actor, offGate)
	}
	edit := format.CatalogEdit{PreviousID: s.Current.ManifestID, PreviousDigest: s.Current.ManifestDigest,
		Start: start, Remove: remove, Catalog: catalog}
	edit.Catalog.Entries = inserted
	return s.publishCatalogObject(catalog, entries, candidate, actor, offGate, true, func() []byte {
		return format.EncodeCatalogEdit(edit)
	})
}

func (s *Store) publishCatalogMetadata(catalog format.Catalog, candidate format.Frontier, actor string) error {
	return s.publishCatalogEdit(catalog, s.catalogEntries.Len(), 0, nil, candidate, actor, nil)
}

func (s *Store) publishStagedCatalogAppend(catalog format.Catalog, inserted []format.CatalogEntry, candidate format.Frontier) error {
	const actor = "staged."
	start := s.catalogEntries.Len()
	entries := s.catalogEntries.splice(start, 0, inserted)
	if len(s.catalogChain) > maxCatalogEdits {
		return s.publishCatalogBase(catalog, entries, candidate, actor, nil)
	}
	editCatalog := catalog
	editCatalog.Entries = inserted
	edit := format.CatalogEdit{
		PreviousID: s.Current.ManifestID, PreviousDigest: s.Current.ManifestDigest,
		Start: start, Catalog: editCatalog, StagedAppend: true,
	}
	return s.publishCatalogObject(catalog, entries, candidate, actor, nil, true, func() []byte {
		return format.EncodeCatalogEdit(edit)
	})
}

func (s *Store) publishCatalogObject(catalog format.Catalog, entries CatalogEntries, candidate format.Frontier, actor string, offGate func(func() error) error, incremental bool, encode func() []byte) (result error) {
	id, err := s.allocateID()
	if err != nil {
		return fault("catalog.identity", publication.NotApplied, false, err)
	}
	name := filepath.Join(s.Dir, ManifestName(s.Current.UUID, id))
	if offGate != nil {
		if s.buildingCatalogs == nil {
			s.buildingCatalogs = make(map[uint64]bool)
		}
		s.buildingCatalogs[id] = true
		defer delete(s.buildingCatalogs, id)
	}
	if actor != "" {
		// Only this actor owns its freshly allocated catalog. Discard a
		// known-unpublished file immediately, even on transient ENOSPC.
		defer func() {
			if result == nil || errors.Is(result, os.ErrExist) {
				return
			}
			var f *Fault
			if errors.As(result, &f) && f.Outcome == publication.Unknown {
				return
			}
			err := s.IO.Do(actor+"discard.catalog", func() error { return os.Remove(name) })
			if errors.Is(err, os.ErrNotExist) {
				return
			}
			if err == nil {
				err = s.IO.SyncDir(s.Dir, actor+"discard.dirsync")
			}
			if err != nil {
				s.retireManifest(id)
				s.retireSyncPending = true
				result = errors.Join(result, err)
			}
		}()
	}
	var b []byte
	var digest [32]byte
	prepare := func() error {
		b = encode()
		digest = sha256.Sum256(b)
		if err := s.IO.NewFile(name, b, actor+"catalog"); err != nil {
			return fault("catalog.prepare", publication.NotApplied, true, err)
		}
		if err := s.IO.SyncDir(s.Dir, actor+"catalog.dirsync"); err != nil {
			return fault("catalog.dirsync", publication.NotApplied, true, err)
		}
		return nil
	}
	if offGate != nil {
		if err := offGate(prepare); err != nil {
			return err
		}
		// Concurrent appends keep this catalog valid but advance the frontier.
		// A rotation installs another catalog and requires a fresh splice.
		if candidate.ManifestID != s.Current.ManifestID {
			return errCatalogChanged
		}
		if s.Current.PublicationGeneration == math.MaxUint64 {
			return fault("clean.generation", publication.NotApplied, false, errors.New("storage: publication generation exhausted"))
		}
		candidate = s.Current
		candidate.PublicationGeneration++
	} else if err := prepare(); err != nil {
		return err
	}
	candidate.ManifestID, candidate.ManifestDigest = id, digest
	if err := s.publishFor(candidate, actor); err != nil {
		return err
	}
	if !incremental {
		for _, previous := range s.catalogChain {
			s.retireManifest(previous)
		}
		s.catalogChain = s.catalogChain[:0]
		s.catalogBytes = 0
	}
	s.catalogChain = append(s.catalogChain, id)
	s.catalogBytes += uint64(len(b))
	if old := s.Catalog.Index.ID; old != catalog.Index.ID {
		next := s.pendingIndexAncestors
		if old != 0 && !next[old] {
			s.retireIndex(old)
		}
		for ancestor := range s.indexAncestors {
			if !next[ancestor] {
				s.retireIndex(ancestor)
			}
		}
		s.indexAncestors = next
	}
	s.Catalog, s.catalogEntries = catalog, entries
	return nil
}

func (s *Store) publish(candidate format.Frontier) error {
	return s.publishFor(candidate, "")
}

func (s *Store) publishFor(candidate format.Frontier, actor string) (result error) {
	pattern := fmt.Sprintf(".tarp-current-%x-*.tmp", s.Current.UUID)
	f, err := os.CreateTemp(s.Dir, pattern)
	if err != nil {
		return fault("current.create", publication.NotApplied, true, err)
	}
	name := f.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			result = errors.Join(result, f.Close())
		}
		if !renamed {
			if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
	}()
	if err := s.IO.Write(f, format.EncodeFrontier(candidate), 0, actor+"current.write"); err != nil {
		return fault("current.write", publication.NotApplied, true, err)
	}
	if err := s.IO.Sync(f, actor+"current.sync"); err != nil {
		return fault("current.sync", publication.NotApplied, true, err)
	}
	if err := f.Close(); err != nil {
		closed = true
		return fault("current.close", publication.NotApplied, true, err)
	}
	closed = true
	if err := s.IO.Do(actor+"current.rename", func() error { return os.Rename(name, filepath.Join(s.Dir, "CURRENT")) }); err != nil {
		return fault("current.rename", publication.Unknown, true, err)
	}
	renamed = true
	if err := s.IO.SyncDir(s.Dir, actor+"current.dirsync"); err != nil {
		return fault("current.dirsync", publication.Unknown, true, err)
	}
	s.Current = candidate
	return nil
}
