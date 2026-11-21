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
	id, err := s.allocateID()
	if err != nil {
		return fault("catalog.identity", publication.NotApplied, false, err)
	}
	b := format.EncodeCatalog(catalog)
	name := filepath.Join(s.Dir, ManifestName(s.Current.UUID, id))
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
	prepare := func() error {
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
		if s.Current.Generation == math.MaxUint64 {
			return fault("clean.generation", publication.NotApplied, false, errors.New("storage: publication generation exhausted"))
		}
		candidate = s.Current
		candidate.Generation++
	} else if err := prepare(); err != nil {
		return err
	}
	candidate.ManifestID, candidate.ManifestHash = id, sha256.Sum256(b)
	previous := s.Current.ManifestID
	if err := s.publishFor(candidate, actor); err != nil {
		return err
	}
	s.retireManifest(previous)
	if old := s.Catalog.Index.ID; old != 0 && old != catalog.Index.ID {
		s.retireIndex(old)
	}
	s.Catalog, s.Manifest = catalog, b
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
