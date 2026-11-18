package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
)

var ErrCorrupt = format.ErrCorrupt

type Config struct {
	CacheBytes, RecordBytes, TargetSegmentBytes int64
	Handles                                     int
}

type CompatibilityError struct {
	CacheBytes, RecordBytes int64
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("storage: incompatible limits: cache=%d record=%d", e.CacheBytes, e.RecordBytes)
}

type File struct {
	ID                     uint64
	Kind                   uint32
	Path                   string
	Size                   atomic.Int64
	refs                   atomic.Int64
	LiveBytes, DeleteBytes atomic.Int64
}

func (f *File) Pin()              { f.refs.Add(1) }
func (f *File) References() int64 { return f.refs.Load() }

func (f *File) Unpin() {
	if f.refs.Add(-1) < 0 {
		panic("storage: negative file reference count")
	}
}

type Location struct {
	File       *File
	Offset     int64
	Bytes      int64
	ValueBytes int64
	Sequence   uint64
}

type Store struct {
	Dir               string
	Config            Config
	IO                *IO
	Current           format.Frontier
	Catalog           format.Catalog
	Manifest          []byte
	Files             map[uint64]*File
	active            *os.File
	lock              *platform.DirectoryLock
	readers           *handles
	nextID            uint64
	retired           map[uint64]*File
	retiredCatalogs   map[uint64]struct{}
	retireSyncPending bool
}

func DataName(uuid [16]byte, id uint64) string {
	return fmt.Sprintf("DATA-%x-%016x", uuid, id)
}

func ManifestName(uuid [16]byte, id uint64) string {
	return fmt.Sprintf("MANIFEST-%x-%016x", uuid, id)
}

func parseID(name, prefix string, uuid [16]byte) (uint64, bool) {
	head := prefix + "-" + hex.EncodeToString(uuid[:]) + "-"
	if !strings.HasPrefix(name, head) || len(name) != len(head)+16 {
		return 0, false
	}
	n, err := strconv.ParseUint(name[len(head):], 16, 64)
	return n, err == nil && n != 0 && fmt.Sprintf("%016x", n) == name[len(head):]
}

func (s *Store) allocateID() (uint64, error) {
	if s.nextID == math.MaxUint64 {
		return 0, errors.New("storage: file identity exhausted")
	}
	s.nextID++
	return s.nextID, nil
}

func Open(dir string, cfg Config, fs *IO) (_ *Store, index map[string]Location, result error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}
	if err := os.Mkdir(absolute, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, nil, err
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, nil, err
	}
	// A restored database has no LOCK inode. Probe CURRENT without creating one
	// in a foreign store. All authoritative validation is repeated under lock.
	if _, err := os.Lstat(filepath.Join(absolute, "LOCK")); errors.Is(err, os.ErrNotExist) {
		b, err := readCurrent(filepath.Join(absolute, "CURRENT"))
		if err == nil {
			if _, err := format.DecodeFrontier(b); err != nil {
				return nil, nil, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, nil, err
		}
	} else if err != nil {
		return nil, nil, err
	}
	lock, err := platform.Lock(absolute)
	if err != nil {
		return nil, nil, err
	}
	s := &Store{Dir: absolute, Config: cfg, IO: fs, lock: lock, readers: newHandles(cfg.Handles), Files: make(map[uint64]*File)}
	defer func() {
		if result != nil {
			result = errors.Join(result, s.Close())
		}
	}()
	if err := fs.Do("lock.sync", lock.Sync); err != nil {
		return nil, nil, err
	}
	if err := fs.SyncDir(absolute, "lock.dirsync"); err != nil {
		return nil, nil, err
	}
	names, err := os.ReadDir(absolute)
	if err != nil {
		return nil, nil, err
	}
	if len(names) == 1 && names[0].Name() == "LOCK" {
		if err := fs.SyncDir(filepath.Dir(absolute), "init.parent.sync"); err != nil {
			return nil, nil, err
		}
		if err := s.initialize(); err != nil {
			return nil, nil, err
		}
		return s, make(map[string]Location), nil
	}
	image, err := Validate(absolute, cfg)
	if err != nil {
		return nil, nil, err
	}
	s.Current, s.Catalog, s.Manifest, s.Files = image.Current, image.Catalog, image.Manifest, image.Files
	if err := s.inspectNames(); err != nil {
		return nil, nil, err
	}
	s.active, err = os.OpenFile(s.Files[s.Current.ActiveID].Path, os.O_RDWR, 0)
	if err != nil {
		return nil, nil, err
	}
	if s.Files[s.Current.ActiveID].Size.Load() != s.Current.ActiveEnd {
		if err := fs.Do("recover.truncate", func() error { return s.active.Truncate(s.Current.ActiveEnd) }); err != nil {
			return nil, nil, err
		}
		if err := fs.Sync(s.active, "recover.sync"); err != nil {
			return nil, nil, err
		}
		s.Files[s.Current.ActiveID].Size.Store(s.Current.ActiveEnd)
	}
	if _, _, err := s.Cleanup(); err != nil {
		return nil, nil, err
	}
	return s, image.Index, nil
}

func (s *Store) initialize() error {
	if _, err := rand.Read(s.Current.UUID[:]); err != nil {
		return err
	}
	s.Catalog.UUID = s.Current.UUID
	file, fd, err := s.newSegment(format.Log, "segment", nil)
	if err != nil {
		return err
	}
	s.active = fd
	s.Catalog.ActiveID = file.ID
	candidate := s.Current
	candidate.Generation, candidate.ActiveID, candidate.ActiveEnd = 1, file.ID, format.SegmentHeaderBytes
	return s.publishCatalog(s.Catalog, candidate)
}

func (s *Store) newSegment(kind uint32, point string, firstRecord []byte) (*File, *os.File, error) {
	id, err := s.allocateID()
	if err != nil {
		return nil, nil, err
	}
	f := &File{ID: id, Kind: kind, Path: filepath.Join(s.Dir, DataName(s.Current.UUID, id))}
	fd, err := os.OpenFile(f.Path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	b := format.EncodeSegment(format.Segment{UUID: s.Current.UUID, ID: id, Kind: kind})
	err = s.IO.Write(fd, b, 0, point+".write")
	if err == nil && len(firstRecord) > 0 {
		err = s.IO.Write(fd, firstRecord, format.SegmentHeaderBytes, "data.write")
	}
	if err == nil {
		err = s.IO.Sync(fd, point+".sync")
	}
	if err != nil {
		closeErr := fd.Close()
		removeErr := s.IO.Do("segment.discard", func() error { return os.Remove(f.Path) })
		return nil, nil, errors.Join(err, closeErr, removeErr)
	}
	f.Size.Store(format.SegmentHeaderBytes + int64(len(firstRecord)))
	s.Files[id] = f
	return f, fd, nil
}

func (s *Store) Rotate() error {
	if s.Current.ActiveEnd == format.SegmentHeaderBytes {
		return nil
	}
	if s.Current.Generation == math.MaxUint64 {
		return fault("rotation.generation", "not-applied", false, errors.New("storage: publication generation exhausted"))
	}
	return s.rotate(nil, nil)
}

// A rotating append syncs the header and its first record together. CURRENT
// selects the new segment, catalog and user sequence in one publication.
// Explicit checkpoint sealing selects a header-only log at the old sequence.
func (s *Store) rotate(firstRecord []byte, healthy func() error) error {
	file, fd, err := s.newSegment(format.Log, "segment", firstRecord)
	if err != nil {
		return fault("rotation.segment", "not-applied", true, err)
	}
	if healthy != nil {
		if err := healthy(); err != nil {
			return errors.Join(fault("commit.health", "not-applied", true, err), fd.Close())
		}
	}
	catalog := s.Catalog
	catalog.Files = append(append([]format.File(nil), catalog.Files...), format.File{
		ID: s.Current.ActiveID, Bytes: s.Current.ActiveEnd, Kind: format.Log,
		First: s.Catalog.SealedEnd() + 1, Last: s.Current.Sequence,
	})
	catalog.ActiveID = file.ID
	candidate := s.Current
	candidate.Generation++
	candidate.ActiveID, candidate.ActiveEnd = file.ID, file.Size.Load()
	if len(firstRecord) > 0 {
		candidate.Sequence++
	}
	if err := s.publishCatalog(catalog, candidate); err != nil {
		return errors.Join(err, fd.Close())
	}
	old := s.active
	s.active = fd
	if err := errors.Join(old.Close(), s.IO.Check("rotation.close:after")); err != nil {
		return fault("rotation.close", "committed", true, err)
	}
	return nil
}

func (s *Store) Append(kind uint8, key, value []byte, healthy func() error) (Location, error) {
	if s.Current.Sequence == math.MaxUint64 || s.Current.Generation == math.MaxUint64 {
		return Location{}, fault("commit.sequence", "not-applied", false, errors.New("storage: sequence or publication generation exhausted"))
	}
	b := format.EncodeRecord(kind, s.Current.Sequence+1, key, value)
	loc := Location{File: s.Files[s.Current.ActiveID], Offset: s.Current.ActiveEnd, Bytes: int64(len(b)), ValueBytes: int64(len(value)), Sequence: s.Current.Sequence + 1}
	if loc.Offset > format.SegmentHeaderBytes && loc.Offset > s.Config.TargetSegmentBytes-loc.Bytes {
		if err := s.rotate(b, healthy); err != nil {
			return Location{}, err
		}
		loc.File, loc.Offset = s.Files[s.Current.ActiveID], format.SegmentHeaderBytes
		return loc, nil
	}
	if err := s.IO.Write(s.active, b, loc.Offset, "data.write"); err != nil {
		return Location{}, fault("data.write", "not-applied", true, err)
	}
	if err := s.IO.Sync(s.active, "data.sync"); err != nil {
		return Location{}, fault("data.sync", "not-applied", true, err)
	}
	if err := healthy(); err != nil {
		return Location{}, fault("commit.health", "not-applied", true, err)
	}
	candidate := s.Current
	candidate.Generation++
	candidate.Sequence, candidate.ActiveEnd = loc.Sequence, loc.Offset+loc.Bytes
	if err := s.publish(candidate); err != nil {
		return Location{}, err
	}
	loc.File.Size.Store(candidate.ActiveEnd)
	return loc, nil
}

func (s *Store) Read(loc Location, key string) ([]byte, error) {
	if err := s.IO.Check("read:before"); err != nil {
		return nil, err
	}
	h, err := s.readers.acquire(loc.File)
	if err != nil {
		return nil, err
	}
	value, err := format.ReadValue(h.fd, loc.Offset, loc.Bytes, key, loc.Sequence)
	s.readers.release(h)
	if err == nil {
		err = s.IO.Check("read:after")
	}
	return value, err
}

func (s *Store) inspectNames() error {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		info, err := e.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("%w: non-regular engine object %s", ErrCorrupt, name)
		}
		if name == "LOCK" || name == "CURRENT" {
			continue
		}
		if id, ok := parseID(name, "DATA", s.Current.UUID); ok {
			s.nextID = max(s.nextID, id)
		} else if id, ok := parseID(name, "MANIFEST", s.Current.UUID); ok {
			s.nextID = max(s.nextID, id)
		} else if !s.ownedTemp(name) {
			return fmt.Errorf("%w: unknown engine object %s", ErrCorrupt, name)
		}
	}
	return nil
}

func (s *Store) ownedTemp(name string) bool {
	return strings.HasPrefix(name, ".tarp-current-"+hex.EncodeToString(s.Current.UUID[:])+"-") && strings.HasSuffix(name, ".tmp")
}

// Cleanup only runs under the writer gate; release pins never performs I/O.
func (s *Store) Cleanup() (removed, pinned int64, result error) {
	if err := s.inspectNames(); err != nil {
		return 0, 0, err
	}
	selected := map[uint64]bool{s.Current.ActiveID: true}
	for _, f := range s.Catalog.Files {
		selected[f.ID] = true
	}
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return 0, 0, err
	}
	changed := false
	for _, e := range entries {
		name := e.Name()
		remove := s.ownedTemp(name)
		var removedID, manifestID uint64
		if id, ok := parseID(name, "MANIFEST", s.Current.UUID); ok {
			remove = id != s.Current.ManifestID
			manifestID = id
		}
		if id, ok := parseID(name, "DATA", s.Current.UUID); ok && !selected[id] {
			if f := s.Files[id]; f != nil && f.refs.Load() != 0 {
				if s.retired == nil {
					s.retired = make(map[uint64]*File)
				}
				s.retired[id] = f
				pinned += f.Size.Load()
				continue
			}
			if f := s.Files[id]; f != nil {
				if err := s.readers.forget(f); err != nil {
					return removed, pinned, err
				}
			}
			remove = true
			removedID = id
		}
		if remove {
			st, err := e.Info()
			if err != nil {
				return removed, pinned, err
			}
			unlinked := false
			unlinkErr := s.IO.Do("cleanup.unlink", func() error {
				err := os.Remove(filepath.Join(s.Dir, name))
				unlinked = err == nil
				return err
			})
			if unlinked {
				removed += st.Size()
				changed = true
				if removedID != 0 {
					delete(s.Files, removedID)
					delete(s.retired, removedID)
				}
			}
			if unlinked && manifestID != 0 {
				delete(s.retiredCatalogs, manifestID)
			}
			if unlinkErr != nil {
				return removed, pinned, unlinkErr
			}
		}
	}
	if changed {
		result = s.IO.SyncDir(s.Dir, "cleanup.dirsync")
	}
	return
}

func (s *Store) Close() error {
	var err error
	if s.readers != nil {
		err = s.readers.close()
	}
	if s.active != nil {
		before := s.IO.Check("close.active:before")
		closeErr := s.active.Close()
		after := s.IO.Check("close.active:after")
		err = errors.Join(err, before, closeErr, after)
		s.active = nil
	}
	if s.lock != nil {
		err = errors.Join(err, s.lock.Close())
		s.lock = nil
	}
	return err
}
