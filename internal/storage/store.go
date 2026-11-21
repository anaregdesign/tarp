package storage

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/publication"
)

var (
	ErrCorrupt         = format.ErrCorrupt
	ErrBackupDirectory = errors.New("tarp: directory contains BACKUP, use Restore to create a database")
)

type Config struct {
	ValueCacheBudgetBytes, RecordBytes, TargetSegmentBytes int64
	Handles                                                int
	IndexedOpen                                            bool
}

// RequiredLimits records the measured minima for a database image's live records.
type RequiredLimits struct {
	ValueCacheBudgetBytes, RecordBytes int64
}

// CheckLimits compares normalized settings with the live record requirements.
func (cfg Config) CheckLimits(required RequiredLimits) error {
	if cfg.ValueCacheBudgetBytes < required.ValueCacheBudgetBytes || cfg.RecordBytes < required.RecordBytes {
		return &CompatibilityError{required}
	}
	return nil
}

type CompatibilityError struct {
	RequiredLimits
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("storage: require value cache budget >= %d bytes, max record size >= %d bytes", e.ValueCacheBudgetBytes, e.RecordBytes)
}

type File struct {
	// Published once the catalog selects this immutable DATA. Readers that
	// already pinned the file can still use its binding after compaction.
	sealed atomic.Pointer[sealedRepair]
	// Only the serialized parity builder reads or writes these flags.
	parityReady, parityNeedsSync bool
	activeDigest                 hash.Hash // writer gate owns the committed active byte stream

	ID                     uint64
	Kind                   uint32
	Path                   string
	Size                   atomic.Int64
	refs                   atomic.Int64
	LiveBytes, DeleteBytes atomic.Int64
	LiveKeys               atomic.Int64 // index ownership independent of transient read and capture pins
}

func (f *File) Pin()              { f.refs.Add(1) }
func (f *File) References() int64 { return f.refs.Load() + f.LiveKeys.Load() }

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
	Dir                   string
	Config                Config
	IO                    *IO
	Current               format.Frontier
	Catalog               format.Catalog
	Manifest              []byte
	Files                 map[uint64]*File
	active                *os.File
	lock                  *platform.DirectoryLock
	readers               *handles
	nextID                uint64
	retired               map[uint64]*File
	retiredCatalogs       map[uint64]struct{}
	retiredIndexes        map[uint64]struct{}
	buildingIndex         atomic.Uint64
	retireSyncPending     bool
	cleanWorkspaceOnClose bool
	// Created reports that this Open published the initial CURRENT.
	Created bool
	// IndexCheckpointHit reports use of the selected index at Open.
	IndexCheckpointHit bool
	// MaxValueBytes is the largest live value at indexed Open.
	MaxValueBytes int64
}

func DataName(uuid [16]byte, id uint64) string {
	return fmt.Sprintf("DATA-%x-%016x", uuid, id)
}

func parityName(uuid [16]byte, id uint64) string {
	return fmt.Sprintf("PARITY-%x-%016x", uuid, id)
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

func Open(dir string, cfg Config, fs *IO) (*Store, map[string]Location, error) {
	return OpenContext(context.Background(), dir, cfg, false, fs)
}

func OpenContext(ctx context.Context, dir string, cfg Config, existingOnly bool, fs *IO) (*Store, map[string]Location, error) {
	s, legacy, _, err := openStore(ctx, dir, cfg, existingOnly, fs, nil, nil)
	return s, legacy, err
}

// OpenIndexed constructs the backend under the directory lock. On success
// the caller owns both objects and must close the backend before the store.
func OpenIndexed(ctx context.Context, dir string, cfg Config, existingOnly bool, fs *IO, newIndex func(*Store) (index.Backend, error), load IndexImporter) (*Store, index.Backend, error) {
	s, _, keys, err := openStore(ctx, dir, cfg, existingOnly, fs, newIndex, load)
	return s, keys, err
}

func lockStore(ctx context.Context, dir string, cfg Config, existingOnly bool, fs *IO) (_ *Store, result error) {
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if !existingOnly {
		if err := os.Mkdir(absolute, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	// Restored databases and backups both have CURRENT but no LOCK. Reject a
	// foreign format or backup before creating LOCK. Validate the DB under lock.
	if _, err := os.Lstat(filepath.Join(absolute, "LOCK")); errors.Is(err, os.ErrNotExist) {
		b, err := readCurrent(filepath.Join(absolute, "CURRENT"))
		if err == nil {
			if _, err := format.DecodeFrontier(b); err != nil {
				return nil, fmt.Errorf("CURRENT: %w", err)
			}
			if _, err := os.Lstat(filepath.Join(absolute, "BACKUP")); err == nil {
				return nil, ErrBackupDirectory
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		} else if existingOnly {
			if err := checkUninitializedDir(absolute); err != nil {
				return nil, err
			}
		}
	} else if err != nil {
		return nil, err
	}
	lock, err := platform.Lock(absolute)
	if err != nil {
		return nil, err
	}
	s := &Store{Dir: absolute, Config: cfg, IO: fs, lock: lock, readers: newHandles(cfg.Handles), Files: make(map[uint64]*File)}
	defer func() {
		if result != nil {
			result = errors.Join(result, s.Close())
		}
	}()
	if err := fs.Do("lock.sync", lock.Sync); err != nil {
		return nil, err
	}
	if err := fs.SyncDir(absolute, "lock.dirsync"); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s, nil
}

func openStore(ctx context.Context, dir string, cfg Config, existingOnly bool, fs *IO, newIndex func(*Store) (index.Backend, error), load IndexImporter) (_ *Store, _ map[string]Location, _ index.Backend, result error) {
	s, err := lockStore(ctx, dir, cfg, existingOnly, fs)
	if err != nil {
		return nil, nil, nil, err
	}
	var keys index.Backend
	defer func() {
		if result != nil {
			if keys != nil {
				result = errors.Join(result, keys.Close())
			}
			result = errors.Join(result, s.Close())
		}
	}()
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return nil, nil, nil, err
	}
	var image *Image
	fresh := len(entries) == 1 && entries[0].Name() == "LOCK"
	hadWorkspace := false
	if fresh {
		if existingOnly {
			return nil, nil, nil, &os.PathError{Op: "open", Path: filepath.Join(s.Dir, "CURRENT"), Err: os.ErrNotExist}
		}
		if err := fs.SyncDir(filepath.Dir(s.Dir), "init.parent.sync"); err != nil {
			return nil, nil, nil, err
		}
		if err := s.initialize(); err != nil {
			return nil, nil, nil, err
		}
		s.Created = true
	} else {
		image, err = readImage(ctx, s.Dir)
		if err != nil {
			return nil, nil, nil, err
		}
		if s.IO != nil {
			image.openRead = s.IO.OpenRead
		}
		s.Current = image.Current
		s.nextID = max(s.nextID, image.Catalog.Index.ID)
		hadWorkspace, err = s.inspectNames(ctx, entries)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	// Until the DATA and limits pass validation, remove only a workspace
	// created by this Open. An incompatible Open preserves prior remnants.
	s.cleanWorkspaceOnClose = !hadWorkspace
	if image != nil && image.Catalog.Index.ID != 0 && load != nil {
		keys, err = s.loadCheckpoint(ctx, image, load)
		if err != nil {
			return nil, nil, nil, err
		}
		s.IndexCheckpointHit = keys != nil
	}
	if keys == nil && newIndex != nil {
		keys, err = newIndex(s)
		if err != nil {
			outcome := publication.NotApplied
			if s.Created {
				outcome = publication.Committed
			}
			return nil, nil, nil, fault("index", outcome, false, err)
		}
	}
	if fresh {
		var legacy map[string]Location
		if keys == nil {
			legacy = make(map[string]Location)
		}
		return s, legacy, keys, nil
	}
	if err := s.recover(ctx, image, entries, keys, hadWorkspace); err != nil {
		return nil, nil, nil, err
	}
	return s, image.Index, keys, nil
}

func (s *Store) recover(ctx context.Context, image *Image, entries []os.DirEntry, keys index.Backend, hadWorkspace bool) error {
	var err error
	if s.IndexCheckpointHit {
		err = image.replayAfterCheckpoint(ctx, keys, s.Config.IndexedOpen)
	} else {
		err = image.replay(ctx, keys)
	}
	if err != nil {
		return err
	}
	if err := s.Config.CheckLimits(image.Required); err != nil {
		return err
	}
	if hadWorkspace {
		// Replay can leave the backend open and using this directory.
		if err := s.sweepWorkspace(false); err != nil {
			return err
		}
		s.cleanWorkspaceOnClose = true
	}
	s.Catalog, s.Manifest, s.Files = image.Catalog, image.Manifest, image.Files
	for _, entry := range s.Catalog.Files {
		if entry.HasPhysicalFile() {
			s.Files[entry.ID].markSealed(s.Catalog.UUID, entry)
		}
	}
	s.MaxValueBytes = image.MaxValueBytes
	active, err := os.OpenFile(s.Files[s.Current.ActiveID].Path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	s.active = active
	if s.Files[s.Current.ActiveID].Size.Load() != s.Current.ActiveEnd {
		if err := s.IO.Do("recover.truncate", func() error { return s.active.Truncate(s.Current.ActiveEnd) }); err != nil {
			return err
		}
		if err := s.IO.Sync(s.active, "recover.sync"); err != nil {
			return err
		}
		s.Files[s.Current.ActiveID].Size.Store(s.Current.ActiveEnd)
	}
	if _, _, err := s.cleanupValidated(ctx, entries); err != nil {
		return err
	}
	return ctx.Err()
}

// Neither LOCK nor CURRENT was observed before locking. Keep empty directories
// untouched and report partial images as corrupt. A concurrently created LOCK
// or CURRENT requires the usual authoritative check under the directory lock.
func checkUninitializedDir(dir string) error {
	names, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return &os.PathError{Op: "open", Path: filepath.Join(dir, "CURRENT"), Err: os.ErrNotExist}
	}
	for _, entry := range names {
		if entry.Name() == "LOCK" || entry.Name() == "CURRENT" {
			return nil
		}
	}
	return fmt.Errorf("%w: missing CURRENT in %s", ErrCorrupt, dir)
}

func (s *Store) initialize() error {
	if _, err := rand.Read(s.Current.UUID[:]); err != nil {
		return err
	}
	s.Catalog.UUID = s.Current.UUID
	file, fd, err := s.newLog(appendData{})
	if err != nil {
		return err
	}
	s.active = fd
	s.Catalog.ActiveID = file.ID
	candidate := s.Current
	candidate.Generation, candidate.ActiveID, candidate.ActiveEnd = 1, file.ID, format.SegmentHeaderBytes
	return s.publishCatalog(s.Catalog, candidate)
}

func (s *Store) newLog(initialRecords appendData) (*File, *os.File, error) {
	f, err := s.reserveSegment(format.Log)
	if err != nil {
		return nil, nil, err
	}
	if initialRecords.prepare != nil {
		if err := initialRecords.prepare(f, format.SegmentHeaderBytes); err != nil {
			// No file was created and the writer gate still owns the reservation.
			s.nextID--
			return nil, nil, &prepareError{err}
		}
	}
	fd, err := f.create(s.Current.UUID, s.IO, "segment", initialRecords)
	if err != nil {
		return nil, nil, err
	}
	s.Files[f.ID] = f
	return f, fd, nil
}

// reserveSegment allocates a unique identity under the writer gate without I/O.
func (s *Store) reserveSegment(kind uint32) (*File, error) {
	id, err := s.allocateID()
	if err != nil {
		return nil, err
	}
	return &File{ID: id, Kind: kind, Path: filepath.Join(s.Dir, DataName(s.Current.UUID, id))}, nil
}

func (f *File) create(uuid [16]byte, fs *IO, point string, initialRecords appendData) (*os.File, error) {
	fd, err := os.OpenFile(f.Path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	b := format.EncodeSegment(format.Segment{UUID: uuid, ID: f.ID, Kind: f.Kind})
	err = fs.Write(fd, b, 0, point+".write")
	if f.Kind == format.Log {
		f.activeDigest = sha256.New()
		f.activeDigest.Write(b)
	}
	if err == nil && initialRecords.size > 0 {
		err = initialRecords.writeAt(fd, format.SegmentHeaderBytes, f.activeDigest)
	}
	// Private checkpoints sync the complete file before publication.
	if err == nil && f.Kind != format.Snapshot {
		err = fs.Sync(fd, point+".sync")
	}
	if err != nil {
		closeErr := fd.Close()
		removeErr := fs.Do("segment.discard", func() error { return os.Remove(f.Path) })
		return nil, errors.Join(err, closeErr, removeErr)
	}
	f.Size.Store(format.SegmentHeaderBytes + initialRecords.size)
	return fd, nil
}

func (s *Store) Rotate() error {
	if s.Current.ActiveEnd == format.SegmentHeaderBytes {
		return nil
	}
	if s.Current.Generation == math.MaxUint64 {
		return fault("rotation.generation", publication.NotApplied, false, errors.New("storage: publication generation exhausted"))
	}
	return s.rotate(appendData{}, s.Current.Sequence, nil)
}

// A rotating append syncs the header and its initial records together. CURRENT
// selects the new segment, catalog and user sequence in one publication.
// Explicit checkpoint sealing selects a header-only log at the old sequence.
func (s *Store) rotate(initialRecords appendData, lastSequence uint64, healthy func() error) error {
	file, fd, err := s.newLog(initialRecords)
	if err != nil {
		var prepare *prepareError
		if errors.As(err, &prepare) {
			return prepare.err
		}
		return fault("rotation.segment", publication.NotApplied, true, err)
	}
	if healthy != nil {
		if err := healthy(); err != nil {
			return errors.Join(fault("commit.health", publication.NotApplied, true, err), fd.Close())
		}
	}
	catalog := s.Catalog
	catalog.Files = append(append([]format.File(nil), catalog.Files...), format.File{
		ID: s.Current.ActiveID, Bytes: s.Current.ActiveEnd, Kind: format.Log,
		First: s.Catalog.SealedEnd() + 1, Last: s.Current.Sequence,
		Digest: [sha256.Size]byte(s.Files[s.Current.ActiveID].activeDigest.Sum(nil)),
	})
	catalog.ActiveID = file.ID
	candidate := s.Current
	candidate.Generation++
	candidate.ActiveID, candidate.ActiveEnd = file.ID, file.Size.Load()
	candidate.Sequence = lastSequence
	if err := s.publishCatalog(catalog, candidate); err != nil {
		return errors.Join(err, fd.Close())
	}
	sealed := catalog.Files[len(catalog.Files)-1]
	prior := s.Files[sealed.ID]
	prior.activeDigest = nil
	prior.markSealed(catalog.UUID, sealed)
	old := s.active
	s.active = fd
	if err := errors.Join(old.Close(), s.IO.Check("rotation.close:after")); err != nil {
		return fault("rotation.close", publication.Committed, true, err)
	}
	return nil
}

// Entry holds caller-owned bytes for an append.
type Entry struct {
	Key, Value []byte
}

func (s *Store) checkSequence(count int) error {
	if s.Current.Sequence > math.MaxUint64-uint64(count) || s.Current.Generation == math.MaxUint64 {
		return fault("commit.sequence", publication.NotApplied, false, errors.New("storage: sequence or publication generation exhausted"))
	}
	return nil
}

func (s *Store) Append(kind uint8, key, value []byte, healthy func() error) (Location, error) {
	return s.AppendPrepared(kind, key, value, healthy, nil)
}

// AppendPrepared supplies the final record location before the first DATA write.
func (s *Store) AppendPrepared(kind uint8, key, value []byte, healthy func() error, prepare func(Location) error) (Location, error) {
	if err := s.checkSequence(1); err != nil {
		return Location{}, err
	}
	sequence := s.Current.Sequence + 1
	b := format.EncodeRecord(kind, sequence, key, value)
	data := appendData{size: int64(len(b)), writeAt: func(fd *os.File, off int64, digest hash.Hash) error {
		if err := s.IO.Write(fd, b, off, "data.write"); err != nil {
			return err
		}
		digest.Write(b)
		return nil
	}}
	if prepare != nil {
		data.prepare = func(f *File, off int64) error {
			return prepare(Location{File: f, Offset: off, Bytes: int64(len(b)), ValueBytes: int64(len(value)), Sequence: sequence})
		}
	}
	file, offset, err := s.appendRecords(data, sequence, healthy)
	if err != nil {
		return Location{}, err
	}
	return Location{File: file, Offset: offset, Bytes: int64(len(b)), ValueBytes: int64(len(value)), Sequence: sequence}, nil
}

// AppendBatch publishes all records at one frontier. The caller validates the
// entries and their combined size against a single segment before admission.
func (s *Store) AppendBatch(kind uint8, entries []Entry, healthy func() error) ([]Location, error) {
	return s.AppendBatchPrepared(kind, entries, healthy, nil)
}

// AppendBatchPrepared stages index changes at the same locations used by the append.
func (s *Store) AppendBatchPrepared(kind uint8, entries []Entry, healthy func() error, prepare func(int, Location) error) ([]Location, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	if err := s.checkSequence(len(entries)); err != nil {
		return nil, err
	}
	locations := make([]Location, len(entries))
	var size int64
	for i, e := range entries {
		n := entrysize.RecordBytes(int64(len(e.Key)), int64(len(e.Value)))
		locations[i] = Location{Offset: size, Bytes: n, ValueBytes: int64(len(e.Value)), Sequence: s.Current.Sequence + uint64(i) + 1}
		size += n
	}
	data := appendData{size: size, writeAt: func(fd *os.File, off int64, digest hash.Hash) error {
		// Buffer small records together; bufio writes large values directly from
		// the caller's slice. The buffer lives only through this append.
		dst := &outputWriter{fs: s.IO, fd: fd, offset: off, point: "data.write", hash: digest}
		w := bufio.NewWriterSize(dst, int(min(size, 32<<10)))
		var header [format.RecordHeaderBytes]byte
		for i, e := range entries {
			format.EncodeRecordHeader(&header, kind, locations[i].Sequence, e.Key, e.Value)
			for _, part := range [3][]byte{header[:], e.Key, e.Value} {
				if _, err := w.Write(part); err != nil {
					return err
				}
			}
		}
		return w.Flush()
	}}
	if prepare != nil {
		data.prepare = func(f *File, off int64) error {
			for i, loc := range locations {
				loc.File, loc.Offset = f, off+loc.Offset
				if err := prepare(i, loc); err != nil {
					return err
				}
			}
			return nil
		}
	}
	file, offset, err := s.appendRecords(data, locations[len(locations)-1].Sequence, healthy)
	if err != nil {
		return nil, err
	}
	for i := range locations {
		locations[i].File = file
		locations[i].Offset += offset
	}
	return locations, nil
}

type prepareError struct{ err error }

func (e *prepareError) Error() string { return e.err.Error() }
func (e *prepareError) Unwrap() error { return e.err }

// appendData is written synchronously to either the current or a new segment.
type appendData struct {
	prepare func(*File, int64) error
	size    int64
	writeAt func(*os.File, int64, hash.Hash) error
}

func (s *Store) appendRecords(data appendData, lastSequence uint64, healthy func() error) (*File, int64, error) {
	offset := s.Current.ActiveEnd
	if offset > format.SegmentHeaderBytes && offset > s.Config.TargetSegmentBytes-data.size {
		if err := s.rotate(data, lastSequence, healthy); err != nil {
			return nil, 0, err
		}
		return s.Files[s.Current.ActiveID], format.SegmentHeaderBytes, nil
	}
	if data.prepare != nil {
		if err := data.prepare(s.Files[s.Current.ActiveID], offset); err != nil {
			return nil, 0, err
		}
	}
	if err := data.writeAt(s.active, offset, s.Files[s.Current.ActiveID].activeDigest); err != nil {
		return nil, 0, fault("data.write", publication.NotApplied, true, err)
	}
	if err := s.IO.Sync(s.active, "data.sync"); err != nil {
		return nil, 0, fault("data.sync", publication.NotApplied, true, err)
	}
	if err := healthy(); err != nil {
		return nil, 0, fault("commit.health", publication.NotApplied, true, err)
	}
	candidate := s.Current
	candidate.Generation++
	candidate.Sequence, candidate.ActiveEnd = lastSequence, offset+data.size
	if err := s.publish(candidate); err != nil {
		return nil, 0, err
	}
	file := s.Files[s.Current.ActiveID]
	file.Size.Store(candidate.ActiveEnd)
	return file, offset, nil
}

func (s *Store) Read(loc Location, key string) ([]byte, error) {
	return s.ReadContext(context.Background(), loc, key)
}

func (s *Store) ReadContext(ctx context.Context, loc Location, key string) ([]byte, error) {
	if err := s.IO.Check("read:before"); err != nil {
		return nil, loc.readError(err)
	}
	value, err := s.readValue(loc, key)
	if err != nil {
		if err = s.RepairRead(ctx, loc.File, err); err != nil {
			if err == ctx.Err() {
				return nil, err
			}
			return nil, loc.readError(err)
		}
		value, err = s.readValue(loc, key)
		if err != nil {
			return nil, loc.readError(err)
		}
	}
	if err := s.IO.Check("read:after"); err != nil {
		return value, loc.readError(err)
	}
	return value, nil
}

func (s *Store) readValue(loc Location, key string) ([]byte, error) {
	h, err := s.readers.acquire(loc.File)
	if err != nil {
		return nil, err
	}
	defer s.readers.release(h)
	return format.ReadValue(h.fd, loc.Offset, loc.Bytes, key, loc.Sequence)
}

func (loc Location) readError(err error) error {
	return fmt.Errorf("%s: record at offset %d: %w", filepath.Base(loc.File.Path), loc.Offset, err)
}

func (s *Store) inspectNames(ctx context.Context, entries []os.DirEntry) (bool, error) {
	hasWorkspace := false
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		name := e.Name()
		if name == workspaceName && e.IsDir() {
			hasWorkspace = true
			continue
		}
		if !e.Type().IsRegular() {
			return false, fmt.Errorf("%w: non-regular engine object %s", ErrCorrupt, name)
		}
		if name == "LOCK" || name == "CURRENT" {
			continue
		}
		if id, ok := parseID(name, "DATA", s.Current.UUID); ok {
			s.nextID = max(s.nextID, id)
		} else if id, ok := parseID(name, "MANIFEST", s.Current.UUID); ok {
			s.nextID = max(s.nextID, id)
		} else if id, ok := parseID(name, "INDEX", s.Current.UUID); ok {
			s.nextID = max(s.nextID, id)
		} else if id, ok := parseID(name, "PARITY", s.Current.UUID); ok {
			s.nextID = max(s.nextID, id)
		} else if !s.ownedTemp(name) {
			return false, fmt.Errorf("%w: unknown engine object %s", ErrCorrupt, name)
		}
	}
	return hasWorkspace, nil
}

func (s *Store) ownedTemp(name string) bool {
	prefix := ".tarp-current-" + hex.EncodeToString(s.Current.UUID[:]) + "-"
	suffix, ok := strings.CutPrefix(name, prefix)
	return ok && len(suffix) > len(".tmp") && strings.HasSuffix(suffix, ".tmp")
}

// Cleanup only runs under the writer gate; release pins never performs I/O.
func (s *Store) Cleanup() (removed, pinned int64, result error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return 0, 0, err
	}
	ctx := context.Background()
	if _, err := s.inspectNames(ctx, entries); err != nil {
		return 0, 0, err
	}
	return s.cleanupValidated(ctx, entries)
}

// The caller validates the entire listing before truncating or unlinking any file.
func (s *Store) cleanupValidated(ctx context.Context, entries []os.DirEntry) (removed, pinned int64, result error) {
	selected := map[uint64]bool{s.Current.ActiveID: true}
	for _, f := range s.Catalog.Files {
		if f.HasPhysicalFile() {
			selected[f.ID] = true
		}
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return removed, pinned, errors.Join(err, s.syncRetirements("cleanup.dirsync"))
		}
		name := e.Name()
		remove := s.ownedTemp(name)
		var removedID, manifestID, indexID uint64
		if id, ok := parseID(name, "INDEX", s.Current.UUID); ok {
			remove = id != s.Catalog.Index.ID && id != s.buildingIndex.Load()
			indexID = id
		}
		if id, ok := parseID(name, "MANIFEST", s.Current.UUID); ok {
			remove = id != s.Current.ManifestID
			manifestID = id
		}
		if id, ok := parseID(name, "PARITY", s.Current.UUID); ok {
			remove = id == s.Current.ActiveID || !selected[id]
			if f := s.Files[id]; f != nil && f.References() != 0 && !selected[id] {
				// A parity publisher may still own its source pin.
				remove = false
			}
		}
		if id, ok := parseID(name, "DATA", s.Current.UUID); ok && !selected[id] {
			if f := s.Files[id]; f != nil && f.References() != 0 {
				s.retireData(f)
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
				s.retireSyncPending = true
				if removedID != 0 {
					delete(s.Files, removedID)
					delete(s.retired, removedID)
				}
			}
			if unlinked && indexID != 0 {
				delete(s.retiredIndexes, indexID)
			}
			if unlinked && manifestID != 0 {
				delete(s.retiredCatalogs, manifestID)
			}
			if unlinkErr != nil {
				return removed, pinned, unlinkErr
			}
		}
	}
	return removed, pinned, s.syncRetirements("cleanup.dirsync")
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
		if s.cleanWorkspaceOnClose {
			err = errors.Join(err, s.cleanWorkspace())
		}
		err = errors.Join(err, s.lock.Close())
		s.lock = nil
	}
	return err
}
