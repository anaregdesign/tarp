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
	ValueCacheBudgetBytes, MaxRecordBytes, TargetSegmentBytes int64
	Handles                                                   int
	IndexedOpen                                               bool
}

// LiveRecordSizes measures the largest encoded record and value-cache charge among live keys.
// Both measurements are zero for an empty image.
type LiveRecordSizes struct {
	LargestLiveCacheChargeBytes, LargestLiveRecordBytes int64
}

// CheckLimits compares normalized settings with the live record requirements.
func (cfg Config) CheckLimits(sizes LiveRecordSizes) error {
	if cfg.ValueCacheBudgetBytes < sizes.LargestLiveCacheChargeBytes || cfg.MaxRecordBytes < sizes.LargestLiveRecordBytes {
		return &CompatibilityError{
			RequiredValueCacheBudgetBytes: sizes.LargestLiveCacheChargeBytes,
			RequiredMaxRecordBytes:        sizes.LargestLiveRecordBytes,
		}
	}
	return nil
}

type CompatibilityError struct {
	RequiredValueCacheBudgetBytes, RequiredMaxRecordBytes int64
}

func (e *CompatibilityError) Error() string {
	return fmt.Sprintf("storage: require value cache budget >= %d bytes, max record size >= %d bytes", e.RequiredValueCacheBudgetBytes, e.RequiredMaxRecordBytes)
}

// DataSegment tracks the identity, ownership and accounting of one physical DATA
// segment in this process. Read handles for its path are managed separately.
type DataSegment struct {
	// Published once the catalog selects this immutable DATA. Readers that
	// already pinned the file can still use its binding after compaction.
	sealed atomic.Pointer[sealedRepair]
	// The serialized parity builder owns readiness. Cleaner ranking can
	// observe it without blocking that builder.
	parityReady     atomic.Bool
	parityNeedsSync bool
	snapshotRange   *snapshotRange // checked bounds for an immutable snapshot; maintained by the maintenance slot
	activeDigest    hash.Hash      // writer gate owns the committed active byte stream

	ID                     uint64
	Kind                   uint32
	Path                   string
	Size                   atomic.Int64
	pins                   atomic.Int64
	retainingPins          atomic.Int64
	LiveBytes, DeleteBytes atomic.Int64
	LiveKeys               atomic.Int64 // index ownership independent of transient read and capture pins
}

func (f *DataSegment) Pin() { f.pins.Add(1) }

// References counts transient segment pins and live index entries pointing to this segment.
func (f *DataSegment) References() int64 { return f.pins.Load() + f.LiveKeys.Load() }

func (f *DataSegment) Unpin() {
	if f.pins.Add(-1) < 0 {
		panic("storage: negative segment pin count")
	}
}

// PinRetained is for captured DATA or value scans, which can outlive a clean
// plan. All pins still prevent unlinks; only these affect ranking.
func (f *DataSegment) PinRetained() {
	f.Pin()
	f.retainingPins.Add(1)
}

func (f *DataSegment) UnpinRetained() {
	if f.retainingPins.Add(-1) < 0 {
		panic("storage: negative retained segment pin count")
	}
	f.Unpin()
}

func (f *DataSegment) HasRetentionPin() bool { return f.retainingPins.Load() > 0 }
func (f *DataSegment) ParityReady() bool     { return f.parityReady.Load() }

// Location binds a DATA record address to the process-local segment used for I/O.
type Location struct {
	Segment      *DataSegment
	RecordOffset int64
	RecordBytes  int64
	ValueBytes   int64
	Sequence     uint64
}

type Store struct {
	checkDamage atomic.Bool // Set by a read repair that discovers unavailable DATA.
	// DamageChanged wakes maintenance when known repair eligibility changes.
	// The owner sets it once before starting any workers or serving requests.
	DamageChanged         func()
	Dir                   string
	Config                Config
	IO                    *IO
	Current               format.Frontier
	Catalog               format.Catalog
	Manifest              []byte
	Segments              map[uint64]*DataSegment
	active                *os.File
	lock                  *platform.DirectoryLock
	readers               *handles
	nextID                uint64
	retired               map[uint64]*DataSegment
	retiredCatalogs       map[uint64]struct{}
	retiredIndexes        map[uint64]struct{}
	buildingIndex         atomic.Uint64
	retireSyncPending     bool
	orderedSnapshot       bool // current snapshot prefix is globally key ordered
	fences                fenceCache
	cleanWorkspaceOnClose bool
	// Created reports that this Open published the initial CURRENT.
	Created bool
	// IndexCheckpointValidated records a successful import or publication. A
	// reusable checkpoint must also match the current catalog and user sequence.
	IndexCheckpointValidated bool
	// LargestLiveValueBytes is the largest live value at indexed Open.
	LargestLiveValueBytes int64
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
	kind, rest, ok := strings.Cut(name, "-")
	if !ok || kind != prefix {
		return 0, false
	}
	return parseObjectID(rest, hex.EncodeToString(uuid[:])+"-")
}

func parseObjectID(rest, uuidPrefix string) (uint64, bool) {
	digits, ok := strings.CutPrefix(rest, uuidPrefix)
	if !ok || len(digits) != 16 || strings.ContainsAny(digits, "ABCDEF") {
		return 0, false
	}
	id, err := strconv.ParseUint(digits, 16, 64)
	return id, err == nil && id != 0
}

func (s *Store) allocateID() (uint64, error) {
	if s.nextID == math.MaxUint64 {
		return 0, errors.New("storage: file identity exhausted")
	}
	s.nextID++
	return s.nextID, nil
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
	// A backup can have missing or damaged files. Reject its marker before
	// creating LOCK, preserving the format error for a foreign CURRENT.
	if _, err := os.Lstat(filepath.Join(absolute, "BACKUP")); err == nil {
		b, err := readCurrent(filepath.Join(absolute, "CURRENT"))
		if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, ErrCorrupt) {
			return nil, err
		}
		if errors.Is(format.CheckFrontierSignature(b), format.ErrUnsupported) {
			return nil, fmt.Errorf("CURRENT: %w", format.ErrUnsupported)
		}
		return nil, ErrBackupDirectory
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// Restored databases have CURRENT but no LOCK. Reject an unrecognized
	// frontier before creating LOCK. Validate the selected DB under lock.
	if _, err := os.Lstat(filepath.Join(absolute, "LOCK")); errors.Is(err, os.ErrNotExist) {
		b, err := readCurrent(filepath.Join(absolute, "CURRENT"))
		if err == nil {
			if _, err := format.DecodeFrontier(b); err != nil {
				return nil, fmt.Errorf("CURRENT: %w", err)
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
	s := &Store{Dir: absolute, Config: cfg, IO: fs, lock: lock, readers: newHandles(cfg.Handles), Segments: make(map[uint64]*DataSegment)}
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

// OpenIndexed constructs the backend under the directory lock. On success
// the caller owns both objects and must close the backend before the store.
func OpenIndexed(ctx context.Context, dir string, cfg Config, existingOnly bool, fs *IO, newIndex func(*Store) (index.Backend, error), load IndexImporter) (_ *Store, _ index.Backend, result error) {
	s, err := lockStore(ctx, dir, cfg, existingOnly, fs)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	var image *Image
	fresh := len(entries) == 1 && entries[0].Name() == "LOCK"
	var listing directoryListing
	if fresh {
		if existingOnly {
			return nil, nil, &os.PathError{Op: "open", Path: filepath.Join(s.Dir, "CURRENT"), Err: os.ErrNotExist}
		}
		if err := fs.SyncDir(filepath.Dir(s.Dir), "init.parent.sync"); err != nil {
			return nil, nil, err
		}
		if err := s.initialize(); err != nil {
			return nil, nil, err
		}
		s.Created = true
	} else {
		image, err = readImage(ctx, s.Dir)
		if err != nil {
			return nil, nil, err
		}
		if s.IO != nil {
			image.openRead = s.IO.OpenRead
		}
		s.Current = image.Current
		s.nextID = max(s.nextID, image.Catalog.Index.ID)
		if err := s.inspectNames(ctx, entries, &listing); err != nil {
			return nil, nil, err
		}
	}
	// Until the DATA and limits pass validation, remove only a workspace
	// created by this Open. An incompatible Open preserves prior remnants.
	s.cleanWorkspaceOnClose = !listing.hasWorkspace
	if image != nil && image.Catalog.Index.ID != 0 && load != nil {
		keys, err = s.loadIndexCheckpoint(ctx, image, load)
		if err != nil {
			return nil, nil, err
		}
		s.IndexCheckpointValidated = keys != nil
	}
	if keys == nil {
		keys, err = newIndex(s)
		if err != nil {
			outcome := publication.NotApplied
			if s.Created {
				outcome = publication.Committed
			}
			return nil, nil, fault("index", outcome, false, err)
		}
	}
	if !fresh {
		if err := s.recover(ctx, image, listing, keys); err != nil {
			return nil, nil, err
		}
	}
	return s, keys, nil
}

func (s *Store) recover(ctx context.Context, image *Image, listing directoryListing, keys index.Backend) error {
	var err error
	var order *snapshotOrderCheck
	if s.IndexCheckpointValidated {
		err = image.replayAfterIndexCheckpoint(ctx, keys, s.Config.IndexedOpen)
	} else {
		if !s.Config.IndexedOpen {
			order = &snapshotOrderCheck{fences: &s.fences}
		}
		err = image.rebuildIndex(ctx, keys, order)
	}
	if err != nil {
		return err
	}
	if err := s.Config.CheckLimits(image.LiveRecordSizes); err != nil {
		return err
	}
	if listing.hasWorkspace {
		// Replay can leave the backend open and using this directory.
		if err := s.sweepWorkspace(false); err != nil {
			return err
		}
		s.cleanWorkspaceOnClose = true
	}
	s.Catalog, s.Manifest, s.Segments = image.Catalog, image.Manifest, image.Segments
	for _, entry := range s.Catalog.Entries {
		if entry.HasPhysicalFile() {
			s.Segments[entry.ID].markSealed(s.Catalog.UUID, entry)
		}
	}
	s.LargestLiveValueBytes = image.LargestLiveValueBytes
	active, err := os.OpenFile(s.Segments[s.Current.ActiveID].Path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	s.active = active
	if s.Segments[s.Current.ActiveID].Size.Load() != s.Current.ActiveCommittedEndOffset {
		if err := s.IO.Do("recover.truncate", func() error { return s.active.Truncate(s.Current.ActiveCommittedEndOffset) }); err != nil {
			return err
		}
		if err := s.IO.Sync(s.active, "recover.sync"); err != nil {
			return err
		}
		s.Segments[s.Current.ActiveID].Size.Store(s.Current.ActiveCommittedEndOffset)
	}
	if _, _, err := s.cleanupValidated(ctx, listing.objects); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Only the record replay path saw encoded keys in snapshot order. A
	// digest-only verification of the INDEX-covered prefix is not a proof.
	s.orderedSnapshot = order != nil && !order.unordered
	return nil
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
	segment, fd, err := s.newLog(appendData{})
	if err != nil {
		return err
	}
	s.active = fd
	s.Catalog.ActiveID = segment.ID
	candidate := s.Current
	candidate.PublicationGeneration, candidate.ActiveID, candidate.ActiveCommittedEndOffset = 1, segment.ID, format.SegmentHeaderBytes
	return s.publishCatalog(s.Catalog, candidate)
}

func (s *Store) newLog(initialRecords appendData) (*DataSegment, *os.File, error) {
	f, err := s.reserveSegment(format.RawLogKind)
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
	s.Segments[f.ID] = f
	return f, fd, nil
}

// reserveSegment allocates a unique identity under the writer gate without I/O.
func (s *Store) reserveSegment(kind uint32) (*DataSegment, error) {
	id, err := s.allocateID()
	if err != nil {
		return nil, err
	}
	return &DataSegment{ID: id, Kind: kind, Path: filepath.Join(s.Dir, DataName(s.Current.UUID, id))}, nil
}

func (f *DataSegment) create(uuid [16]byte, fs *IO, point string, initialRecords appendData) (*os.File, error) {
	fd, err := os.OpenFile(f.Path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := f.initializeFile(fd, uuid, fs, point, initialRecords); err != nil {
		closeErr := fd.Close()
		removeErr := fs.Do("segment.discard", func() error { return os.Remove(f.Path) })
		return nil, errors.Join(err, closeErr, removeErr)
	}
	return fd, nil
}

// initializeFile leaves ownership of the exclusively created file with the
// caller even when the first write fails.
func (f *DataSegment) initializeFile(fd *os.File, uuid [16]byte, fs *IO, point string, initialRecords appendData) error {
	b := format.EncodeSegmentHeader(format.SegmentHeader{UUID: uuid, ID: f.ID, Kind: f.Kind})
	err := fs.Write(fd, b, 0, point+".write")
	if f.Kind == format.RawLogKind {
		f.activeDigest = sha256.New()
		f.activeDigest.Write(b)
	}
	if err == nil && initialRecords.size > 0 {
		err = initialRecords.writeAt(fd, format.SegmentHeaderBytes, f.activeDigest)
	}
	// Private checkpoints sync the complete file before publication.
	if err == nil && f.Kind != format.SnapshotKind {
		err = fs.Sync(fd, point+".sync")
	}
	if err == nil {
		f.Size.Store(format.SegmentHeaderBytes + initialRecords.size)
	}
	return err
}

func (s *Store) Rotate() error {
	if s.Current.ActiveCommittedEndOffset == format.SegmentHeaderBytes {
		return nil
	}
	if s.Current.PublicationGeneration == math.MaxUint64 {
		return fault("rotation.generation", publication.NotApplied, false, errors.New("storage: publication generation exhausted"))
	}
	return s.rotate(appendData{}, s.Current.LastUserSequence, nil)
}

// A rotating append syncs the header and its initial records together. CURRENT
// selects the new segment, catalog and user sequence in one publication.
// Explicit checkpoint sealing selects a header-only log at the old sequence.
func (s *Store) rotate(initialRecords appendData, lastSequence uint64, healthy func() error) error {
	segment, fd, err := s.newLog(initialRecords)
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
	catalog.Entries = append(append([]format.CatalogEntry(nil), catalog.Entries...), format.CatalogEntry{
		ID: s.Current.ActiveID, Bytes: s.Current.ActiveCommittedEndOffset, Kind: format.RawLogKind,
		FirstSequence: s.Catalog.SealedThroughSequence() + 1, LastSequence: s.Current.LastUserSequence,
		DataFileDigest: [sha256.Size]byte(s.Segments[s.Current.ActiveID].activeDigest.Sum(nil)),
	})
	catalog.ActiveID = segment.ID
	candidate := s.Current
	candidate.PublicationGeneration++
	candidate.ActiveID, candidate.ActiveCommittedEndOffset = segment.ID, segment.Size.Load()
	candidate.LastUserSequence = lastSequence
	if err := s.publishCatalog(catalog, candidate); err != nil {
		return errors.Join(err, fd.Close())
	}
	sealed := catalog.Entries[len(catalog.Entries)-1]
	prior := s.Segments[sealed.ID]
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
	if s.Current.LastUserSequence > math.MaxUint64-uint64(count) || s.Current.PublicationGeneration == math.MaxUint64 {
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
	sequence := s.Current.LastUserSequence + 1
	b := format.EncodeRecord(kind, sequence, key, value)
	data := appendData{size: int64(len(b)), writeAt: func(fd *os.File, off int64, digest hash.Hash) error {
		if err := s.IO.Write(fd, b, off, "data.write"); err != nil {
			return err
		}
		digest.Write(b)
		return nil
	}}
	if prepare != nil {
		data.prepare = func(f *DataSegment, off int64) error {
			return prepare(Location{Segment: f, RecordOffset: off, RecordBytes: int64(len(b)), ValueBytes: int64(len(value)), Sequence: sequence})
		}
	}
	segment, offset, err := s.appendRecords(data, sequence, healthy)
	if err != nil {
		return Location{}, err
	}
	return Location{Segment: segment, RecordOffset: offset, RecordBytes: int64(len(b)), ValueBytes: int64(len(value)), Sequence: sequence}, nil
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
		locations[i] = Location{RecordOffset: size, RecordBytes: n, ValueBytes: int64(len(e.Value)), Sequence: s.Current.LastUserSequence + uint64(i) + 1}
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
		data.prepare = func(f *DataSegment, off int64) error {
			for i, loc := range locations {
				loc.Segment, loc.RecordOffset = f, off+loc.RecordOffset
				if err := prepare(i, loc); err != nil {
					return err
				}
			}
			return nil
		}
	}
	segment, offset, err := s.appendRecords(data, locations[len(locations)-1].Sequence, healthy)
	if err != nil {
		return nil, err
	}
	for i := range locations {
		locations[i].Segment = segment
		locations[i].RecordOffset += offset
	}
	return locations, nil
}

type prepareError struct{ err error }

func (e *prepareError) Error() string { return e.err.Error() }
func (e *prepareError) Unwrap() error { return e.err }

// appendData is written synchronously to either the current or a new segment.
type appendData struct {
	prepare func(*DataSegment, int64) error
	size    int64
	writeAt func(*os.File, int64, hash.Hash) error
}

func (s *Store) appendRecords(data appendData, lastSequence uint64, healthy func() error) (*DataSegment, int64, error) {
	offset := s.Current.ActiveCommittedEndOffset
	if offset > format.SegmentHeaderBytes && offset > s.Config.TargetSegmentBytes-data.size {
		if err := s.rotate(data, lastSequence, healthy); err != nil {
			return nil, 0, err
		}
		return s.Segments[s.Current.ActiveID], format.SegmentHeaderBytes, nil
	}
	if data.prepare != nil {
		if err := data.prepare(s.Segments[s.Current.ActiveID], offset); err != nil {
			return nil, 0, err
		}
	}
	if err := data.writeAt(s.active, offset, s.Segments[s.Current.ActiveID].activeDigest); err != nil {
		return nil, 0, fault("data.write", publication.NotApplied, true, err)
	}
	if err := s.IO.Sync(s.active, "data.sync"); err != nil {
		return nil, 0, fault("data.sync", publication.NotApplied, true, err)
	}
	if err := healthy(); err != nil {
		return nil, 0, fault("commit.health", publication.NotApplied, true, err)
	}
	candidate := s.Current
	candidate.PublicationGeneration++
	candidate.LastUserSequence, candidate.ActiveCommittedEndOffset = lastSequence, offset+data.size
	if err := s.publish(candidate); err != nil {
		return nil, 0, err
	}
	segment := s.Segments[s.Current.ActiveID]
	segment.Size.Store(candidate.ActiveCommittedEndOffset)
	return segment, offset, nil
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
		if err = s.RepairRead(ctx, loc.Segment, err); err != nil {
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
	h, err := s.readers.acquire(loc.Segment)
	if err != nil {
		return nil, err
	}
	defer s.readers.release(h)
	value, err := format.ReadValue(h.fd, loc.RecordOffset, loc.RecordBytes, key, loc.Sequence)
	if err != nil {
		return nil, err
	}
	if err := h.verifyReadIdentity(); err != nil {
		return nil, err
	}
	return value, nil
}

func (loc Location) readError(err error) error {
	return fmt.Errorf("%s: record at offset %d: %w", filepath.Base(loc.Segment.Path), loc.RecordOffset, err)
}

// directoryListing retains the type and ID of every object cleanup may retire.
// Current selection and pins are checked when cleanup actually runs.
type directoryListing struct {
	objects      []directoryObject
	hasWorkspace bool
}

type directoryObject struct {
	entry os.DirEntry
	kind  string
	id    uint64
}

// Offline repair only validates names and passes a nil listing.
func (s *Store) inspectNames(ctx context.Context, entries []os.DirEntry, listing *directoryListing) error {
	if listing != nil {
		listing.objects = make([]directoryObject, 0, len(entries))
	}
	uuidPrefix := hex.EncodeToString(s.Current.UUID[:]) + "-"
	tempPrefix := ".tarp-current-" + uuidPrefix
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := e.Name()
		if name == workspaceName && e.IsDir() {
			if listing != nil {
				listing.hasWorkspace = true
			}
			continue
		}
		if !e.Type().IsRegular() {
			return fmt.Errorf("%w: non-regular engine object %s", ErrCorrupt, name)
		}
		if name == "LOCK" || name == "CURRENT" {
			continue
		}
		kind, rest, _ := strings.Cut(name, "-")
		var id uint64
		switch kind {
		case "DATA", "MANIFEST", "INDEX", "PARITY":
			var ok bool
			id, ok = parseObjectID(rest, uuidPrefix)
			if !ok {
				return fmt.Errorf("%w: unknown engine object %s", ErrCorrupt, name)
			}
			s.nextID = max(s.nextID, id)
		default:
			suffix, ok := strings.CutPrefix(name, tempPrefix)
			if !ok || len(suffix) <= len(".tmp") || !strings.HasSuffix(suffix, ".tmp") {
				return fmt.Errorf("%w: unknown engine object %s", ErrCorrupt, name)
			}
			kind = ".tarp-current"
		}
		if listing != nil {
			listing.objects = append(listing.objects, directoryObject{entry: e, kind: kind, id: id})
		}
	}
	return nil
}

// Cleanup runs under the writer gate. Releasing a segment pin performs no I/O.
func (s *Store) Cleanup() (removed, pinned int64, result error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		return 0, 0, err
	}
	ctx := context.Background()
	var listing directoryListing
	if err := s.inspectNames(ctx, entries, &listing); err != nil {
		return 0, 0, err
	}
	return s.cleanupValidated(ctx, listing.objects)
}

// The caller validates the entire listing before truncating or unlinking any file.
func (s *Store) cleanupValidated(ctx context.Context, objects []directoryObject) (removed, pinned int64, result error) {
	selected := map[uint64]bool{s.Current.ActiveID: true}
	for _, f := range s.Catalog.Entries {
		if f.HasPhysicalFile() {
			selected[f.ID] = true
		}
	}
	for _, obj := range objects {
		if err := ctx.Err(); err != nil {
			return removed, pinned, errors.Join(err, s.syncRetirements("cleanup.dirsync"))
		}
		id := obj.id
		switch obj.kind {
		case "INDEX":
			if id == s.Catalog.Index.ID || id == s.buildingIndex.Load() {
				continue
			}
		case "MANIFEST":
			if id == s.Current.ManifestID {
				continue
			}
		case "PARITY":
			if id != s.Current.ActiveID && selected[id] {
				continue
			}
			if f := s.Segments[id]; !selected[id] && f != nil && f.References() != 0 {
				// A parity publisher may still own its source pin.
				continue
			}
		case "DATA":
			if selected[id] {
				continue
			}
			if f := s.Segments[id]; f != nil {
				if f.References() != 0 {
					s.retireData(f)
					pinned += f.Size.Load()
					continue
				}
				if err := s.readers.forget(f); err != nil {
					return removed, pinned, err
				}
			}
		case ".tarp-current":
			// Only owned temporary files were included in the listing.
		default:
			continue
		}
		st, err := obj.entry.Info()
		if err != nil {
			return removed, pinned, err
		}
		unlinked := false
		unlinkErr := s.IO.Do("cleanup.unlink", func() error {
			err := os.Remove(filepath.Join(s.Dir, obj.entry.Name()))
			unlinked = err == nil
			return err
		})
		if unlinked {
			removed += st.Size()
			s.retireSyncPending = true
			switch obj.kind {
			case "DATA":
				delete(s.Segments, id)
				delete(s.retired, id)
			case "INDEX":
				delete(s.retiredIndexes, id)
			case "MANIFEST":
				delete(s.retiredCatalogs, id)
			}
		}
		if unlinkErr != nil {
			return removed, pinned, unlinkErr
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
