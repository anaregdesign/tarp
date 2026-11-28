package storage

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"os"

	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
)

// MaxStagedAppendSegments bounds the DATA files owned by one staged commit.
const MaxStagedAppendSegments = format.MaxStagedCatalogEntries

var ErrStagedAppendSegmentLimit = errors.New("storage: staged append DATA segment limit exceeded")

// StagedAppend writes private DATA files for one atomic append. The caller
// holds the writer gate for its lifetime and calls Discard on every exit.
type StagedAppend struct {
	store     *Store
	base      format.Frontier
	last      uint64
	first     uint64 // first record in the current private file
	files     []*DataSegment
	sealed    []format.CatalogEntry
	fd        *os.File
	buffer    *bufio.Writer
	failed    error
	finished  bool
	keepFiles bool // durable or uncertain selector publication
}

func (s *Store) BeginStagedAppend() (*StagedAppend, error) {
	if err := s.checkSequence(0); err != nil {
		return nil, err
	}
	return &StagedAppend{store: s, base: s.Current, last: s.Current.LastUserSequence}, nil
}

// Append returns a location in a private file. Its segment is registered
// with the Store only once Publish selects the new frontier.
func (a *StagedAppend) Append(kind uint8, key, value []byte) (_ Location, result error) {
	if a.finished {
		return Location{}, errors.New("storage: staged append is finished")
	}
	if a.failed != nil {
		return Location{}, a.failed
	}
	defer func() { a.failed = result }()
	if a.last == math.MaxUint64 {
		return Location{}, fault("staged.sequence", publication.NotApplied, false, errors.New("storage: sequence exhausted"))
	}
	if len(key) == 0 || (kind != format.Put && kind != format.Delete) || (kind == format.Delete && len(value) != 0) {
		return Location{}, errors.New("storage: invalid staged record")
	}
	keyBytes, valueBytes := uint64(len(key)), uint64(len(value))
	target := a.store.Config.TargetSegmentBytes
	if target <= format.SegmentHeaderBytes || !entrysize.Representable(keyBytes, valueBytes) ||
		entrysize.RecordBytes(keyBytes, valueBytes) > uint64(target-format.SegmentHeaderBytes) {
		return Location{}, errors.New("storage: staged record cannot fit one DATA segment")
	}
	size := int64(entrysize.RecordBytes(keyBytes, valueBytes))
	if len(a.files) == 0 || a.files[len(a.files)-1].Size.Load() > target-size {
		if err := a.roll(); err != nil {
			return Location{}, err
		}
	}
	file := a.files[len(a.files)-1]
	offset, sequence := file.Size.Load(), a.last+1
	var header [format.RecordHeaderBytes]byte
	format.EncodeRecordHeader(&header, kind, sequence, key, value)
	for _, bytes := range [3][]byte{header[:], key, value} {
		if _, err := a.buffer.Write(bytes); err != nil {
			return Location{}, fault("staged.data.write", publication.NotApplied, false, err)
		}
	}
	file.Size.Store(offset + size)
	a.last = sequence
	return Location{Segment: file, RecordOffset: offset, RecordBytes: size, ValueBytes: int64(valueBytes), Sequence: sequence}, nil
}

func (a *StagedAppend) roll() error {
	if len(a.files) == MaxStagedAppendSegments {
		return ErrStagedAppendSegmentLimit
	}
	if a.fd != nil {
		if err := a.sync(); err != nil {
			return err
		}
		if err := a.closeFile(); err != nil {
			return fault("staged.data.close", publication.NotApplied, false, err)
		}
		file := a.files[len(a.files)-1]
		a.sealed = append(a.sealed, stagedLogEntry(file, a.first, a.last))
		file.activeDigest = nil
	}
	file, err := a.store.reserveSegment(format.RawLogKind)
	if err != nil {
		return fault("staged.data.identity", publication.NotApplied, false, err)
	}
	a.fd, err = os.OpenFile(file.Path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return fault("staged.data.create", publication.NotApplied, false, err)
	}
	a.files = append(a.files, file)
	a.first = a.last + 1
	file.activeDigest = sha256.New()
	w := &outputWriter{fs: a.store.IO, fd: a.fd, hash: file.activeDigest, point: "staged.data.write"}
	if a.buffer == nil {
		a.buffer = bufio.NewWriterSize(w, 32<<10)
	} else {
		a.buffer.Reset(w)
	}
	header := format.EncodeSegmentHeader(format.SegmentHeader{UUID: a.base.UUID, ID: file.ID, Kind: format.RawLogKind})
	if _, err := a.buffer.Write(header); err != nil {
		return fault("staged.data.write", publication.NotApplied, false, err)
	}
	file.Size.Store(format.SegmentHeaderBytes)
	return nil
}

func stagedLogEntry(file *DataSegment, first, last uint64) format.CatalogEntry {
	return format.CatalogEntry{ID: file.ID, Bytes: file.Size.Load(), Kind: format.RawLogKind,
		FirstSequence: first, LastSequence: last, DataFileDigest: [sha256.Size]byte(file.activeDigest.Sum(nil))}
}

func (a *StagedAppend) sync() error {
	if err := a.buffer.Flush(); err != nil {
		return fault("staged.data.write", publication.NotApplied, false, err)
	}
	if err := a.store.IO.Sync(a.fd, "staged.data.sync"); err != nil {
		return fault("staged.data.sync", publication.NotApplied, false, err)
	}
	return nil
}

func (a *StagedAppend) closeFile() error {
	if a.fd == nil {
		return nil
	}
	err := a.fd.Close()
	a.fd = nil
	return err
}

// Publish selects every staged record with a single CURRENT publication. It
// returns the net selected DATA file count change for a known commit.
func (a *StagedAppend) Publish(ctx context.Context, healthy func() error) (int, error) {
	if a.finished {
		return 0, errors.New("storage: staged append is finished")
	}
	a.finished = true
	if a.failed != nil {
		return 0, a.failed
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(a.files) == 0 {
		return 0, nil
	}
	s := a.store
	if s.Current != a.base {
		return 0, errors.New("storage: frontier changed during staged append")
	}
	if err := a.sync(); err != nil {
		return 0, err
	}
	if err := errors.Join(ctx.Err(), healthy()); err != nil {
		return 0, err
	}
	catalog := s.Catalog
	candidate := s.Current
	previous := s.Segments[candidate.ActiveID]
	active := a.files[len(a.files)-1]
	count := len(a.files)
	inserted := a.sealed
	var previousEntry format.CatalogEntry
	if candidate.ActiveCommittedEndOffset > format.SegmentHeaderBytes {
		previousEntry = stagedLogEntry(previous, s.sealedThroughSequence()+1, candidate.LastUserSequence)
		inserted = make([]format.CatalogEntry, 1, len(a.sealed)+1)
		inserted[0] = previousEntry
		inserted = append(inserted, a.sealed...)
	} else {
		count-- // A header-only former active file need not stay selected.
	}
	candidate.PublicationGeneration++
	candidate.ActiveID, catalog.ActiveID = active.ID, active.ID
	candidate.ActiveCommittedEndOffset, candidate.LastUserSequence = active.Size.Load(), a.last
	// The MANIFEST directory sync also makes all staged DATA names durable
	// before the replacement CURRENT can select them.
	err := s.publishStagedCatalogAppend(catalog, inserted, candidate)
	if err != nil {
		var failure *Fault
		if errors.As(err, &failure) {
			a.keepFiles = failure.Outcome == publication.Unknown
			if failure.Outcome == publication.NotApplied {
				// Only unselected DATA and metadata were written.
				failure.Unsafe = false
			}
		}
		return 0, err
	}
	a.keepFiles = true
	previous.activeDigest = nil
	if previousEntry.ID != 0 {
		previous.markSealed(catalog.UUID, previousEntry)
	} else {
		s.retireData(previous)
	}
	for i, file := range a.files {
		s.Segments[file.ID] = file
		if i < len(a.sealed) {
			file.markSealed(catalog.UUID, a.sealed[i])
		}
	}
	old := s.active
	s.active, a.fd = a.fd, nil
	if err := errors.Join(old.Close(), s.IO.Check("staged.old-active.close:after")); err != nil {
		return count, fault("staged.old-active.close", publication.Committed, false, err)
	}
	return count, nil
}

// Discard releases private resources. If CURRENT publication was uncertain,
// keep every candidate name until a subsequent Open decides what is selected.
func (a *StagedAppend) Discard() error {
	a.finished = true
	err := a.closeFile()
	if a.keepFiles || len(a.files) == 0 {
		return err
	}
	s := a.store
	for _, file := range a.files {
		removeErr := s.IO.Do("staged.discard.data", func() error { return os.Remove(file.Path) })
		if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			s.retireData(file)
			err = errors.Join(err, removeErr)
		}
	}
	a.files = nil
	if syncErr := s.IO.SyncDir(s.Dir, "staged.discard.dirsync"); syncErr != nil {
		s.retireSyncPending = true
		err = errors.Join(err, syncErr)
	}
	return err
}
