package storage

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/publication"
)

// ErrIndexMiss means that authoritative replay can replace this optional file.
var ErrIndexMiss = errors.New("storage: unusable index checkpoint")
var ErrIndexLayoutChanged = errors.New("storage: checkpoint prefix was replaced")

const indexSignature = "TARPIDX1"
const indexHeaderBytes = 48

func IndexName(uuid [16]byte, id uint64) string {
	return fmt.Sprintf("INDEX-%x-%016x", uuid, id)
}

// A reader never holds the immutable INDEX past Open. The returned backend
// owns a private copy. The selected catalog supplies the trusted file digest.
func (s *Store) readIndexCheckpoint(ctx context.Context, c format.Catalog, load IndexImporter, visit func(string, index.Address) error) (_ index.Backend, _ []int64, result error) {
	cp := c.Index
	fd, err := OpenRegular(filepath.Join(s.Dir, IndexName(c.UUID, cp.ID)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, ErrIndexMiss
	}
	if err != nil {
		return nil, nil, err
	}
	var keys index.Backend
	defer func() {
		result = errors.Join(result, fd.Close())
		if result != nil && keys != nil {
			result = errors.Join(result, keys.Close())
		}
	}()
	st, err := fd.Stat()
	if err != nil {
		return nil, nil, err
	}
	if st.Size() != cp.Bytes || cp.Bytes < indexHeaderBytes || cp.Entries > uint64(len(c.Files)) || cp.Entries > uint64((cp.Bytes-indexHeaderBytes)/8) {
		return nil, nil, ErrIndexMiss
	}
	h := sha256.New()
	var source io.Reader = fd
	if s.IO != nil && s.IO.OpenRead != nil {
		source = checkpointReadMeter{fd, s.IO.OpenRead}
	}
	rd := bufio.NewReaderSize(io.TeeReader(source, h), 32<<10)
	var header [indexHeaderBytes]byte
	if _, err := io.ReadFull(rd, header[:]); err != nil {
		return nil, nil, indexReadError(err)
	}
	if string(header[:8]) != indexSignature || [32]byte(header[8:40]) != cp.Layout || binary.LittleEndian.Uint64(header[40:]) != cp.Entries {
		return nil, nil, ErrIndexMiss
	}
	deletes := make([]int64, int(cp.Entries))
	var raw [8]byte
	for i := range deletes {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if _, err := io.ReadFull(rd, raw[:]); err != nil {
			return nil, nil, indexReadError(err)
		}
		deletes[i] = int64(binary.LittleEndian.Uint64(raw[:]))
	}
	packed := cp.Bytes - indexHeaderBytes - int64(len(deletes))*8
	keys, err = load(ctx, s, rd, packed, visit)
	if err != nil {
		return nil, nil, indexReadError(err)
	}
	if [32]byte(h.Sum(nil)) != cp.Digest {
		return nil, nil, ErrIndexMiss
	}
	return keys, deletes, nil
}

type checkpointReadMeter struct {
	io.Reader
	observe func(string, int)
}

func (r checkpointReadMeter) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	if n > 0 {
		r.observe("index", n)
	}
	return n, err
}

// IsIndexMiss reports whether every cause describes an unusable optional
// index. An additional read, close or cancellation failure must reach the
// caller even when a decoder also found an invalid page.
func IsIndexMiss(err error, equivalent ...error) bool {
	return onlyCauses(err, ErrIndexMiss, equivalent...)
}

func indexReadError(err error) error {
	if IsIndexMiss(err, io.EOF, io.ErrUnexpectedEOF) {
		return ErrIndexMiss
	}
	return err
}

// IndexBuild owns a pinned DATA selection and a private, named staging file.
// Only BeginIndex and Install require the writer gate.
type IndexBuild struct {
	store         *Store
	capture       *Capture
	descriptor    format.IndexCheckpoint
	stage         string
	final         string // unselected, protected from cleanup until Release
	filePublished bool
}

func (b *IndexBuild) Sources() []CapturedFile { return b.capture.Files }

func (s *Store) BeginIndex() (*IndexBuild, error) {
	if s.Current.ActiveEnd != format.SegmentHeaderBytes {
		return nil, errors.New("storage: index checkpoint requires a sealed boundary")
	}
	n, seq := uint64(len(s.Catalog.Files)), s.Current.Sequence
	layout, err := s.Catalog.IndexLayout(n, seq)
	if err != nil {
		return nil, err
	}
	id, err := s.allocateID()
	if err != nil {
		return nil, err
	}
	c, err := s.Capture()
	if err != nil {
		return nil, err
	}
	s.buildingIndex.Store(id)
	return &IndexBuild{store: s, capture: c, descriptor: format.IndexCheckpoint{ID: id, Entries: n, Sequence: seq, Layout: layout}}, nil
}

func (b *IndexBuild) Release() error {
	b.capture.Release()
	defer b.store.buildingIndex.CompareAndSwap(b.descriptor.ID, 0)
	path := b.stage
	if b.final != "" {
		path = b.final
	}
	if path == "" {
		return nil
	}
	err := os.Remove(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if b.final != "" {
		return b.store.IO.SyncDir(b.store.Dir, "index.discard.dirsync")
	}
	return nil
}

func (b *IndexBuild) Write(ctx context.Context, tree index.Backend, export func(context.Context, index.Backend, io.Writer) error) (result error) {
	// Isolate recovery counters from live accounting. The capture pins the
	// original files even if rotations occur during the scan.
	c := b.capture
	image := Image{
		Current: c.Current, Catalog: c.Catalog,
		Files: make(map[uint64]*File, len(c.Files)), liveCapture: true,
	}
	for _, f := range c.Files {
		image.Files[f.File.ID] = &File{ID: f.File.ID, Kind: f.File.Kind, Path: f.File.Path}
	}
	if err := b.store.IO.Check("index.scan:before"); err != nil {
		return err
	}
	if err := image.replayIndexed(ctx, tree); err != nil {
		var work *IndexWorkError
		if errors.As(err, &work) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return &SourceError{Err: err}
	}
	dir, err := b.store.WorkspaceDir()
	if err != nil {
		return err
	}
	fd, err := os.CreateTemp(dir, "tarp-index-*")
	if err != nil {
		return err
	}
	b.stage = fd.Name()
	defer func() { result = errors.Join(result, fd.Close()) }()
	h := sha256.New()
	writer := &outputWriter{fs: b.store.IO, fd: fd, hash: h, point: "index.write"}
	out := bufio.NewWriterSize(writer, 32<<10)
	var header [indexHeaderBytes]byte
	copy(header[:8], indexSignature)
	copy(header[8:40], b.descriptor.Layout[:])
	binary.LittleEndian.PutUint64(header[40:], b.descriptor.Entries)
	if _, err := out.Write(header[:]); err != nil {
		return err
	}
	var raw [8]byte
	for _, entry := range c.Catalog.Files {
		var count int64
		if entry.HasPhysicalFile() {
			count = image.Files[entry.ID].DeleteBytes.Load()
		}
		binary.LittleEndian.PutUint64(raw[:], uint64(count))
		if _, err := out.Write(raw[:]); err != nil {
			return err
		}
	}
	if err := export(ctx, tree, out); err != nil {
		return err
	}
	if err := out.Flush(); err != nil {
		return err
	}
	if err := b.store.IO.Sync(fd, "index.sync"); err != nil {
		return err
	}
	b.descriptor.Bytes = writer.offset
	b.descriptor.Digest = [32]byte(h.Sum(nil))
	return nil
}

// PublishFile durably names the candidate outside the writer gate. Its
// reserved ID protects it from cleanup until selector publication or discard.
func (b *IndexBuild) PublishFile() error {
	s := b.store
	path := filepath.Join(s.Dir, IndexName(b.capture.Current.UUID, b.descriptor.ID))
	if err := s.IO.Do("index.rename", func() error {
		if err := platform.RenameNoReplace(b.stage, path); err != nil {
			return err
		}
		b.final, b.stage = path, ""
		return nil
	}); err != nil {
		return err
	}
	err := s.IO.SyncDir(s.Dir, "index.dirsync")
	b.filePublished = err == nil
	return err
}

func (b *IndexBuild) Install() error {
	if !b.filePublished || b.final == "" {
		return errors.New("storage: checkpoint file has not been published")
	}
	s := b.store
	cp := b.descriptor
	layout, err := s.Catalog.IndexLayout(cp.Entries, cp.Sequence)
	if err != nil || layout != cp.Layout {
		return ErrIndexLayoutChanged
	}
	if s.Current.Generation == math.MaxUint64 {
		return errors.New("storage: publication generation exhausted")
	}
	catalog, frontier := s.Catalog, s.Current
	catalog.Index = cp
	frontier.Generation++
	if err := s.publishCatalogFor(catalog, frontier, "index.", nil); err != nil {
		var f *Fault
		if errors.As(err, &f) && f.Outcome == publication.Unknown {
			b.final = "" // Reopen decides which catalog won.
		}
		return err
	}
	b.final = "" // The catalog now owns the file.
	s.IndexCheckpointHit = true
	// A cleanup failure represents an already published checkpoint.
	if _, _, _, err := s.RetireCleaned(); err != nil {
		return fault("index.cleanup", publication.Committed, false, err)
	}
	return nil
}
