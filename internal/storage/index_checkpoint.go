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

func indexReadError(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrIndexMiss
	}
	return err
}

// IndexBuild owns a pinned DATA selection and a private, named staging file.
// Only BeginIndex and Install require the writer gate.
type IndexBuild struct {
	store      *Store
	capture    *Capture
	descriptor format.IndexCheckpoint
	stage      string
}

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
	return &IndexBuild{store: s, capture: c, descriptor: format.IndexCheckpoint{ID: id, Entries: n, Sequence: seq, Layout: layout}}, nil
}

func (b *IndexBuild) Release() error {
	b.capture.Release()
	if b.stage == "" {
		return nil
	}
	err := os.Remove(b.stage)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (b *IndexBuild) Write(ctx context.Context, tree index.Backend, export func(context.Context, index.Backend, io.Writer) error) (result error) {
	// Isolate recovery counters from live accounting. The capture pins the
	// original files even if rotations occur during the scan.
	c := b.capture
	image := Image{Current: c.Current, Catalog: c.Catalog, Files: make(map[uint64]*File, len(c.Files))}
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

func (b *IndexBuild) Install() error {
	s := b.store
	cp := b.descriptor
	layout, err := s.Catalog.IndexLayout(cp.Entries, cp.Sequence)
	if err != nil || layout != cp.Layout {
		return ErrIndexLayoutChanged
	}
	if s.Current.Generation == math.MaxUint64 {
		return errors.New("storage: publication generation exhausted")
	}
	path := filepath.Join(s.Dir, IndexName(s.Current.UUID, cp.ID))
	renamed := false
	discard := func(cause error) error {
		if !renamed {
			return cause
		}
		err := os.Remove(path)
		s.retireSyncPending = true
		if err != nil {
			s.retireIndex(cp.ID)
		}
		return errors.Join(cause, err, s.syncRetirements("index.discard.dirsync"))
	}
	err = s.IO.Do("index.rename", func() error {
		err := platform.RenameNoReplace(b.stage, path)
		if err == nil {
			renamed = true
			b.stage = ""
		}
		return err
	})
	if err != nil {
		return discard(err)
	}
	if err := s.IO.SyncDir(s.Dir, "index.dirsync"); err != nil {
		return discard(err)
	}
	catalog, frontier := s.Catalog, s.Current
	catalog.Index = cp
	frontier.Generation++
	if err := s.publishCatalogFor(catalog, frontier, "index.", nil); err != nil {
		var f *Fault
		if errors.As(err, &f) && f.Outcome == publication.Unknown {
			return err
		}
		return discard(err)
	}
	s.IndexCheckpointHit = true
	// A cleanup failure represents an already published checkpoint.
	if _, _, _, err := s.RetireCleaned(); err != nil {
		return fault("index.cleanup", publication.Committed, false, err)
	}
	return nil
}
