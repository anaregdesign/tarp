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
	"github.com/anaregdesign/tarp/internal/index/btree"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/publication"
)

// ErrIndexCheckpointUnusable indicates that the runtime index must be rebuilt from DATA.
var ErrIndexCheckpointUnusable = errors.New("storage: unusable index checkpoint")
var ErrCoveredCatalogPrefixChanged = errors.New("storage: the catalog prefix covered by the index checkpoint was replaced")

type indexPageCorruption struct{}

func (indexPageCorruption) Error() string { return "storage: checkpoint page integrity failure" }
func (indexPageCorruption) Is(target error) bool {
	return target == ErrIndexCheckpointUnusable || target == ErrCorrupt
}

const indexSignature = "TARPIDX2"
const (
	indexHeaderBytes = 128
	indexCountsBytes = 24
)

func IndexName(uuid [16]byte, id uint64) string {
	return fmt.Sprintf("INDEX-%x-%016x", uuid, id)
}

type indexCheckpointCounts struct {
	deletes, liveKeys, liveBytes int64
}

type selectedIndexFile struct {
	fd          *os.File
	r           io.ReaderAt
	packedStart int64
	packedSize  int64
	levels      []indexHashLevel
	header      [indexHeaderBytes]byte
	packedHead  [btree.PackedHeaderBytes]byte
	parent      format.IndexCheckpoint
	tree        btree.OverlayLayout
	inherited   uint64
	layout      format.Catalog
	counts      []indexCheckpointCounts
}

func (s *Store) openIndexFile(ctx context.Context, c format.Catalog) (*selectedIndexFile, error) {
	var observe func(string, int)
	if s.IO != nil {
		observe = s.IO.OpenRead
	}
	return s.readIndexFile(ctx, c, observe)
}

func (s *Store) readIndexFile(ctx context.Context, c format.Catalog, observe func(string, int)) (_ *selectedIndexFile, result error) {
	cp := c.Index
	fd, err := OpenRegular(filepath.Join(s.Dir, IndexName(c.UUID, cp.ID)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, errIndexCheckpointMissing
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if result != nil {
			result = errors.Join(result, fd.Close())
		}
	}()
	st, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() != cp.Bytes || cp.Bytes < indexHeaderBytes+btree.PackedHeaderBytes {
		return nil, ErrIndexCheckpointUnusable
	}
	file := &selectedIndexFile{fd: fd, r: fd}
	if observe != nil {
		file.r = checkpointReadMeter{fd, observe}
	}
	if _, err := file.r.ReadAt(file.header[:], 0); err != nil {
		return nil, indexReadError(err)
	}
	h := file.header[:]
	u64 := binary.LittleEndian.Uint64
	if [32]byte(h[8:40]) != cp.CoveredLayoutFingerprint ||
		u64(h[40:48]) != cp.CoveredCatalogEntries || u64(h[120:]) != 0 {
		return nil, ErrIndexCheckpointUnusable
	}
	digest := sha256.New()
	start := int64(indexHeaderBytes)
	switch string(h[:8]) {
	case indexSignature:
	case indexLayerSignature:
		var parent [format.IndexCheckpointBytes]byte
		if _, err := file.r.ReadAt(parent[:], start); err != nil {
			return nil, indexReadError(err)
		}
		file.parent, err = format.DecodeIndexCheckpoint(parent[:])
		if err != nil || file.parent.ID == 0 || file.parent.ID >= cp.ID ||
			file.parent.CoveredSequence > cp.CoveredSequence || file.parent.CoveredSequence < c.CheckpointCutoffSequence {
			return nil, ErrIndexCheckpointUnusable
		}
		_, _ = digest.Write(parent[:])
		start += format.IndexCheckpointBytes
	default:
		return nil, ErrIndexCheckpointUnusable
	}
	n, layoutSize, packed := cp.CoveredCatalogEntries, u64(h[48:56]), u64(h[56:64])
	if cp.Bytes < start {
		return nil, ErrIndexCheckpointUnusable
	}
	room := uint64(cp.Bytes - start)
	// The summary embeds an ordinary base catalog and one count tuple per entry.
	emptyCatalog := format.CatalogEncodedBytes(0)
	entryBytes := format.CatalogEncodedBytes(1) - emptyCatalog
	if room < emptyCatalog || n > (room-emptyCatalog)/(entryBytes+indexCountsBytes) || layoutSize != emptyCatalog+entryBytes*n ||
		layoutSize+n*indexCountsBytes > room || packed > room-layoutSize-n*indexCountsBytes ||
		layoutSize > math.MaxInt || n > math.MaxInt {
		return nil, ErrIndexCheckpointUnusable
	}
	file.packedStart, file.packedSize = start+int64(layoutSize+n*indexCountsBytes), int64(packed)
	var end int64
	file.levels, end, err = indexHashLayout(file.packedStart, file.packedSize)
	if err != nil || end != cp.Bytes {
		return nil, ErrIndexCheckpointUnusable
	}
	raw := make([]byte, int(layoutSize))
	if _, err := file.r.ReadAt(raw, start); err != nil {
		return nil, indexReadError(err)
	}
	_, _ = digest.Write(raw)
	file.counts = make([]indexCheckpointCounts, int(n))
	countsReader := bufio.NewReaderSize(io.NewSectionReader(file.r, start+int64(layoutSize), int64(n*indexCountsBytes)), 32<<10)
	var tuple [indexCountsBytes]byte
	for i := range file.counts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(countsReader, tuple[:]); err != nil {
			return nil, indexReadError(err)
		}
		_, _ = digest.Write(tuple[:])
		file.counts[i] = indexCheckpointCounts{int64(u64(tuple[:8])), int64(u64(tuple[8:16])), int64(u64(tuple[16:24]))}
	}
	if _, err := file.r.ReadAt(file.packedHead[:], file.packedStart); err != nil {
		return nil, indexReadError(err)
	}
	_, _ = digest.Write(h)
	_, _ = digest.Write(file.packedHead[:])
	if [32]byte(digest.Sum(nil)) != cp.IndexFileDigest {
		return nil, ErrIndexCheckpointUnusable
	}
	if file.parent.ID != 0 {
		file.tree, file.inherited, err = decodeIndexLayer(file.packedHead[:], file.packedSize)
	} else {
		file.tree, err = btree.PackedOverlayLayout(file.packedHead[:], file.packedSize)
	}
	if err != nil || file.tree.Keys != u64(h[64:72]) ||
		len(file.levels) == 0 && [32]byte(h[88:120]) != ([32]byte{}) {
		return nil, ErrIndexCheckpointUnusable
	}
	file.layout, err = format.DecodeCatalog(raw)
	if err != nil || file.layout.UUID != c.UUID || file.layout.Index != (format.IndexCheckpoint{}) ||
		file.layout.CheckpointCutoffSequence != c.CheckpointCutoffSequence ||
		len(file.layout.Entries) != len(file.counts) {
		return nil, ErrIndexCheckpointUnusable
	}
	fingerprint, err := file.layout.CoveredLayoutFingerprint(cp.CoveredCatalogEntries, cp.CoveredSequence)
	if err != nil || fingerprint != cp.CoveredLayoutFingerprint {
		return nil, ErrIndexCheckpointUnusable
	}
	return file, nil
}

func (f *selectedIndexFile) verifiedPages() *verifiedIndexPages {
	return &verifiedIndexPages{
		r: f.r, start: f.packedStart, size: f.packedSize,
		root: [sha256.Size]byte(f.header[88:120]), header: f.packedHead[:],
		levels: f.levels, cached: make([]indexHashCache, len(f.levels)),
	}
}

// The eager importer checks the complete packed topology and file. Memory
// mode additionally measures the live records while populating its map.
func (s *Store) readIndexCheckpoint(ctx context.Context, c format.Catalog, load IndexImporter, visit func(string, index.Address) error) (_ index.Backend, _ []int64, result error) {
	file, err := s.openIndexFile(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	var keys index.Backend
	defer func() {
		result = errors.Join(result, file.fd.Close())
		if result != nil && keys != nil {
			result = errors.Join(result, keys.Close())
		}
	}()
	if file.parent.ID != 0 {
		return nil, nil, ErrIndexCheckpointUnusable
	}
	reader := io.NewSectionReader(file.verifiedPages(), 0, file.packedSize)
	keys, err = load(ctx, s, reader, file.packedSize, visit)
	if err != nil {
		return nil, nil, indexReadError(err)
	}
	deletes := make([]int64, len(file.counts))
	for i := range file.counts {
		deletes[i] = file.counts[i].deletes
	}
	return keys, deletes, nil
}

type checkpointReadMeter struct {
	io.ReaderAt
	observe func(string, int)
}

func (r checkpointReadMeter) ReadAt(b []byte, off int64) (int, error) {
	n, err := r.ReaderAt.ReadAt(b, off)
	if n > 0 {
		r.observe("index_checkpoint_file", n)
	}
	return n, err
}

// IsIndexCheckpointUnusable reports whether every cause invalidates the index checkpoint. An additional read, close or cancellation failure must reach the
// caller even when a decoder also found an invalid page.
func IsIndexCheckpointUnusable(err error, equivalent ...error) bool {
	return onlyCauses(err, ErrIndexCheckpointUnusable, equivalent...)
}

func indexReadError(err error) error {
	if IsIndexCheckpointUnusable(err, io.EOF, io.ErrUnexpectedEOF) {
		return ErrIndexCheckpointUnusable
	}
	return err
}

// IndexCheckpointBuild owns a pinned DATA selection and a private, named staging file.
// Only BeginIndexCheckpoint and Install require the writer gate.
type IndexCheckpointBuild struct {
	store         *Store
	capture       *Capture
	descriptor    format.IndexCheckpoint
	stage         string
	final         string // unselected, protected from cleanup until Release
	filePublished bool
	ancestors     map[uint64]bool
}

func (b *IndexCheckpointBuild) Sources() []CapturedSegment { return b.capture.Segments }

func (s *Store) CatalogMatchesIndexCheckpoint() bool {
	cp := s.Catalog.Index
	if cp.ID == 0 || cp.CoveredSequence != s.Current.LastUserSequence || cp.CoveredCatalogEntries != uint64(s.CatalogEntries().Len()) {
		return false
	}
	catalog := s.CatalogSnapshot()
	hash, err := catalog.CoveredLayoutFingerprint(cp.CoveredCatalogEntries, cp.CoveredSequence)
	return err == nil && hash == cp.CoveredLayoutFingerprint
}

func (s *Store) BeginIndexCheckpoint() (*IndexCheckpointBuild, error) {
	if s.Current.ActiveCommittedEndOffset != format.SegmentHeaderBytes {
		return nil, errors.New("storage: index checkpoint requires a sealed boundary")
	}
	id, err := s.allocateID()
	if err != nil {
		return nil, err
	}
	c, err := s.Capture()
	if err != nil {
		return nil, err
	}
	n, seq := uint64(len(c.Catalog.Entries)), c.Current.LastUserSequence
	fingerprint, err := c.Catalog.CoveredLayoutFingerprint(n, seq)
	if err != nil {
		c.Release()
		return nil, err
	}
	s.buildingIndex.Store(id)
	return &IndexCheckpointBuild{store: s, capture: c, descriptor: format.IndexCheckpoint{ID: id, CoveredCatalogEntries: n, CoveredSequence: seq, CoveredLayoutFingerprint: fingerprint}}, nil
}

func (b *IndexCheckpointBuild) Release() error {
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

func (b *IndexCheckpointBuild) Write(ctx context.Context, tree index.Backend, export func(context.Context, index.Backend, io.Writer) error) (result error) {
	// Isolate recovery counters from live accounting. The capture pins the
	// original files even if rotations occur during the scan.
	image := b.replayImage()
	if err := b.store.IO.Check("index.scan:before"); err != nil {
		return err
	}
	if err := image.rebuildIndexFromData(ctx, tree); err != nil {
		var work *IndexWorkError
		if errors.As(err, &work) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return &SourceError{Err: err}
	}
	return b.writeImage(ctx, image, format.IndexCheckpoint{}, func(pages *indexPageWriter) error {
		return export(ctx, tree, pages)
	})
}

func (b *IndexCheckpointBuild) replayImage() Image {
	c := b.capture
	image := Image{
		Current: c.Current, Catalog: c.Catalog,
		Segments: make(map[uint64]*DataSegment, len(c.Segments)), liveCapture: true,
	}
	for _, f := range c.Segments {
		image.Segments[f.Segment.ID] = &DataSegment{ID: f.Segment.ID, Kind: f.Segment.Kind, Path: f.Segment.Path}
	}
	return image
}

func (b *IndexCheckpointBuild) writeImage(ctx context.Context, image Image, parent format.IndexCheckpoint, writePages func(*indexPageWriter) error) (result error) {
	dir, err := b.store.WorkspaceDir()
	if err != nil {
		return err
	}
	if b.stage != "" {
		if err := os.Remove(b.stage); err != nil {
			return err
		}
		b.stage = ""
	}
	fd, err := os.CreateTemp(dir, "tarp-index-*")
	if err != nil {
		return err
	}
	b.stage = fd.Name()
	defer func() { result = errors.Join(result, fd.Close()) }()
	metadata := sha256.New()
	writer := &outputWriter{fs: b.store.IO, fd: fd, offset: indexHeaderBytes, hash: metadata, point: "index.write"}
	out := bufio.NewWriterSize(writer, 32<<10)
	signature := indexSignature
	if parent.ID != 0 {
		signature = indexLayerSignature
		encoded := format.EncodeIndexCheckpoint(parent)
		if _, err := out.Write(encoded[:]); err != nil {
			return err
		}
	}
	c := b.capture
	layout := c.Catalog
	layout.Index = format.IndexCheckpoint{}
	encodedLayout := format.EncodeCatalog(layout)
	if _, err := out.Write(encodedLayout); err != nil {
		return err
	}
	put := binary.LittleEndian.PutUint64
	var raw [indexCountsBytes]byte
	for _, entry := range c.Catalog.Entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		clear(raw[:])
		if entry.HasPhysicalFile() {
			f := image.Segments[entry.ID]
			put(raw[:8], uint64(f.DeleteBytes.Load()))
			put(raw[8:16], uint64(f.LiveKeys.Load()))
			put(raw[16:24], uint64(f.LiveBytes.Load()))
		}
		if _, err := out.Write(raw[:]); err != nil {
			return err
		}
	}
	if err := out.Flush(); err != nil {
		return err
	}
	packedStart := writer.offset
	writer.hash = nil
	pages := indexPageWriter{packed: out, fs: b.store.IO, fd: fd, start: packedStart}
	if err := writePages(&pages); err != nil {
		return err
	}
	if err := pages.finish(); err != nil {
		return err
	}
	if err := out.Flush(); err != nil {
		return err
	}
	var header [indexHeaderBytes]byte
	copy(header[:8], signature)
	copy(header[8:40], b.descriptor.CoveredLayoutFingerprint[:])
	put(header[40:48], b.descriptor.CoveredCatalogEntries)
	put(header[48:56], uint64(len(encodedLayout)))
	put(header[56:64], uint64(pages.size))
	put(header[64:72], image.LiveKeys)
	put(header[72:80], uint64(image.LargestLiveRecordBytes))
	put(header[80:88], uint64(image.LargestLiveValueBytes))
	copy(header[88:120], pages.root[:])
	if err := b.store.IO.Write(fd, header[:], 0, "index.header"); err != nil {
		return err
	}
	_, _ = metadata.Write(header[:])
	_, _ = metadata.Write(pages.header)
	if err := b.store.IO.Sync(fd, "index.sync"); err != nil {
		return err
	}
	b.descriptor.Bytes = pages.end
	b.descriptor.IndexFileDigest = [32]byte(metadata.Sum(nil))
	return nil
}

// PublishFile durably names the candidate outside the writer gate. Its
// reserved ID protects it from cleanup until selector publication or discard.
func (b *IndexCheckpointBuild) PublishFile() error {
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

func (b *IndexCheckpointBuild) Install() error {
	if !b.filePublished || b.final == "" {
		return errors.New("storage: checkpoint file has not been published")
	}
	s := b.store
	cp := b.descriptor
	catalog := s.CatalogSnapshot()
	fingerprint, err := catalog.CoveredLayoutFingerprint(cp.CoveredCatalogEntries, cp.CoveredSequence)
	if err != nil || fingerprint != cp.CoveredLayoutFingerprint {
		return ErrCoveredCatalogPrefixChanged
	}
	if s.Current.PublicationGeneration == math.MaxUint64 {
		return errors.New("storage: publication generation exhausted")
	}
	frontier := s.Current
	catalog.Index = cp
	frontier.PublicationGeneration++
	s.pendingIndexAncestors = b.ancestors
	defer func() { s.pendingIndexAncestors = nil }()
	if err := s.publishCatalogMetadata(catalog, frontier, "index."); err != nil {
		var f *Fault
		if errors.As(err, &f) && f.Outcome == publication.Unknown {
			b.final = "" // Reopen decides which catalog won.
		}
		return err
	}
	b.final = "" // The catalog now owns the file.
	s.IndexCheckpointValidated = true
	// A cleanup failure represents an already published checkpoint.
	if _, _, _, err := s.RetireCleaned(); err != nil {
		return fault("index.cleanup", publication.Committed, false, err)
	}
	return nil
}
