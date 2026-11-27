package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/btree"
)

const (
	indexLayerSignature     = "TARPIDXL"
	indexLayerPageSignature = "TARPBTL1"
	maxIndexLayers          = 8
	maxIndexLayerPages      = (256 << 20) / btree.PackedPageBytes
)

var errIndexLayerFull = errors.New("storage: incremental INDEX limit")

func decodeIndexLayer(header []byte, size int64) (btree.OverlayLayout, uint64, error) {
	if len(header) != btree.PackedHeaderBytes || string(header[:8]) != indexLayerPageSignature {
		return btree.OverlayLayout{}, 0, ErrIndexCheckpointUnusable
	}
	read := binary.LittleEndian.Uint64
	base := read(header[8:16])
	tree := btree.OverlayLayout{
		Root: read(header[16:24]), Top: read(header[24:32]),
		Keys: read(header[32:40]), MaxKey: read(header[40:48]),
	}
	if tree.Top < base || tree.Top > math.MaxInt64/btree.PackedPageBytes ||
		tree.Top-base > maxIndexLayerPages || size != btree.PackedHeaderBytes+int64(tree.Top-base)*btree.PackedPageBytes ||
		tree.Root > tree.Top || tree.Keys > math.MaxInt || tree.MaxKey > math.MaxInt ||
		(tree.Root == 0) != (tree.Keys == 0) || (tree.Root == 0) != (tree.MaxKey == 0) {
		return btree.OverlayLayout{}, 0, ErrIndexCheckpointUnusable
	}
	return tree, base, nil
}

type indexChainPart struct {
	id    uint64
	file  *selectedIndexFile
	pages *verifiedIndexPages
}

// Parts are ordered from the selected tip back to the packed base.
type indexChain struct{ parts []indexChainPart }

func (s *Store) openIndexChain(ctx context.Context, c format.Catalog) (_ *indexChain, result error) {
	chain := new(indexChain)
	defer func() {
		if result != nil {
			result = errors.Join(result, chain.Close())
		}
	}()
	for c.Index.ID != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(chain.parts) > maxIndexLayers {
			return nil, ErrIndexCheckpointUnusable
		}
		f, err := s.openIndexFile(ctx, c)
		if err != nil {
			return nil, err
		}
		chain.parts = append(chain.parts, indexChainPart{c.Index.ID, f, f.verifiedPages()})
		if n := len(chain.parts); n > 1 {
			child := chain.parts[n-2].file
			if child.inherited != f.tree.Top {
				return nil, ErrIndexCheckpointUnusable
			}
			// Ancestors supply pages only. Keep accounting for the selected tip.
			f.layout.Entries, f.counts = nil, nil
		}
		c.Index = f.parent
	}
	if len(chain.parts) == 0 {
		return nil, ErrIndexCheckpointUnusable
	}
	return chain, nil
}

func (c *indexChain) ids(skip int) map[uint64]bool {
	if len(c.parts) <= skip {
		return nil
	}
	ids := make(map[uint64]bool, len(c.parts)-skip)
	for _, part := range c.parts[skip:] {
		ids[part.id] = true
	}
	return ids
}

func (c *indexChain) Close() (result error) {
	for _, part := range c.parts {
		result = errors.Join(result, part.file.fd.Close())
	}
	return result
}

func (c *indexChain) stopMetering() {
	for _, part := range c.parts {
		part.pages.r = part.file.fd
	}
}

func (c *indexChain) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off%btree.PackedPageBytes != 0 || len(p) != btree.PackedPageBytes {
		return 0, ErrIndexCheckpointUnusable
	}
	id := uint64(off/btree.PackedPageBytes) + 1
	for _, part := range c.parts {
		if id > part.file.inherited && id <= part.file.tree.Top {
			offset := int64(id-part.file.inherited-1)*btree.PackedPageBytes + btree.PackedHeaderBytes
			return part.pages.ReadAt(p, offset)
		}
	}
	return 0, ErrIndexCheckpointUnusable
}

func (c *indexChain) open(ctx context.Context, s *Store, load FastIndexImporter) (index.Backend, error) {
	tip := c.parts[0]
	if len(c.parts) == 1 {
		return load(ctx, s, tip.pages, c, tip.file.packedSize, nil)
	}
	return load(ctx, s, c, c, 0, &tip.file.tree)
}

func (b *IndexCheckpointBuild) WriteIncremental(ctx context.Context, budget int64) (result error) {
	c := b.capture.Catalog
	if c.Index.ID == 0 {
		return errIndexLayerFull
	}
	s := b.store
	chain, err := s.openIndexChain(ctx, c)
	if err != nil {
		return err
	}
	var backend index.Backend
	defer func() {
		if backend == nil {
			result = errors.Join(result, chain.Close())
		} else {
			result = errors.Join(result, backend.Close())
		}
	}()
	if len(chain.parts) > maxIndexLayers {
		return errIndexLayerFull
	}
	image := b.replayImage()
	if s.IO != nil {
		image.openRead = s.IO.OpenRead
	}
	recovered, replay, err := prepareIndexReplayLimit(&image, chain.parts[0].file, math.MaxInt64)
	if err != nil {
		return err
	}
	backend, err = chain.open(ctx, s, func(ctx context.Context, s *Store, source io.ReaderAt, closer io.Closer, size int64, layer *btree.OverlayLayout) (index.Backend, error) {
		dir, err := s.WorkspaceDir()
		if err != nil {
			return nil, err
		}
		if layer != nil {
			return btree.OpenOverlay(ctx, source, closer, *layer, dir, budget)
		}
		return btree.OpenPacked(ctx, source, closer, size, dir, budget)
	})
	if err != nil {
		return err
	}
	if err := s.IO.Check("index.scan:before"); err != nil {
		return err
	}
	if err := replayIndexSources(ctx, &recovered, replay, backend, "index_refresh"); err != nil {
		return err
	}
	if recovered.LiveKeys == 0 {
		recovered.LiveRecordSizes = LiveRecordSizes{}
		recovered.LargestLiveValueBytes = 0
	}
	err = b.writeImage(ctx, recovered, c.Index, func(pages *indexPageWriter) error {
		return btree.ExportOverlay(ctx, backend, func(tree btree.OverlayLayout, inherited uint64) error {
			count := tree.Top - inherited
			if count > maxIndexLayerPages {
				return errIndexLayerFull
			}
			var header [btree.PackedHeaderBytes]byte
			put := binary.LittleEndian.PutUint64
			copy(header[:8], indexLayerPageSignature)
			put(header[8:16], inherited)
			put(header[16:24], tree.Root)
			put(header[24:32], tree.Top)
			put(header[32:40], tree.Keys)
			put(header[40:48], tree.MaxKey)
			_, err := pages.writeHeader(header[:], count)
			return err
		}, func(_ uint64, page []byte) error {
			_, err := pages.Write(page)
			return err
		})
	})
	if err == nil {
		b.ancestors = chain.ids(0)
	}
	return err
}

func IndexRefreshNeedsBase(err error) bool {
	return IsIndexCheckpointUnusable(err, btree.ErrInvalidPacked, errIndexLayerFull)
}
