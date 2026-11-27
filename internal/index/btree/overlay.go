package btree

import (
	"context"
	"errors"
	"io"
	"math"
	"os"

	"github.com/anaregdesign/tarp/internal/index"
)

// OverlayLayout describes an immutable page space assembled from a base and
// zero or more overlays. Top is the last reserved page ID, including holes.
type OverlayLayout struct {
	Root, Top, Keys, MaxKey uint64
}

func (l OverlayLayout) valid() bool {
	if l.Top > math.MaxInt64/pageSize || l.Root > l.Top || l.Keys > math.MaxInt || l.MaxKey > math.MaxInt {
		return false
	}
	if l.Root == 0 {
		return l.Keys == 0 && l.MaxKey == 0
	}
	pageBytes := l.Top * pageSize
	return l.Keys != 0 && l.Keys <= pageBytes/addressSize && l.MaxKey != 0 && l.MaxKey <= pageBytes
}

// PackedOverlayLayout decodes the logical root and ID bounds of a dense packed
// base. The caller authenticates header against the selected file.
func PackedOverlayLayout(header []byte, size int64) (OverlayLayout, error) {
	if len(header) != packedHeaderBytes {
		return OverlayLayout{}, ErrInvalidPacked
	}
	h, err := decodePackedHeader(header, size)
	if err != nil {
		return OverlayLayout{}, err
	}
	return OverlayLayout{h.root, h.pages, h.keys, h.maxKey}, nil
}

// OpenOverlay uses an authenticated reader for the complete immutable page
// space. Its byte offsets are (pageID - 1) * PackedPageBytes. Holes must not
// be reachable. The caller retains closer on error and transfers it on success.
func OpenOverlay(ctx context.Context, pages io.ReaderAt, closer io.Closer, layout OverlayLayout, dir string, budget int64) (_ index.Backend, result error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !layout.valid() || pages == nil && layout.Top != 0 {
		return nil, ErrInvalidPacked
	}
	backend, err := NewIn(dir, budget)
	if err != nil {
		return nil, err
	}
	t := backend.(*tree)
	defer func() {
		if result != nil {
			result = errors.Join(result, t.Close())
		}
	}()
	t.p.base, t.p.basePages, t.p.top = pages, layout.Top, layout.Top
	t.p.maxKey.Store(layout.MaxKey)
	t.root, t.count = layout.Root, int(layout.Keys)
	t.p.root.Store(layout.Root)
	if layout.Root != 0 {
		p, err := t.p.read(layout.Root)
		if err != nil {
			if errors.Is(err, errPage) {
				return nil, ErrInvalidPacked
			}
			return nil, err
		}
		if p[4] != branch && p[4] != leaf || p.count() == 0 {
			return nil, ErrInvalidPacked
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	t.p.baseCloser = closer
	return t, nil
}

// ExportOverlay serializes a detached checkpoint builder's new page IDs. The
// exporter skips immutable subtrees and writes zeroes for unreachable slots in
// ID order. The callback consumes borrowed bytes before it returns.
func ExportOverlay(ctx context.Context, backend index.Backend, describe func(layout OverlayLayout, baseTop uint64) error, emit func(id uint64, page []byte) error) (result error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	t, ok := backend.(*tree)
	if !ok {
		return errors.New("btree: overlay export requires a B+tree")
	}
	t.writer.Lock()
	defer t.writer.Unlock()
	if t.closed {
		return os.ErrClosed
	}
	p := t.p
	layout := OverlayLayout{t.root, p.top, uint64(t.count), p.maxKey.Load()}
	if layout.Root == 0 {
		layout.MaxKey = 0
	}
	if !layout.valid() || p.basePages > layout.Top {
		return ErrInvalidPacked
	}
	if err := describe(layout, p.basePages); err != nil {
		return err
	}
	if layout.Top == p.basePages {
		return ctx.Err()
	}
	walk := overlayReachability{ctx: ctx, pager: p}
	defer func() {
		if walk.seen.file != nil {
			result = errors.Join(result, walk.seen.file.Close())
		}
	}()
	if err := walk.seen.openCount(p, layout.Top-p.basePages); err != nil {
		return err
	}
	if layout.Root != 0 {
		if err := walk.tree(layout.Root, 1); err != nil {
			return err
		}
	}
	var out page
	for id := p.basePages + 1; id <= layout.Top; id++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		at, bit, err := walk.seen.position(id - p.basePages)
		if err != nil {
			return err
		}
		clear(out[:])
		if walk.seen.bits[at]&bit != 0 {
			next, err := p.read(id)
			if err != nil {
				return err
			}
			out = *next
		}
		if err := emit(id, out[:]); err != nil {
			return err
		}
	}
	return ctx.Err()
}

type overlayReachability struct {
	ctx   context.Context
	pager *pager
	seen  packedSeen
}

func (w *overlayReachability) tree(id uint64, depth int) error {
	if id == 0 || depth > maxDepth {
		return ErrInvalidPacked
	}
	if id <= w.pager.basePages {
		return nil
	}
	p, err := w.read(id)
	if err != nil {
		return err
	}
	if p[4] == branch {
		if err := w.tree(p.link(), depth+1); err != nil {
			return err
		}
	} else if p[4] != leaf {
		return ErrInvalidPacked
	}
	for i := range p.count() {
		c, err := p.raw(i)
		if err != nil {
			return err
		}
		if err := w.overflow(c.tail); err != nil {
			return err
		}
		if p[4] == branch {
			if err := w.tree(c.child, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *overlayReachability) overflow(id uint64) error {
	for id > w.pager.basePages {
		p, err := w.read(id)
		if err != nil {
			return err
		}
		if p[4] != overflow {
			return ErrInvalidPacked
		}
		id = p.link()
	}
	return nil
}

func (w *overlayReachability) read(id uint64) (*page, error) {
	if err := w.ctx.Err(); err != nil {
		return nil, err
	}
	p, err := w.pager.read(id)
	if err != nil {
		return nil, err
	}
	if err := w.seen.claim(id - w.pager.basePages); err != nil {
		return nil, err
	}
	return p, nil
}
