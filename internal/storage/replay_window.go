package storage

import (
	"container/heap"
	"context"
	"errors"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

// verifyWindowBytes limits the logical bytes of retained keys and replay state.
// A single larger key occupies a window by itself. Parsing necessarily borrows
// the current record's key independently of the window.
const verifyWindowBytes = 4 << 20
const replayKeyCharge = 128

// keyHeap keeps the lexicographically largest eligible key at its root.
type keyHeap []string

func (h keyHeap) Len() int           { return len(h) }
func (h keyHeap) Less(i, j int) bool { return h[i] > h[j] }
func (h keyHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *keyHeap) Push(x any)        { *h = append(*h, x.(string)) }
func (h *keyHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	old[len(old)-1] = ""
	*h = old[:len(old)-1]
	return last
}

type recordSize struct{ record, value int64 }

type keyWindow struct {
	after string
	limit int64
	keys  keyHeap
	live  map[string]recordSize // a zero size records an absent key retained in this window
	used  int64
	more  bool
}

func newKeyWindow(after string, limit int64) *keyWindow {
	return &keyWindow{after: after, limit: limit, live: make(map[string]recordSize)}
}

// The high key can only decrease once an eviction occurs. Therefore a key
// that survives the pass has been tracked since its first record, even when
// the physical replay order bears no relation to key order.
func (w *keyWindow) apply(kind uint32, pruned uint64, r format.Record, key string) error {
	if key <= w.after || (w.more && key > w.keys[0]) {
		return nil
	}
	state, known := w.live[key]
	if !known {
		heap.Push(&w.keys, key)
		w.live[key] = recordSize{}
		w.used += replayKeyCharge + int64(len(key))
		for w.used > w.limit && len(w.keys) > 1 {
			remove := heap.Pop(&w.keys).(string)
			delete(w.live, remove)
			w.used -= replayKeyCharge + int64(len(remove))
			w.more = true
		}
		if _, retained := w.live[key]; !retained {
			return nil
		}
	}
	if err := validateKey(kind, r, state.record != 0, pruned); err != nil {
		return err
	}
	if r.Kind == format.Delete {
		w.live[key] = recordSize{}
	} else {
		w.live[key] = recordSize{record: r.Bytes(), value: r.ValueBytes}
	}
	return nil
}

func validateReadOnly(ctx context.Context, dir string, limit int64) (*Image, error) {
	return validateWindows(ctx, dir, limit, nil)
}

func validateWindows(ctx context.Context, dir string, limit int64, spill func() (index.Backend, error)) (*Image, error) {
	image, err := readImage(ctx, dir)
	if err != nil {
		return nil, err
	}
	after := ""
	for {
		w := newKeyWindow(after, limit)
		if err := image.scan(ctx, func(f *File, _ int64, r format.Record, key string) error {
			return w.apply(f.Kind, image.Catalog.PrunedThrough, r, key)
		}); err != nil {
			return nil, err
		}
		for key, rec := range w.live {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if rec.record != 0 {
				if err := image.measure(key, rec.record, rec.value); err != nil {
					return nil, err
				}
			}
		}
		if !w.more {
			return image, ctx.Err()
		}
		if spill != nil {
			backend, err := spill()
			if err != nil {
				return nil, &IndexWorkError{Err: err}
			}
			result, validationErr := validateIndexed(ctx, dir, backend)
			if err := errors.Join(validationErr, backend.Close()); err != nil {
				return nil, err
			}
			return result, nil
		}
		after = w.keys[0]
	}
}

// IndexWorkError identifies failures from the disposable replay workspace.
type IndexWorkError struct{ Err error }

func (e *IndexWorkError) Error() string { return "index workspace: " + e.Err.Error() }
func (e *IndexWorkError) Unwrap() error { return e.Err }

// replayBuilder makes small private transactions so even a very large image
// can be replayed without buffering an image-sized pending transaction.
type replayBuilder struct {
	direct   index.InitialBuild
	buildErr error
	bulk     index.SortedBuilder
	sort     *sortedReplay
	backend  index.Backend
	tx       index.Tx
	size     int64
	count    int
	pruned   uint64
}

func newReplayBuilder(ctx context.Context, backend index.Backend, pruned uint64) replayBuilder {
	b := replayBuilder{backend: backend, pruned: pruned}
	if bulk, ok := backend.(index.SortedBuilder); ok {
		dir, limit := bulk.BulkWorkspace()
		b.bulk, b.sort = bulk, &sortedReplay{ctx: ctx, dir: dir, limit: limit}
	} else if builder, ok := backend.(index.InitialBuilder); ok {
		b.direct, b.buildErr = builder.BeginBuild()
	}
	return b
}

func replayWorkError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var source *SourceError
	if errors.As(err, &source) {
		return err
	}
	return &IndexWorkError{Err: err}
}

func (b *replayBuilder) apply(f *File, off int64, r format.Record, key string) error {
	if b.sort != nil {
		a := index.Address{FileID: f.ID, Offset: off, Bytes: r.Bytes(), ValueBytes: r.ValueBytes, Sequence: r.Sequence}
		return replayWorkError(b.sort.add(replayEvent{key: key, addr: a, fileKind: f.Kind, recordKind: r.Kind}))
	}
	if b.direct != nil {
		return applyIndexedRecord(b.direct, f, b.pruned, off, r, key)
	}
	if b.buildErr != nil {
		return &IndexWorkError{Err: b.buildErr}
	}
	if b.tx == nil {
		b.tx = b.backend.Begin()
	}
	if err := applyIndexedRecord(b.tx, f, b.pruned, off, r, key); err != nil {
		return err
	}
	b.count++
	b.size += int64(len(key)) + replayKeyCharge
	if b.count >= 128 || b.size >= 64<<10 {
		b.flushTx()
	}
	return nil
}

func (b *replayBuilder) flushTx() {
	if b.tx != nil {
		b.tx.Commit()
		b.tx = nil
	}
	b.count, b.size = 0, 0
}

func (b *replayBuilder) finishValidation() error {
	if b.sort != nil {
		return replayWorkError(b.sort.reduce(b.pruned, nil))
	}
	return b.finish()
}

func (b *replayBuilder) finish() error {
	return b.finishWith(nil)
}

// The exclusive builder can measure the final addresses as it packs them,
// avoiding an extra runtime-index traversal and leaving the fresh cache cold.
func (b *replayBuilder) finishWith(measure func(string, index.Address) error) error {
	if b.sort != nil {
		return replayWorkError(b.bulk.BuildSorted(b.sort.ctx, func(put func(string, index.Address) error) error {
			return b.sort.reduce(b.pruned, func(key string, addr index.Address) error {
				if measure != nil {
					if err := measure(key, addr); err != nil {
						return err
					}
				}
				return put(key, addr)
			})
		}))
	}
	if b.buildErr != nil {
		return &IndexWorkError{Err: b.buildErr}
	}
	if b.direct != nil {
		err := b.direct.Finish(measure)
		b.direct = nil
		return err
	}
	b.flushTx()
	if measure == nil {
		return nil
	}
	view := b.backend.View()
	defer view.Close()
	var measureErr error
	err := view.All(func(key string, addr index.Address) bool {
		measureErr = measure(key, addr)
		return measureErr == nil
	})
	return errors.Join(replayWorkError(err), measureErr)
}

func (b *replayBuilder) abort() {
	if b.direct != nil {
		b.direct.Abort()
		b.direct = nil
	}
	if b.sort != nil {
		b.sort.close()
	}
	if b.tx != nil {
		b.tx.Abort()
		b.tx = nil
	}
}
