package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/entrysize"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
)

// Immutable once the snapshot has been certified. Boundaries can be much
// larger than a typical record. Keep only a fixed-size summary per file.
type snapshotRange struct {
	rows, lastOffset      int64
	firstBytes, lastBytes int
	digest                [sha256.Size]byte
}

type snapshotBoundsBuilder struct {
	first, last      string
	rows, lastOffset int64
}

func (b *snapshotBoundsBuilder) add(key string, offset int64) {
	if b.rows == 0 {
		b.first = key
	}
	b.rows++
	b.last, b.lastOffset = key, offset
}

func boundsDigest(first, last string) [sha256.Size]byte {
	h := sha256.New()
	var n [8]byte
	var chunk [4096]byte
	for _, key := range [...]string{first, last} {
		binary.LittleEndian.PutUint64(n[:], uint64(len(key)))
		h.Write(n[:])
		for len(key) > 0 {
			copied := copy(chunk[:], key)
			h.Write(chunk[:copied])
			key = key[copied:]
		}
	}
	var sum [sha256.Size]byte
	h.Sum(sum[:0])
	return sum
}

func (b *snapshotBoundsBuilder) complete(f *DataSegment, fences *fenceCache) {
	f.snapshotRange = &snapshotRange{
		rows: b.rows, lastOffset: b.lastOffset,
		firstBytes: len(b.first), lastBytes: len(b.last), digest: boundsDigest(b.first, b.last),
	}
	fences.put(f.ID, b.first, b.last)
}

// Only the serialized maintenance slot uses the cache. Entries are hints,
// scoped to the open Store, where each file ID is allocated exactly once.
const snapshotFenceBudget = 256 << 10

type fenceCache struct{ keys *cache.Cache }

func (c *fenceCache) put(id uint64, first, last string) {
	if len(first) > snapshotFenceBudget || len(last) > snapshotFenceBudget ||
		entrysize.CacheCharge(8, int64(len(first)+len(last))) > snapshotFenceBudget {
		return
	}
	if c.keys == nil {
		c.keys = cache.New(snapshotFenceBudget)
	}
	var k [8]byte
	binary.LittleEndian.PutUint64(k[:], id)
	bounds := make([]byte, len(first)+len(last))
	copy(bounds, first)
	copy(bounds[len(first):], last)
	c.keys.Put(string(k[:]), bounds)
}

func (c *fenceCache) get(id uint64, firstBytes int) (first, last string, ok bool) {
	if c.keys == nil {
		return "", "", false
	}
	var k [8]byte
	binary.LittleEndian.PutUint64(k[:], id)
	bounds, ok := c.keys.Get(k[:])
	if !ok {
		return "", "", false
	}
	return string(bounds[:firstBytes]), string(bounds[firstBytes:]), true
}

// HasOrderedBase is read under the writer gate. Log rotations and INDEX
// publications leave this proof intact. An empty database is already ordered.
func (s *Store) HasOrderedBase() bool {
	return s.orderedSnapshot || s.Current.LastUserSequence == 0
}

// OrderedCleanupPending checks only objects already known to be retired.
func (s *Store) OrderedCleanupPending() bool {
	return s.retireSyncPending || len(s.retired) != 0 || len(s.retiredCatalogs) != 0 || len(s.retiredIndexes) != 0
}

func (c *DataCheckpoint) HasOrderedBase() bool { return c.capture.orderedSnapshot || c.certifiedBase }
func (c *DataCheckpoint) snapshotRows(src CapturedSegment) int64 {
	return src.Segment.snapshotRange.rows
}

func (c *DataCheckpoint) findSnapshot(entry format.CatalogEntry) (CapturedSegment, int, error) {
	if c.ordinals == nil {
		c.ordinals = make(map[uint64]int)
		for pos, src := range c.capture.Segments {
			if src.Entry.Kind != format.SnapshotKind {
				break
			}
			c.ordinals[src.Entry.ID] = pos
		}
	}
	pos, ok := c.ordinals[entry.ID]
	if !ok || c.capture.Segments[pos].Entry != entry {
		return CapturedSegment{}, 0, fmt.Errorf("%w: snapshot outside captured base", ErrCorrupt)
	}
	return c.capture.Segments[pos], pos, nil
}

// certifyCapturedBase establishes order once after Open. Full record and file
// digest validation precede use of any fence. Unordered input is valid DATA.
func (c *DataCheckpoint) certifyCapturedBase(ctx context.Context, live index.View) (bool, error) {
	if c.HasOrderedBase() {
		return true, nil
	}
	var previous string
	for _, src := range c.capture.Segments {
		if src.Entry.Kind != format.SnapshotKind {
			break
		}
		if err := ctx.Err(); err != nil {
			return false, err
		}
		fd, _, _, err := openSelected(src.Segment.Path, c.capture.Current.UUID, src.Entry, false)
		if err != nil {
			return false, selectedReadFailure(src.Entry.ID, err)
		}
		var bounds snapshotBoundsBuilder
		var mismatch bool
		ordered := true
		err = c.store.IO.Do("compact.key.certify", func() error {
			return scanSelectedRecords(ctx, fd, src.Entry, c.capture.Catalog.CheckpointCutoffSequence, func(off int64, record format.RecordHeader, key string) error {
				if live != nil && c.capture.Catalog.CheckpointCutoffSequence == c.capture.Current.LastUserSequence {
					addr, found, err := live.Lookup(key)
					if err != nil {
						return &IndexWorkError{Err: err}
					}
					expected := index.Address{FileID: src.Entry.ID, RecordOffset: off, RecordBytes: record.EncodedRecordBytes(), ValueBytes: record.ValueBytes, Sequence: record.Sequence}
					if !found || addr != expected {
						mismatch = true
					}
				}
				if previous >= key {
					ordered = false
				}
				previous = key
				bounds.add(key, off)
				return nil
			})
		})
		if err = errors.Join(selectedReadFailure(src.Entry.ID, err), fd.Close()); err != nil {
			return false, err
		}
		if mismatch {
			return false, fmt.Errorf("%w: captured index disagrees with snapshot record", ErrCorrupt)
		}
		if !ordered {
			return false, nil
		}
		if bounds.rows == 0 {
			return false, fmt.Errorf("%w: empty physical snapshot", ErrCorrupt)
		}
		bounds.complete(src.Segment, &c.store.fences)
	}
	c.certifiedBase = true
	return true, nil
}

// A cold probe authenticates both exact keys against their digest from the
// checked source stream. It need not fetch a potentially large value payload.
func (c *DataCheckpoint) snapshotBounds(ctx context.Context, entry format.CatalogEntry) (first, last string, err error) {
	if !c.HasOrderedBase() {
		return "", "", fmt.Errorf("%w: unverified snapshot range", ErrCorrupt)
	}
	src, _, err := c.findSnapshot(entry)
	if err != nil {
		return "", "", err
	}
	r := src.Segment.snapshotRange
	if r == nil || r.rows < 1 {
		return "", "", fmt.Errorf("%w: missing snapshot range", ErrCorrupt)
	}
	if first, last, ok := c.store.fences.get(entry.ID, r.firstBytes); ok {
		return first, last, nil
	}
	fd, _, _, err := openSelected(src.Segment.Path, c.capture.Current.UUID, entry, false)
	if err != nil {
		return "", "", selectedReadFailure(entry.ID, err)
	}
	defer func() { err = errors.Join(selectedReadFailure(entry.ID, err), fd.Close()) }()
	read := func(off int64, n int, final bool) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		var header [format.RecordHeaderBytes]byte
		if _, err := fd.ReadAt(header[:], off); err != nil {
			return "", err
		}
		rec, err := format.DecodeRecordHeader(header[:], entry.Bytes-off)
		if err != nil {
			return "", err
		}
		if rec.Kind != format.Put || rec.Sequence > c.capture.Catalog.CheckpointCutoffSequence || rec.KeyBytes != int64(n) || final && rec.EncodedRecordBytes() != entry.Bytes-off {
			return "", fmt.Errorf("%w: snapshot boundary record", ErrCorrupt)
		}
		var key strings.Builder
		key.Grow(n)
		var buffer [32 << 10]byte
		for key.Len() < n {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			chunk := buffer[:min(n-key.Len(), len(buffer))]
			if _, err := fd.ReadAt(chunk, off+format.RecordHeaderBytes+int64(key.Len())); err != nil {
				return "", err
			}
			key.Write(chunk)
		}
		return key.String(), nil
	}
	err = c.store.IO.Do("compact.key.fence", func() error {
		first, err = read(format.SegmentHeaderBytes, r.firstBytes, r.rows == 1)
		if err != nil {
			return err
		}
		last = first
		if r.rows > 1 {
			last, err = read(r.lastOffset, r.lastBytes, true)
			if err != nil {
				return err
			}
		}
		if boundsDigest(first, last) != r.digest {
			return fmt.Errorf("%w: snapshot boundary digest", ErrCorrupt)
		}
		return nil
	})
	if err == nil {
		c.store.fences.put(entry.ID, first, last)
	}
	return first, last, err
}
