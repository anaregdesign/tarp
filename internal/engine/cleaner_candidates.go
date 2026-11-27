package engine

import (
	"container/list"
	"errors"
	"math"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

const (
	cleanerCandidateWindow = 32
	cleanerInventoryWindow = 128
	cleanerGroupEntries    = 8
	cleanerRepackEntries   = 32
)

// The writer gate owns the inventory and candidate queue. Updates enqueue a
// segment only when its current DATA could qualify for cleaning or repacking.
// A pass considers a bounded number of candidates before letting writers run.
type cleanerCandidates struct {
	files      map[uint64]*cleanerCandidate
	pending    list.List
	pinned     list.List
	seeded     int
	ready      bool
	debt       uint64
	pinnedDebt uint64
}

type cleanerCandidate struct {
	entry   format.CatalogEntry
	pos     int
	pending *list.Element
	pinned  *list.Element
	pinGen  uint64
	debt    uint64
}

func (q *cleanerCandidates) track(s *storage.Store, entry format.CatalogEntry, position int, target int64) {
	if !entry.HasPhysicalFile() {
		return
	}
	if q.files == nil {
		q.files = make(map[uint64]*cleanerCandidate)
	}
	file := q.files[entry.ID]
	if file == nil {
		file = &cleanerCandidate{entry: entry}
		q.files[entry.ID] = file
	}
	file.pos = position
	q.changed(file, s.Segments[entry.ID], target)
}

func (q *cleanerCandidates) changed(file *cleanerCandidate, segment *storage.DataSegment, target int64) {
	if file == nil || segment == nil {
		return
	}
	retained := retainedRecordBytes(segment)
	q.debt -= file.debt
	if file.pinned != nil {
		q.pinnedDebt -= file.debt
	}
	file.debt = 0
	if bytes := uint64(file.entry.Bytes); retained <= bytes/2 {
		file.debt = bytes - min(bytes, replacementBytes(retained))
		q.debt += file.debt
		if file.pinned != nil {
			q.pinnedDebt += file.debt
		}
	}
	if file.debt != 0 || file.entry.Bytes <= target/4 {
		q.enqueue(file)
	}
}

func (q *cleanerCandidates) enqueue(file *cleanerCandidate) {
	if file != nil && file.pending == nil && file.pinned == nil {
		file.pending = q.pending.PushBack(file)
	}
}

func (q *cleanerCandidates) deferPin(file *cleanerCandidate, generation uint64) {
	if file.pinned == nil {
		file.pinGen = generation
		file.pinned = q.pinned.PushBack(file)
		q.pinnedDebt += file.debt
	}
}

func (q *cleanerCandidates) resumePins(generation uint64, pressure bool) {
	for n := 0; n < cleanerCandidateWindow; n++ {
		next := q.pinned.Front()
		if next == nil {
			return
		}
		file := next.Value.(*cleanerCandidate)
		if pressure && file.pinGen == generation {
			return
		}
		q.pinned.Remove(next)
		file.pinned = nil
		q.pinnedDebt -= file.debt
		q.enqueue(file)
	}
}

func (q *cleanerCandidates) pinsReady(generation uint64, pressure bool) bool {
	first := q.pinned.Front()
	return first != nil && (!pressure || first.Value.(*cleanerCandidate).pinGen != generation)
}

func (q *cleanerCandidates) remove(id uint64) {
	if file := q.files[id]; file != nil {
		if file.pending != nil {
			q.pending.Remove(file.pending)
		}
		if file.pinned != nil {
			q.pinned.Remove(file.pinned)
			q.pinnedDebt -= file.debt
		}
		q.debt -= file.debt
		delete(q.files, id)
	}
}

func (q *cleanerCandidates) take() []*cleanerCandidate {
	files := make([]*cleanerCandidate, 0, min(q.pending.Len(), cleanerCandidateWindow))
	for len(files) < cap(files) {
		front := q.pending.Front()
		file := front.Value.(*cleanerCandidate)
		q.pending.Remove(front)
		file.pending = nil
		files = append(files, file)
	}
	return files
}

func (db *DB) cleanerSealed() {
	entries := db.store.CatalogEntries()
	if n := entries.Len(); n != 0 {
		db.cleaner.candidates.track(db.store, entries.At(n-1), n-1, db.opts.TargetSegmentBytes)
	}
}

func (db *DB) seedCleanerInventory(maxEntries int) bool {
	q := &db.cleaner.candidates
	if q.ready {
		return true
	}
	entries := db.store.CatalogEntries()
	if q.seeded == 0 && maxEntries >= entries.Len() {
		for entry := range entries.All() {
			q.track(db.store, entry, q.seeded, db.opts.TargetSegmentBytes)
			q.seeded++
		}
		q.ready = true
		return true
	}
	for end := q.seeded + min(maxEntries, entries.Len()-q.seeded); q.seeded < end; q.seeded++ {
		q.track(db.store, entries.At(q.seeded), q.seeded, db.opts.TargetSegmentBytes)
	}
	q.ready = q.seeded == entries.Len()
	return q.ready
}

func (db *DB) resetCleanerInventory() {
	db.cleaner.candidates = cleanerCandidates{}
}

// Close has drained foreground calls and stopped maintenance. Preserve the
// numeric status snapshot while dropping file-sized bookkeeping.
func (db *DB) releaseCleanerCandidates() {
	db.resetCleanerInventory()
	db.cleaner.blocked = nil
	db.cleaner.damageMu.Lock()
	db.cleaner.damage = nil
	db.cleaner.damageMu.Unlock()
}

type cleanerWindow struct {
	entries []format.CatalogEntry
	start   int
	through int
}

func (db *DB) candidateWindows(files []*cleanerCandidate) []cleanerWindow {
	db.writer.Lock()
	view := db.store.CatalogEntries()
	db.writer.Unlock()
	var windows []cleanerWindow
	for _, file := range files {
		if file == nil {
			continue
		}
		p := file.pos
		if p >= view.Len() || view.At(p) != file.entry {
			p = view.Index(file.entry)
		}
		if p < 0 {
			continue
		}
		start := max(0, p-cleanerRepackEntries+1)
		end := p + min(cleanerRepackEntries, view.Len()-p)
		windows = append(windows, cleanerWindow{entries: view.Slice(start, end), start: start, through: p - start})
	}
	return windows
}

func (w *cleaner) rememberDamage(id uint64, next *storage.UnavailableDataError) {
	prior := w.blocked[id]
	if next == nil {
		delete(w.blocked, id)
		if prior != nil {
			w.recoverPending.Store(true)
		}
		return
	}
	if w.blocked == nil {
		w.blocked = make(map[uint64]*storage.UnavailableDataError)
	}
	w.blocked[id] = next
	if errors.Is(next.Err, format.ErrUnrecoverableDataBlocks) &&
		(prior == nil || !errors.Is(prior.Err, format.ErrUnrecoverableDataBlocks)) {
		w.recoverPending.Store(true)
	}
}

func (db *DB) observeCleanerDamage() {
	w := &db.cleaner
	if !w.candidates.ready {
		return
	}
	w.damageMu.Lock()
	damage := w.damage
	w.damage = nil
	w.damageMu.Unlock()
	for id := range w.blocked {
		if w.candidates.files[id] == nil {
			delete(w.blocked, id)
		}
	}
	for id, segment := range damage {
		if file := w.candidates.files[id]; file != nil {
			unavailable := segment.UnavailableData()
			w.rememberDamage(id, unavailable)
			if unavailable == nil {
				w.candidates.changed(file, segment, db.opts.TargetSegmentBytes)
			}
		}
	}
}

func chooseCandidateGroups(s *storage.Store, windows []cleanerWindow, target int64, metadata uint64, blocked map[uint64]*storage.UnavailableDataError, pressure bool) (inputs []format.CatalogEntry, staging uint64) {
	groupStart, repackStart := math.MaxInt, math.MaxInt
	var repack []format.CatalogEntry
	var repackBytes int64
	for _, window := range windows {
		for i := 0; i <= window.through; i++ {
			entries := window.entries[i:]
			entry := entries[0]
			if !entry.HasPhysicalFile() {
				continue
			}
			position := window.start + i
			if window.through-i < cleanerGroupEntries && position < groupStart && entry.Kind != format.SnapshotKind && entry.Bytes <= target {
				group, bytes := groupedLogCleaningAt(s, entries, uint64(target), metadata)
				if len(group) > window.through-i && !(pressure && hasPinnedInput(s, group)) {
					inputs, staging, groupStart = group, bytes, position
				}
			}
			if position < repackStart && entry.Bytes <= target/4 {
				group, bytes := smallSegmentRepackAt(entries, target, blocked)
				if len(group) > window.through-i && !(pressure && hasPinnedInput(s, group)) {
					repack, repackBytes, repackStart = group, bytes, position
				}
			}
		}
	}
	if len(inputs) != 0 {
		return inputs, staging
	}
	if len(repack) != 0 {
		return repack, uint64(repackBytes) + metadata
	}
	return nil, 0
}

func hasPinnedInput(s *storage.Store, inputs []format.CatalogEntry) bool {
	for _, entry := range inputs {
		if f := s.Segments[entry.ID]; entry.HasPhysicalFile() && f != nil && s.HasDataRetentionPin(f) {
			return true
		}
	}
	return false
}
