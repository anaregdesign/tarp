package engine

import (
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// dataTotals describes a set of physical DATA files selected by a frontier.
type dataTotals struct {
	segments   uint64
	bytes      uint64
	tombstones uint64
}

func (d *dataTotals) addSegment(size, tombstones int64) {
	d.segments++
	d.bytes += uint64(size)
	d.tombstones += uint64(tombstones)
}

// initDataStats runs once before this DB admits callers.
func (db *DB) initDataStats() {
	var selected dataTotals
	for _, entry := range db.store.Catalog.Entries {
		if entry.HasPhysicalFile() {
			selected.addSegment(entry.Bytes, db.store.Segments[entry.ID].DeleteBytes.Load())
		}
	}
	active := db.store.Segments[db.store.Current.ActiveID]
	selected.addSegment(db.store.Current.ActiveCommittedEndOffset, active.DeleteBytes.Load())
	db.stats.SelectedSegments = selected.segments
	db.stats.SelectedDataBytes = selected.bytes
	db.stats.TombstoneBytes = selected.tombstones
}

// replaceDataStats applies a published catalog change while the writer gate
// is held. Unchanged files, including a concurrently written tail, retain
// their accounting. The state lock covers only the constant-time update.
func (db *DB) replaceDataStats(old, next dataTotals) {
	db.state.Lock()
	defer db.state.Unlock()
	db.replaceDataStatsLocked(old, next)
}

func (db *DB) replaceDataStatsLocked(old, next dataTotals) {
	db.stats.SelectedSegments = db.stats.SelectedSegments - old.segments + next.segments
	db.stats.SelectedDataBytes = db.stats.SelectedDataBytes - old.bytes + next.bytes
	db.stats.TombstoneBytes = db.stats.TombstoneBytes - old.tombstones + next.tombstones
}

// sealActive seals the active log segment for a DATA checkpoint, an index
// checkpoint or parity protection. The previously active segment stays selected,
// and the new active segment contributes its header to selected DATA bytes.
func (db *DB) sealActive() error {
	previous := db.store.Current.ActiveID
	err := db.store.Rotate()
	if db.store.Current.ActiveID != previous {
		db.notifyParity()
		db.replaceDataStats(dataTotals{}, dataTotals{segments: 1, bytes: format.SegmentHeaderBytes})
	}
	return err
}

// checkpointDataTotals counts only the immutable prefix being replaced. The
// active file captured alongside it belongs to the tail of the new catalog.
func checkpointDataTotals(capture *storage.Capture, outputs []format.CatalogEntry) (old, next dataTotals) {
	for _, source := range capture.Segments {
		if source.Entry.ID != capture.Current.ActiveID {
			old.addSegment(source.Entry.Bytes, source.Segment.DeleteBytes.Load())
		}
	}
	for _, entry := range outputs {
		next.addSegment(entry.Bytes, 0)
	}
	return old, next
}
