package engine

import "github.com/anaregdesign/tarp/internal/format"

// CacheStats describes this DB's exact LRU and its cumulative activity since Open.
type CacheStats struct {
	UsedBytes  uint64 // Logical entry charges, including cache metadata.
	LimitBytes uint64 // Effective cache budget.
	Hits       uint64 // Distinct keys per Get or BatchGet call found in the LRU.
	Misses     uint64 // Distinct keys per Get or BatchGet call found only in the live index.
	Evictions  uint64 // Entries evicted to stay within LimitBytes.
}

// Stats is a point-in-time copy of the DB's published in-memory statistics.
// Byte gauges describe encoded data or logical cache charges.
type Stats struct {
	LiveKeys                uint64 // Number of keys present.
	LiveRecordBytes         uint64 // Encoded bytes of the latest live Put for each key.
	SelectedSegments        uint64 // Selected physical DATA files, including the active log.
	SelectedDataBytes       uint64 // Committed lengths of selected DATA files, including headers.
	ObsoletePutBytes        uint64 // Superseded or deleted Put records in the selected DATA.
	TombstoneBytes          uint64 // Retained encoded Delete records in the selected DATA.
	Cache                   CacheStats
	MissingKeys             uint64 // Distinct keys per Get or BatchGet call absent from the live index.
	BatchGetDuplicateInputs uint64 // Input positions beyond the first for each encoded key at BatchGet snapshot capture.
	WriteCommits            uint64 // Foreground write calls published in memory since Open.
	WrittenRecords          uint64 // Foreground Put and Delete records in those commits.
}

// Stats also returns the retained final snapshot after Close.
func (db *DB) Stats() Stats {
	db.state.Lock()
	defer db.state.Unlock()
	return db.statsLocked()
}

func (db *DB) statsLocked() Stats {
	s := db.stats
	if db.cache != nil {
		s.LiveKeys = uint64(len(db.index))
		s.Cache.UsedBytes = uint64(db.cache.Bytes())
		s.Cache.LimitBytes = uint64(db.opts.CacheBytes)
		s.Cache.Evictions = db.cache.Evictions()
		s.ObsoletePutBytes = s.SelectedDataBytes - s.SelectedSegments*format.SegmentHeaderBytes - s.LiveRecordBytes - s.TombstoneBytes
	}
	return s
}
