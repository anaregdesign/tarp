package format

// SealedThroughSequence returns the last user sequence covered by the full DATA
// checkpoint and all sealed log intervals, inclusive.
func (c Catalog) SealedThroughSequence() uint64 {
	if n := len(c.Entries); n > 0 && c.Entries[n-1].Kind != SnapshotKind {
		return c.Entries[n-1].LastSequence
	}
	return c.CheckpointCutoffSequence
}

// ActiveEntry derives the active log descriptor from a validated catalog and
// CURRENT. The MANIFEST stores sealed entries separately from the active log.
func (c Catalog) ActiveEntry(current Frontier) (CatalogEntry, error) {
	if c.UUID != current.UUID || c.ActiveID != current.ActiveID || c.PrunedThroughSequence > current.LastUserSequence {
		return CatalogEntry{}, bad("catalog/frontier identity or pruning boundary")
	}
	if c.Index.ID != 0 && c.Index.ID == current.ManifestID {
		return CatalogEntry{}, bad("catalog and index share an ID")
	}
	for _, entry := range c.Entries {
		if entry.HasPhysicalFile() && entry.ID == current.ManifestID {
			return CatalogEntry{}, bad("catalog and data file share an ID")
		}
	}
	sealedThrough := c.SealedThroughSequence()
	if sealedThrough > current.LastUserSequence || current.ActiveCommittedEndOffset < SegmentHeaderBytes {
		return CatalogEntry{}, bad("active log coverage")
	}
	entry := CatalogEntry{ID: current.ActiveID, Bytes: current.ActiveCommittedEndOffset, Kind: RawLogKind}
	if sealedThrough == current.LastUserSequence {
		if current.ActiveCommittedEndOffset != SegmentHeaderBytes {
			return CatalogEntry{}, bad("unexpected active log records")
		}
		return entry, nil
	}
	if current.LastUserSequence-sealedThrough > uint64((current.ActiveCommittedEndOffset-SegmentHeaderBytes)/(RecordHeaderBytes+1)) {
		return CatalogEntry{}, bad("active interval cannot fit committed prefix")
	}
	entry.FirstSequence, entry.LastSequence = sealedThrough+1, current.LastUserSequence
	return entry, nil
}

// RecordSequenceValidator checks record sequences against one catalog entry.
type RecordSequenceValidator struct {
	entry  CatalogEntry
	cutoff uint64
	last   uint64
}

func NewRecordSequenceValidator(entry CatalogEntry, cutoff uint64) RecordSequenceValidator {
	r := RecordSequenceValidator{entry: entry, cutoff: cutoff}
	if entry.FirstSequence != 0 {
		r.last = entry.FirstSequence - 1
	}
	return r
}

func (r *RecordSequenceValidator) Accept(record RecordHeader) error {
	return r.accept(record, false)
}

// AcceptAfterGap checks the first surviving record after an unreadable region.
// A raw log may skip sequences across that region, but still increases within
// the selected interval. Call Accept for subsequent contiguous records.
func (r *RecordSequenceValidator) AcceptAfterGap(record RecordHeader) error {
	return r.accept(record, true)
}

func (r *RecordSequenceValidator) accept(record RecordHeader, afterGap bool) error {
	if r.entry.Kind == SnapshotKind {
		if record.Kind != Put || record.Sequence == 0 || record.Sequence > r.cutoff {
			return bad("snapshot record outside checkpoint")
		}
		return nil
	}
	if !r.entry.HasPhysicalFile() || record.Sequence <= r.last || record.Sequence > r.entry.LastSequence {
		return bad("record outside selected interval")
	}
	if r.entry.Kind == RawLogKind && !afterGap && record.Sequence != r.last+1 {
		return bad("noncontiguous raw log")
	}
	r.last = record.Sequence
	return nil
}

func (r *RecordSequenceValidator) Finish() error {
	if r.entry.Kind == RawLogKind && r.last != r.entry.LastSequence {
		return bad("incomplete raw log")
	}
	if r.entry.Kind == CleanedLogKind && r.entry.HasPhysicalFile() && r.last < r.entry.FirstSequence {
		return bad("empty physical cleaned segment")
	}
	return nil
}
