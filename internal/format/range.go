package format

// SealedEnd returns the sequence covered by the checkpoint and all sealed intervals.
func (c Catalog) SealedEnd() uint64 {
	if n := len(c.Files); n > 0 && c.Files[n-1].Kind != Snapshot {
		return c.Files[n-1].Last
	}
	return c.Cutoff
}

// ActiveFile derives the final dense interval from a validated catalog and frontier.
func (c Catalog) ActiveFile(current Frontier) (File, error) {
	if c.UUID != current.UUID || c.ActiveID != current.ActiveID || c.PrunedThrough > current.Sequence {
		return File{}, bad("catalog/frontier identity or pruning boundary")
	}
	if c.Index.ID != 0 && c.Index.ID == current.ManifestID {
		return File{}, bad("catalog and index share an ID")
	}
	for _, file := range c.Files {
		if file.HasPhysicalFile() && file.ID == current.ManifestID {
			return File{}, bad("catalog and data file share an ID")
		}
	}
	end := c.SealedEnd()
	if end > current.Sequence || current.ActiveEnd < SegmentHeaderBytes {
		return File{}, bad("active log coverage")
	}
	file := File{ID: current.ActiveID, Bytes: current.ActiveEnd, Kind: Log}
	if end == current.Sequence {
		if current.ActiveEnd != SegmentHeaderBytes {
			return File{}, bad("unexpected active log records")
		}
		return file, nil
	}
	if current.Sequence-end > uint64((current.ActiveEnd-SegmentHeaderBytes)/(RecordHeaderBytes+1)) {
		return File{}, bad("active interval cannot fit committed prefix")
	}
	file.First, file.Last = end+1, current.Sequence
	return file, nil
}

// RecordRange validates record order and membership for one selected file.
type RecordRange struct {
	file   File
	cutoff uint64
	last   uint64
}

func NewRecordRange(file File, cutoff uint64) RecordRange {
	r := RecordRange{file: file, cutoff: cutoff}
	if file.First != 0 {
		r.last = file.First - 1
	}
	return r
}

func (r *RecordRange) Accept(record Record) error {
	if r.file.Kind == Snapshot {
		if record.Kind != Put || record.Sequence == 0 || record.Sequence > r.cutoff {
			return bad("snapshot record outside checkpoint")
		}
		return nil
	}
	if !r.file.HasPhysicalFile() || record.Sequence <= r.last || record.Sequence > r.file.Last {
		return bad("record outside selected interval")
	}
	if r.file.Kind == Log && record.Sequence != r.last+1 {
		return bad("noncontiguous raw log")
	}
	r.last = record.Sequence
	return nil
}

func (r *RecordRange) Finish() error {
	if r.file.Kind == Log && r.last != r.file.Last {
		return bad("incomplete raw log")
	}
	if r.file.Kind == Cleaned && r.file.HasPhysicalFile() && r.last < r.file.First {
		return bad("empty physical cleaned file")
	}
	return nil
}
