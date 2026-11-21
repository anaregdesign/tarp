package storage

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/anaregdesign/tarp/internal/format"
)

// SalvagedRecord holds one independently verified historical record.
type SalvagedRecord struct {
	Offset     int64
	Sequence   uint64
	Deleted    bool
	Key, Value []byte
}

// DataRange is a half-open range of DATA byte offsets.
type DataRange struct{ Start, End int64 }

// SalvageReport counts delivered records and identifies unreadable DATA bytes.
type SalvageReport struct {
	Records       uint64
	Unrecoverable []DataRange
}

type salvageReader struct {
	ctx               context.Context
	end, limit, start int64
	pending           bytes.Buffer
	synced            bool
	afterGap          bool
	span              format.RecordRange
	visit             func(SalvagedRecord) error
	report            SalvageReport
}

var salvageCRC = crc32.MakeTable(crc32.Castagnoli)

func (r *salvageReader) block(off int64, first uint16, b []byte) error {
	if b == nil {
		end := off + min(int64(format.ParityBlockBytes), r.end-off)
		gaps := r.report.Unrecoverable
		if n := len(gaps); n != 0 && gaps[n-1].End == off {
			gaps[n-1].End = end
		} else {
			r.report.Unrecoverable = append(gaps, DataRange{off, end})
		}
		r.pending.Reset()
		r.synced = false
		r.afterGap = true
		return nil
	}
	if !r.synced {
		if first == format.ParityNoAnchor {
			return nil
		}
		r.start = off + int64(first)
		b = b[first:]
		r.synced = true
	}
	r.pending.Write(b)
	for r.pending.Len() >= format.RecordHeaderBytes {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		wire := r.pending.Bytes()
		rec, err := format.DecodeRecord(wire[:format.RecordHeaderBytes], r.end-r.start)
		if err != nil {
			return fmt.Errorf("salvage record at %d: %w", r.start, err)
		}
		if rec.Bytes() > r.limit {
			return &CompatibilityError{RequiredLimits{RecordBytes: rec.Bytes()}}
		}
		if rec.Bytes() > int64(len(wire)) {
			break
		}
		payload := wire[format.RecordHeaderBytes:rec.Bytes()]
		if crc32.Checksum(payload, salvageCRC) != rec.PayloadCRC {
			return fmt.Errorf("%w: salvage record checksum at %d", ErrCorrupt, r.start)
		}
		if r.afterGap {
			err = r.span.AcceptAfterGap(rec)
		} else {
			err = r.span.Accept(rec)
		}
		if err != nil {
			return err
		}
		r.afterGap = false
		owned := bytes.Clone(payload)
		record := SalvagedRecord{
			Offset: r.start, Sequence: rec.Sequence, Deleted: rec.Kind == format.Delete,
			Key: owned[:rec.KeyBytes:rec.KeyBytes], Value: owned[rec.KeyBytes:],
		}
		if err := r.visit(record); err != nil {
			return err
		}
		r.report.Records++
		r.pending.Next(int(rec.Bytes()))
		r.start += rec.Bytes()
	}
	return nil
}

// SalvageData walks one selected sealed file while the database is closed.
// A nil gap from the codec invalidates a partial record. Only the next
// independently checksummed anchor can resume parsing.
func SalvageData(ctx context.Context, dir string, id uint64, limit int64, visit func(SalvagedRecord) error) (report SalvageReport, result error) {
	s, image, err := lockParityImage(ctx, dir, nil)
	if err != nil {
		return report, err
	}
	defer func() { result = errors.Join(result, s.Close()) }()
	var entry format.File
	for _, f := range image.Catalog.Files {
		if f.ID == id && f.HasPhysicalFile() {
			entry = f
			break
		}
	}
	if !entry.HasPhysicalFile() {
		return report, fmt.Errorf("sealed DATA %d: %w", id, os.ErrNotExist)
	}
	side, err := OpenRegular(filepath.Join(s.Dir, parityName(image.Current.UUID, id)))
	if err != nil {
		return report, err
	}
	defer func() { result = errors.Join(result, side.Close()) }()
	// Validate the entire anchor directory before any externally visible output.
	metadata := bufio.NewReaderSize(side, parityBufferSize(entry.Bytes))
	if err := format.ValidateParity(ctx, metadata, image.Current.UUID, entry); err != nil {
		return report, err
	}
	if _, err := side.Seek(0, io.SeekStart); err != nil {
		return report, err
	}
	metadata.Reset(side)
	var source io.ReaderAt = bytes.NewReader(nil)
	fd, err := OpenRegular(image.Files[id].Path)
	if err == nil {
		defer func() { result = errors.Join(result, fd.Close()) }()
		source = fd
	} else if !errors.Is(err, os.ErrNotExist) {
		return report, err
	}
	reader := salvageReader{ctx: ctx, end: entry.Bytes, limit: limit, visit: visit,
		span: format.NewRecordRange(entry, image.Catalog.Cutoff)}
	err = format.WalkParityBlocks(ctx, source, metadata, image.Current.UUID, entry, reader.block)
	if err == nil && reader.pending.Len() != 0 {
		err = fmt.Errorf("%w: incomplete final salvage record", ErrCorrupt)
	}
	if err == nil && !reader.afterGap {
		err = reader.span.Finish()
	}
	return reader.report, err
}
