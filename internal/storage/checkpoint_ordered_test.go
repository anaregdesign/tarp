package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"slices"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func TestCopyByKeyCarriesUnchangedRangeAfterInteriorInsertAndDelete(t *testing.T) {
	s, err := openTestStore(t, t.TempDir(), Config{MaxRecordBytes: 1024, ValueCacheBudgetBytes: 2048, TargetSegmentBytes: 1024, Handles: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	live := memory.New()
	tx := live.Begin()
	for _, key := range []string{"z", "a"} {
		loc, err := s.Append(format.Put, []byte(key), []byte(key), func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Put(key, loc.Address()); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	var first []format.CatalogEntry
	for pass := 0; pass < 2; pass++ {
		if pass != 0 {
			if _, err := s.Append(format.Put, []byte("k"), []byte("temporary"), func() error { return nil }); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(format.Delete, []byte("k"), nil, func() error { return nil }); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Rotate(); err != nil {
			t.Fatal(err)
		}
		capture, err := s.Capture()
		if err != nil {
			t.Fatal(err)
		}
		moves := s.NewRelocations()
		cp := s.NewDataCheckpoint(capture)
		if pass == 0 {
			v := live.View()
			err = cp.CopyByKey(t.Context(), v, 2, moves, s.ReserveSnapshotSegment)
			v.Close()
		} else {
			err = cp.CopyByKey(t.Context(), nil, 2, moves, s.ReserveSnapshotSegment)
		}
		if err != nil {
			t.Fatal(err)
		}
		if pass == 0 {
			first = slices.Clone(cp.Entries())
			if len(first) != 1 || moves.count != 2 {
				t.Fatalf("initial entries %d moves %d", len(first), moves.count)
			}
		} else if !slices.Equal(first, cp.Entries()) || moves.count != 0 || len(cp.privateEntries) != 0 {
			t.Fatalf("unchanged range acquired private outputs or relocations: %v, %d", cp.Entries(), moves.count)
		}
		if err := cp.Protect(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := cp.Install(); err != nil {
			t.Fatal(err)
		}
		capture.Release()
		if err := moves.Close(); err != nil {
			t.Fatal(err)
		}
	}
	image, err := Validate(t.Context(), s.Dir)
	if err != nil || image.LiveKeys != 2 {
		t.Fatalf("verify image: %v, %v", image, err)
	}
}

func TestCopyByKeyStreamsOversizedWinnerAndRetriesInsufficientStaging(t *testing.T) {
	value := bytes.Repeat([]byte("v"), orderedWindowBytes+1)
	target := int64(len(value)) + 1024
	s, err := openTestStore(t, t.TempDir(), Config{ValueCacheBudgetBytes: target * 2, MaxRecordBytes: target - 64, TargetSegmentBytes: target, Handles: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	live := memory.New()
	tx := live.Begin()
	for _, row := range []struct {
		key   string
		value []byte
	}{{"z", nil}, {"a", value}} {
		loc, err := s.Append(format.Put, []byte(row.key), row.value, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Put(row.key, loc.Address()); err != nil {
			t.Fatal(err)
		}
	}
	tx.Commit()
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	before := s.Current
	capture, err := s.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	v := live.View()
	defer v.Close()
	for _, exhaust := range []bool{true, false} {
		cp := s.NewDataCheckpoint(capture)
		if exhaust {
			cp.scratch = &orderedScratchBudget{remaining: 1}
		}
		moves := s.NewRelocations()
		err = cp.CopyByKey(t.Context(), v, 2, moves, s.ReserveSnapshotSegment)
		if exhaust {
			if !errors.Is(err, syscall.ENOSPC) || s.Current != before {
				t.Fatalf("partial checkpoint published: %v", err)
			}
			if err := cp.Discard(); err != nil {
				t.Fatal(err)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if moves.count != 2 {
				t.Fatalf("copied %d", moves.count)
			}
			if err := cp.Protect(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := cp.Install(); err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, entry := range cp.Entries() {
				fd, _, _, err := openSelected(s.Segments[entry.ID].Path, s.Current.UUID, entry, false)
				if err != nil {
					t.Fatal(err)
				}
				err = scanSelectedRecords(t.Context(), fd, entry, s.Catalog.CheckpointCutoffSequence, func(off int64, r format.RecordHeader, key string) error {
					got = append(got, key)
					v, err := format.ReadValue(fd, off, r.EncodedRecordBytes(), key, r.Sequence)
					if err == nil && key == "a" && !bytes.Equal(v, value) {
						t.Fatal("oversized value changed")
					}
					return err
				})
				if err = errors.Join(err, fd.Close()); err != nil {
					t.Fatal(err)
				}
			}
			if !slices.Equal(got, []string{"a", "z"}) {
				t.Fatal(got)
			}
		}
		if err := moves.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// A second read can encounter a completely framed alternate Put (including
// its own valid CRC). The initial sealed digest identified the first payload.
func TestCopyByKeyRejectsChangedPayloadBetweenVerifiedScanAndCopy(t *testing.T) {
	fs := &IO{}
	cfg := Config{MaxRecordBytes: 128, ValueCacheBudgetBytes: 256, TargetSegmentBytes: 256, Handles: 1}
	s, err := openTestStore(t, t.TempDir(), cfg, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loc, err := s.Append(format.Put, []byte("a"), []byte("first"), func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	cap, err := s.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer cap.Release()
	live := memory.New()
	tx := live.Begin()
	if err := tx.Put("a", loc.Address()); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	v := live.View()
	defer v.Close()
	moves := s.NewRelocations()
	defer moves.Close()
	cp := s.NewDataCheckpoint(cap)
	changed := false
	fs.Hook = func(point string) error {
		if point != "compact.scan:after" || changed {
			return nil
		}
		changed = true
		fd, err := os.OpenFile(loc.Segment.Path, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, err = fd.WriteAt(format.EncodeRecord(format.Put, loc.Sequence, []byte("a"), []byte("other")), loc.RecordOffset)
		return errors.Join(err, fd.Close())
	}
	err = cp.CopyByKey(t.Context(), v, 1, moves, s.ReserveSnapshotSegment)
	if !changed || !errors.Is(err, ErrCorrupt) || failedDataRead(err) == nil || failedDataRead(err).id != loc.Segment.ID {
		t.Fatalf("wrong payload/selected source result: %v", err)
	}
	if err := cp.Discard(); err != nil {
		t.Fatal(err)
	}
	if s.Current != cap.Current {
		t.Fatal("published bytes that disagreed with the captured source")
	}
}

func TestCopyByKeySeparatesInputPermissionFailuresFromPrivateWorkspace(t *testing.T) {
	for _, tc := range []struct {
		point                    string
		large, source, indexWork bool
		err                      error
	}{
		{point: "compact.scan:before", source: true, err: syscall.EACCES},
		{point: "compact.source.open:after", source: true, err: syscall.EPERM},
		{point: "compact.source.open:after", large: true, source: true, err: syscall.EACCES},
		{point: "compact.write:before", err: syscall.EACCES},
		{point: "compact.write:before", large: true, err: syscall.EPERM},
		{point: "index.moves.write:before", indexWork: true, err: syscall.EACCES},
	} {
		t.Run(tc.point, func(t *testing.T) {
			fs := &IO{}
			target := int64(orderedWindowBytes + 1024)
			cfg := Config{MaxRecordBytes: target - 64, ValueCacheBudgetBytes: target, TargetSegmentBytes: target, Handles: 1}
			s, err := openTestStore(t, t.TempDir(), cfg, fs)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var value []byte
			if tc.large {
				value = make([]byte, orderedWindowBytes)
			}
			loc, err := s.Append(format.Put, []byte("one"), value, func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Rotate(); err != nil {
				t.Fatal(err)
			}
			capture, err := s.Capture()
			if err != nil {
				t.Fatal(err)
			}
			defer capture.Release()
			live := memory.New()
			tx := live.Begin()
			if err := tx.Put("one", loc.Address()); err != nil {
				t.Fatal(err)
			}
			tx.Commit()
			v := live.View()
			defer v.Close()
			cp := s.NewDataCheckpoint(capture)
			moves := s.NewRelocations()
			defer moves.Close()
			fs.Hook = func(point string) error {
				if point == tc.point {
					return tc.err
				}
				return nil
			}
			err = cp.CopyByKey(t.Context(), v, 1, moves, s.ReserveSnapshotSegment)
			var source *SourceError
			var work *IndexWorkError
			if !errors.Is(err, tc.err) || errors.As(err, &source) != tc.source || errors.As(err, &work) != tc.indexWork || failedDataRead(err) != nil {
				t.Fatalf("permission failure classification: %v", err)
			}
			if err := cp.Discard(); err != nil {
				t.Fatal(err)
			}
			if s.Current != capture.Current {
				t.Fatal("failed staged copy replaced CURRENT")
			}
		})
	}
}

// Exercise the first-pass digest, contiguous range read, and streamed-record
// paths separately, so a yield during output cannot mask an unpaced input.
func TestOrderedCheckpointInputPacesEachPassAndSeparatesSchedulingFailures(t *testing.T) {
	const valueBytes = 3 * orderedPaceBytes
	target := int64(valueBytes + 1024)
	s, err := openTestStore(t, t.TempDir(), Config{MaxRecordBytes: target - 64, ValueCacheBudgetBytes: target, TargetSegmentBytes: target, Handles: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	value := bytes.Repeat([]byte("v"), valueBytes)
	loc, err := s.Append(format.Put, []byte("k"), value, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	cap, err := s.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer cap.Release()
	src := cap.Segments[0]
	record := format.EncodeRecord(format.Put, loc.Sequence, []byte("k"), value)
	h, err := format.DecodeRecordHeader(record[:format.RecordHeaderBytes], int64(len(record)))
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"scan", "window", "stream"} {
		t.Run(phase, func(t *testing.T) {
			for _, stop := range []error{nil, context.Canceled, syscall.EIO, &IndexWorkError{Err: syscall.EPERM}} {
				cp := s.NewDataCheckpoint(cap)
				yields := 0
				cp.SetOrderedYield(func() error {
					yields++
					if yields == 2 {
						return stop
					}
					return nil
				})
				done := false
				w := orderedCopy{c: cp, ctx: t.Context(), sources: map[uint64]CapturedSegment{src.Entry.ID: src}, buf: make([]byte, 32<<10), emit: func(string, Location) (io.Writer, error) {
					done = true
					return io.Discard, nil
				}}
				row := orderedWinner{payloadCRC: h.PayloadCRC, key: "k", loc: loc}
				var err error
				switch phase {
				case "scan":
					err = cp.scanByKey(t.Context(), src, func(int64, format.RecordHeader, string) error { done = true; return nil })
				case "window":
					w.window = []orderedWinner{row}
					w.payload = int(loc.RecordBytes)
					err = w.flush()
				case "stream":
					var got bytes.Buffer
					err = w.readOne(src, &row, &got)
					done = bytes.Equal(got.Bytes(), record)
				}
				if stop == nil {
					if err != nil || !done || yields != 3 {
						t.Fatalf("%s completion: done %t yields %d error %v", phase, done, yields, err)
					}
					continue
				}
				var source *SourceError
				var work *IndexWorkError
				if !errors.Is(err, stop) || done || yields != 2 || errors.As(err, &source) || failedDataRead(err) != nil {
					t.Fatalf("%s failure: done %t yields %d error %v", phase, done, yields, err)
				}
				if errors.Is(stop, context.Canceled) {
					if errors.As(err, &work) {
						t.Fatalf("cancellation became a private I/O error: %v", err)
					}
				} else if !errors.As(err, &work) || (errors.Is(stop, syscall.EPERM) && work != stop) {
					t.Fatalf("scheduling error was reclassified: %v", err)
				}
			}
		})
	}
}
