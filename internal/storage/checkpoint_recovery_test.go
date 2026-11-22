package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func TestRecoveryRejectsIndexAddressInsideDamagedRecord(t *testing.T) {
	s, c, src, raw := protectedRecoveryFixture(t, nil)
	raw[256] ^= 1
	raw[format.DataBlockBytes+256] ^= 1
	if err := os.WriteFile(src.Segment.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	keys := memory.New()
	defer keys.Close()
	// A forged entry has plausible DATA ID, sequence, segment bounds and
	// record geometry and lies in a proven bad block. It does not start at
	// an original record boundary. Simulate matching aggregate counters so
	// they alone cannot establish membership.
	addr := index.Address{FileID: src.Segment.ID, Sequence: 1, RecordOffset: 200, RecordBytes: format.RecordHeaderBytes + 4 + 24, ValueBytes: 24}
	tx := keys.Begin()
	if err := tx.Put("fake", addr); err != nil {
		t.Fatal(err)
	}
	tx.Commit()
	c.capture.Segments[0].liveKeys, c.capture.Segments[0].liveBytes = 1, addr.RecordBytes
	v := keys.View()
	defer v.Close()
	h := memory.New()
	defer h.Close()
	moves, lost := s.NewRelocations(), s.NewRelocations()
	defer moves.Close()
	defer lost.Close()
	before := s.Current
	err := c.CopyRecovered(t.Context(), h, v, moves, lost, s.ReserveSnapshotSegment, nil)
	if !errors.Is(err, ErrCorrupt) || lost.count != 0 || s.Current != before {
		t.Fatalf("forged index address accepted for loss: %v; lost %d", err, lost.count)
	}
}

func TestRecoveryPreservesOtherLiveKeyWhenDamagedKeyBytesCollide(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 1 << 20, MaxRecordBytes: 1 << 20, TargetSegmentBytes: 2 << 20, Handles: 1}
	s, err := openTestStore(t, t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h, keys := memory.New(), memory.New()
	defer h.Close()
	defer keys.Close()
	tx := keys.Begin()
	var newest Location
	for _, k := range []string{"a", "b"} {
		v := []byte("keep")
		if k == "b" {
			v = bytes.Repeat([]byte{'v'}, 2*format.DataBlockBytes)
		}
		loc, err := s.Append(format.Put, []byte(k), v, func() error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if err := tx.Put(k, loc.Address()); err != nil {
			t.Fatal(err)
		}
		loc.Segment.LiveKeys.Add(1)
		loc.Segment.LiveBytes.Add(loc.RecordBytes)
		newest = loc
	}
	tx.Commit()
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	c, err := s.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	src := c.Segments[0]
	if err := s.EnsureParitySidecar(t.Context(), c.Current.UUID, src, true, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(src.Segment.Path)
	if err != nil {
		t.Fatal(err)
	}
	raw[newest.RecordOffset+format.RecordHeaderBytes] = 'a'
	raw[format.DataBlockBytes+20] ^= 1
	if err := os.WriteFile(src.Segment.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	v := keys.View()
	defer v.Close()
	moves, lost := s.NewRelocations(), s.NewRelocations()
	defer moves.Close()
	defer lost.Close()
	out := s.NewDataCheckpoint(c)
	defer out.Discard()
	if err := out.CopyRecovered(t.Context(), h, v, moves, lost, s.ReserveSnapshotSegment, nil); err != nil {
		t.Fatal(err)
	}
	for _, want := range []struct {
		spool *Relocations
		key   string
	}{{moves, "a"}, {lost, "b"}} {
		if want.spool.count != 1 {
			t.Fatalf("%s count %d", want.key, want.spool.count)
		}
		if err := want.spool.Each(func(key string, _, _ Location) error {
			if key != want.key {
				t.Errorf("got %q, want %q", key, want.key)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRecoverySpoolBindsOriginalPayloadChecksumAcrossFragmentedWrites(t *testing.T) {
	input := format.EncodeRecord(format.Put, 17, []byte("k"), []byte("value"))
	h, err := format.DecodeRecordHeader(input[:format.RecordHeaderBytes], int64(len(input)))
	if err != nil {
		t.Fatal(err)
	}
	var staged bytes.Buffer
	runs := sortedReplay{ctx: t.Context(), dir: t.TempDir(), limit: 1024}
	defer runs.close()
	row := recoveryScratchWriter{writer: &staged, runs: &runs, event: replayEvent{key: "k", addr: index.Address{RecordBytes: int64(len(input)), ValueBytes: h.ValueBytes, Sequence: h.Sequence}}, left: int64(len(input))}
	for i := range input {
		if _, err := row.Write(input[i : i+1]); err != nil {
			t.Fatal(err)
		}
		if i != len(input)-1 && len(runs.chunk) != 0 {
			t.Fatal("published a partial recovery record")
		}
	}
	if !bytes.Equal(staged.Bytes(), input) || len(runs.chunk) != 1 || runs.chunk[0].payloadCRC != h.PayloadCRC {
		t.Fatalf("staged input lost its checked checksum: %+v", runs.chunk)
	}
	var work *IndexWorkError
	if _, err := row.Write([]byte("x")); !errors.As(err, &work) {
		t.Fatalf("accepted staging beyond original record: %v", err)
	}
}
