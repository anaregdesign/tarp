package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

// One long Put has a recoverable first stripe, an uncorrectable second
// stripe and independently checked original bytes at the end.
func protectedRecoveryFixture(t *testing.T, fs *IO) (*Store, *Checkpoint, CapturedFile, []byte) {
	t.Helper()
	cfg := Config{ValueCacheBudgetBytes: 1 << 20, RecordBytes: 1 << 20, TargetSegmentBytes: 2 << 20, Handles: 1}
	s, _, err := Open(t.TempDir(), cfg, fs)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	_, err = s.Append(format.Put, []byte("live"), bytes.Repeat([]byte{'v'}, 160<<10), func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rotate(); err != nil {
		t.Fatal(err)
	}
	c, err := s.Capture()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Release)
	src := c.Files[0]
	if err := s.EnsureParity(t.Context(), c.Current.UUID, src, true, nil); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(src.File.Path)
	if err != nil {
		t.Fatal(err)
	}
	return s, s.NewCheckpoint(c), src, raw
}

func TestRecoveredBlocksUseRawBytesOnlyFromIdentifiedOriginal(t *testing.T) {
	_, checkpoint, src, raw := protectedRecoveryFixture(t, nil)
	uuid := checkpoint.capture.Current.UUID
	uuid[0] ^= 1
	copy(raw, format.EncodeSegment(format.Segment{UUID: uuid, ID: src.File.ID, Kind: src.Entry.Kind}))
	stripe := format.ParityBlockBytes * format.ParityDataBlocks
	raw[stripe+8] ^= 1
	raw[stripe+format.ParityBlockBytes+8] ^= 1
	if err := os.WriteFile(src.File.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	stage, err := checkpoint.stageBlocks(t.Context(), src)
	if stage != nil {
		defer stage.Close()
	}
	if err != nil {
		t.Fatal(err)
	}
	var h [format.SegmentHeaderBytes]byte
	if _, err := stage.ReadAt(h[:], 0); err != nil {
		t.Fatal(err)
	}
	if err := verifySegmentHeader(h[:], checkpoint.capture.Current.UUID, src.Entry); err != nil {
		t.Fatal(err)
	}
	var got [16]byte
	if _, err := stage.ReadAt(got[:], int64(stripe)); err != nil || got != ([16]byte{}) {
		t.Fatalf("foreign raw bytes accepted: %x, %v", got, err)
	}
	if _, err := stage.ReadAt(got[:], int64(2*stripe)); err != nil || !bytes.Equal(got[:], bytes.Repeat([]byte{'v'}, len(got))) {
		t.Fatalf("lost certified bytes: %x, %v", got, err)
	}
}

func TestRecoveryStageSeparatesSourceOutageFromPrivateWriteDamage(t *testing.T) {
	fs := &IO{}
	_, c, src, _ := protectedRecoveryFixture(t, fs)
	for _, fault := range []string{"compact.recover.source:before", "compact.recover.parity:before"} {
		fs.Hook = func(point string) error {
			if point == fault {
				return syscall.EIO
			}
			return nil
		}
		stage, err := c.stageBlocks(t.Context(), src)
		if stage != nil {
			stage.Close()
		}
		if !IsDataUnavailable(err) || !errors.Is(err, syscall.EIO) {
			t.Fatalf("%s: %v", fault, err)
		}
	}
	fs.Hook = nil
	fs.WriteAt = func(f *os.File, b []byte, off int64) (int, error) {
		if strings.HasPrefix(filepath.Base(f.Name()), "tarp-checkpoint-recover-") && off == 0 {
			b = bytes.Clone(b)
			b[0] ^= 1
		}
		return f.WriteAt(b, off)
	}
	stage, err := c.stageBlocks(t.Context(), src)
	if stage != nil {
		defer stage.Close()
	}
	if err == nil || IsDataUnavailable(err) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("private write damage classified as source damage: %v", err)
	}
}
