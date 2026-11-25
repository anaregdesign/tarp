package storage

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/index/memory"
)

func newBackupTestIndex() (index.Backend, error) { return memory.New(), nil }

func TestRestoreRejectsStagedBytesThatDifferFromBackup(t *testing.T) {
	cfg := Config{Handles: 1, ValueCacheBudgetBytes: 1024, MaxRecordBytes: 256, TargetSegmentBytes: 1024}
	store, err := openTestStore(t, t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Append(format.Put, []byte("key"), []byte("value"), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	capture, err := store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer capture.Release()
	backup := filepath.Join(t.TempDir(), "backup")
	if err := Backup(t.Context(), capture, cfg, nil, backup, nil, newBackupTestIndex); err != nil {
		t.Fatal(err)
	}
	active := DataName(capture.Current.UUID, capture.Current.ActiveID)
	for _, tc := range []struct {
		name, file string
		change     func([]byte) []byte
	}{
		{"different valid record", active, func(b []byte) []byte {
			copy(b[format.SegmentHeaderBytes:], format.EncodeRecord(format.Put, 1, []byte("key"), []byte("other")))
			return b
		}},
		{"extra active suffix", active, func(b []byte) []byte { return append(b, 42) }},
		{"different valid frontier", "CURRENT", func(_ []byte) []byte {
			frontier := capture.Current
			frontier.PublicationGeneration++
			return format.EncodeFrontier(frontier)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := false
			fs := &IO{WriteAt: func(fd *os.File, p []byte, off int64) (int, error) {
				if filepath.Base(fd.Name()) != tc.file {
					return fd.WriteAt(p, off)
				}
				changed = true
				n, err := fd.WriteAt(tc.change(bytes.Clone(p)), off)
				return min(n, len(p)), err
			}}
			target := filepath.Join(t.TempDir(), "restored")
			err := Restore(t.Context(), backup, target, cfg, fs)
			if !changed || !errors.Is(err, ErrCorrupt) {
				t.Fatalf("changed = %v, restore error = %v", changed, err)
			}
			if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("destination after failed restore: %v", err)
			}
			if err := Restore(t.Context(), backup, target, cfg, nil); err != nil {
				t.Fatalf("retry: %v", err)
			}
		})
	}
}

func TestDestinationPathDistinguishesDescendantsFromSiblings(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "image")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		parent string
		inside bool
	}{
		{"descendant", filepath.Join(source, "child", "grandchild"), true},
		{"sibling", filepath.Join(root, "image-work", "child"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.MkdirAll(tc.parent, 0700); err != nil {
				t.Fatal(err)
			}
			_, err := destinationPath(source, filepath.Join(tc.parent, "output"))
			if (err != nil) != tc.inside {
				t.Fatalf("inside = %v, destination error = %v", tc.inside, err)
			}
		})
	}
}

func TestRestoreVerifierCarriesOnlySourceSpillDecisionToStage(t *testing.T) {
	cfg := Config{Handles: 1, TargetSegmentBytes: 1 << 20}
	healthy := func() error { return nil }
	source := t.TempDir()
	store, err := openTestStore(t, source, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	puts := []Entry{{Key: []byte("a"), Value: []byte("old")}, {Key: []byte("b"), Value: []byte("longest value")}}
	if _, err := store.AppendBatch(format.Put, puts, healthy); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(format.Delete, puts[1].Key, nil, healthy); err != nil {
		t.Fatal(err)
	}
	want, err := Validate(t.Context(), source)
	if err != nil {
		t.Fatal(err)
	}
	var workErr *IndexWorkError
	if _, err := verifySorted(t.Context(), source, filepath.Join(source, "CURRENT"), verifyWindowBytes); !errors.As(err, &workErr) {
		t.Fatalf("small image accepted unusable scratch: %v", err)
	}

	stage := t.TempDir()
	staged, err := openTestStore(t, stage, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	// This validly encoded record deletes a key absent from the staged history.
	if _, err := staged.Append(format.Delete, []byte("absent"), nil, healthy); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		limit int64
		spill bool
	}{{"fits", verifyWindowBytes, false}, {"overflow", replayKeyCharge, true}} {
		t.Run(tc.name, func(t *testing.T) {
			scratch := t.TempDir()
			verify := restoreVerifier(scratch, tc.limit)
			image, err := verify(t.Context(), source)
			if err != nil {
				t.Fatal(err)
			}
			if image.LiveKeys != want.LiveKeys || image.LiveRecordSizes != want.LiveRecordSizes || image.LargestLiveValueBytes != want.LargestLiveValueBytes {
				t.Fatalf("source: live=%d, limits=%+v maxValue=%d", image.LiveKeys, image.LiveRecordSizes, image.LargestLiveValueBytes)
			}
			if _, err := verify(t.Context(), stage); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("invalid staged history: %v", err)
			}
			// A fresh workspace must open before scanning even a one-record stage
			// when the source overflowed. A small source never needs scratch.
			if err := os.Remove(scratch); err != nil {
				t.Fatal(err)
			}
			wantErr := ErrCorrupt
			if tc.spill {
				wantErr = os.ErrNotExist
			}
			if _, err := verify(t.Context(), stage); !errors.Is(err, wantErr) {
				t.Fatalf("stage after scratch removal: got %v, want %v", err, wantErr)
			}
		})
	}
}

func TestRestoreVerifiesSealedDigestEvenWhenBackupFileListMatches(t *testing.T) {
	cfg := Config{ValueCacheBudgetBytes: 1024, MaxRecordBytes: 128, TargetSegmentBytes: 256, Handles: 1}
	store, err := openTestStore(t, t.TempDir(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.Append(format.Put, []byte("key"), []byte("value"), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := store.Rotate(); err != nil {
		t.Fatal(err)
	}
	c, err := store.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	dir := filepath.Join(t.TempDir(), "backup")
	if err := Backup(t.Context(), c, cfg, store.IO, dir, nil, newBackupTestIndex); err != nil {
		t.Fatal(err)
	}
	listPath := filepath.Join(dir, "BACKUP")
	list, err := os.ReadFile(listPath)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := format.DecodeBackup(list)
	if err != nil {
		t.Fatal(err)
	}
	name := DataName(c.Current.UUID, c.Catalog.Entries[0].ID)
	path := filepath.Join(dir, name)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copy(b[format.SegmentHeaderBytes:], format.EncodeRecord(format.Put, 1, []byte("key"), []byte("other")))
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	for i, f := range meta.Files {
		if f.Name == name {
			meta.Files[i].Digest = sha256.Sum256(b)
		}
	}
	if err := os.WriteFile(listPath, format.EncodeBackup(meta), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBackup(t.Context(), dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verified backup: %v", err)
	}
	if _, err := VerifyBackupWithScratch(t.Context(), dir, t.TempDir()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verified backup with scratch: %v", err)
	}
	restore := filepath.Join(t.TempDir(), "restored")
	if err := Restore(t.Context(), dir, restore, cfg, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("restore: %v", err)
	}
	if _, err := os.Stat(restore); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published destination: %v", err)
	}
}
