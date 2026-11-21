package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/publication"
)

type repairFixture struct {
	dir       string
	paths     []string
	originals [][]byte
}

func newRepairFixture(t *testing.T, lengths ...int) repairFixture {
	t.Helper()
	dir := t.TempDir()
	db, _, err := Open(dir, Config{Handles: 1, RecordBytes: 1 << 20, ValueCacheBudgetBytes: 1 << 20, TargetSegmentBytes: 1 << 20}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	fixture := repairFixture{dir: dir}
	for i, size := range lengths {
		if _, err := db.Append(format.Put, []byte(fmt.Sprint(i)), bytes.Repeat([]byte{byte(i + 1)}, size), func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if err := db.Rotate(); err != nil {
			t.Fatal(err)
		}
		entry := db.Catalog.Files[i]
		path := db.Files[entry.ID].Path
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		sidecar, err := os.Create(filepath.Join(dir, parityName(db.Current.UUID, entry.ID)))
		if err != nil {
			t.Fatal(err)
		}
		err = format.WriteParity(t.Context(), bytes.NewReader(data), sidecar, db.Current.UUID, entry)
		if err := errors.Join(err, sidecar.Close()); err != nil {
			t.Fatal(err)
		}
		fixture.paths = append(fixture.paths, path)
		fixture.originals = append(fixture.originals, data)
	}
	return fixture
}

func TestRepairParityRecoversIndependentFilesAndPreservesFailures(t *testing.T) {
	f := newRepairFixture(t, 10000, 6000, 30)
	bad := bytes.Clone(f.originals[0])
	bad[format.SegmentHeaderBytes+120] ^= 0x90
	bad[4500] ^= 0x90
	if err := os.WriteFile(f.paths[0], bad, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(f.paths[1], 4900); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(f.paths[2]); err != nil {
		t.Fatal(err)
	}

	err := RepairParity(t.Context(), f.dir, nil)
	var failure *Fault
	if !errors.Is(err, ErrCorrupt) || !errors.As(err, &failure) || failure.Outcome != publication.Committed {
		t.Fatalf("partial recovery: %v", err)
	}
	for i, want := range [][]byte{bad, f.originals[1], f.originals[2]} {
		got, err := os.ReadFile(f.paths[i])
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("file %d: did not preserve/repair expected contents: %v", i, err)
		}
	}
	if err := os.WriteFile(f.paths[0], f.originals[0], 0600); err != nil {
		t.Fatal(err)
	}
	if err := RepairParity(t.Context(), f.dir, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRepairParityPublicationFaultsAndDurableRetry(t *testing.T) {
	for _, phase := range []string{"repair.write:after", "repair.sync:after", "repair.rename:before", "repair.rename:after", "repair.dirsync:before"} {
		t.Run(phase, func(t *testing.T) {
			f := newRepairFixture(t, 5000)
			bad := bytes.Clone(f.originals[0])
			bad[len(bad)-1] ^= 1
			if err := os.WriteFile(f.paths[0], bad, 0600); err != nil {
				t.Fatal(err)
			}
			fs := &IO{Hook: func(point string) error {
				if point == phase {
					return syscall.EIO
				}
				return nil
			}}
			err := RepairParity(t.Context(), f.dir, fs)
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("missing original error: %v", err)
			}
			got, readErr := os.ReadFile(f.paths[0])
			var failure *Fault
			renamed := phase == "repair.rename:after" || phase == "repair.dirsync:before"
			if readErr != nil || renamed != errors.As(err, &failure) || renamed && (failure.Outcome != publication.Unknown || !bytes.Equal(got, f.originals[0])) || !renamed && !bytes.Equal(got, bad) {
				t.Fatalf("wrong state after %s: %v (read %v)", phase, err, readErr)
			}
			if files, err := filepath.Glob(filepath.Join(f.dir, ".tarp-current-*.tmp")); err != nil || len(files) != 0 {
				t.Fatalf("staging remnants: %v %v", files, err)
			}
			if renamed {
				blockSync := &IO{Hook: func(point string) error {
					if point == "lock.dirsync:before" {
						return syscall.EIO
					}
					return nil
				}}
				if err := RepairParity(t.Context(), f.dir, blockSync); !errors.Is(err, syscall.EIO) {
					t.Fatalf("retry skipped directory durability: %v", err)
				}
			}
			if err := RepairParity(t.Context(), f.dir, nil); err != nil {
				t.Fatal(err)
			}
			if got, err := os.ReadFile(f.paths[0]); err != nil || !bytes.Equal(got, f.originals[0]) {
				t.Fatalf("retry: %v", err)
			}
		})
	}
}

func TestRepairParityVerifiesActualStagedBytesBeforeRename(t *testing.T) {
	f := newRepairFixture(t, 5000)
	bad := bytes.Clone(f.originals[0])
	bad[110] ^= 1
	if err := os.WriteFile(f.paths[0], bad, 0600); err != nil {
		t.Fatal(err)
	}
	fs := &IO{WriteAt: func(fd *os.File, b []byte, off int64) (int, error) {
		changed := bytes.Clone(b)
		changed[0] ^= 4
		return fd.WriteAt(changed, off)
	}}
	if err := RepairParity(t.Context(), f.dir, fs); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("accepted unverified staged bytes: %v", err)
	}
	if got, err := os.ReadFile(f.paths[0]); err != nil || !bytes.Equal(got, bad) {
		t.Fatalf("damaged source replaced: %v", err)
	}
}

func TestRepairParityRequiresTrustedSelectionAndSafePaths(t *testing.T) {
	for _, object := range []string{"CURRENT", "MANIFEST", "DATA symlink", "PARITY symlink", "PARITY contents", "BACKUP"} {
		t.Run(object, func(t *testing.T) {
			f := newRepairFixture(t, 4999)
			img, err := readImage(t.Context(), f.dir)
			if err != nil {
				t.Fatal(err)
			}
			bad := bytes.Clone(f.originals[0])
			bad[len(bad)-1] ^= 1
			if err := os.WriteFile(f.paths[0], bad, 0600); err != nil {
				t.Fatal(err)
			}
			want := ErrCorrupt
			path := ""
			switch object {
			case "CURRENT", "MANIFEST":
				name := object
				if name == "MANIFEST" {
					name = ManifestName(img.Current.UUID, img.Current.ManifestID)
				}
				name = filepath.Join(f.dir, name)
				raw, err := os.ReadFile(name)
				if err != nil {
					t.Fatal(err)
				}
				raw[len(raw)-1] ^= 1
				if err := os.WriteFile(name, raw, 0600); err != nil {
					t.Fatal(err)
				}
			case "DATA symlink":
				path = f.paths[0]
			case "PARITY symlink":
				path = filepath.Join(f.dir, parityName(img.Current.UUID, img.Catalog.Files[0].ID))
			case "PARITY contents":
				path := filepath.Join(f.dir, parityName(img.Current.UUID, img.Catalog.Files[0].ID))
				if err := os.Truncate(path, 15); err != nil {
					t.Fatal(err)
				}
				want = format.ErrInvalidParity
			case "BACKUP":
				if err := os.WriteFile(filepath.Join(f.dir, "BACKUP"), []byte("preserve"), 0600); err != nil {
					t.Fatal(err)
				}
				want = ErrBackupDirectory
			}
			if path != "" {
				other := filepath.Join(t.TempDir(), "keep")
				if err := os.Rename(path, other); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(other, path); err != nil {
					t.Fatal(err)
				}
			}
			if err := RepairParity(t.Context(), f.dir, nil); !errors.Is(err, want) {
				t.Fatalf("unsafe repair: %v, want %v", err, want)
			}
			if got, err := os.ReadFile(f.paths[0]); err != nil || !bytes.Equal(got, bad) {
				t.Fatalf("source changed: %v", err)
			}
			if path != "" {
				if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("symlink replaced: %v", err)
				}
			}
		})
	}
}

func TestRepairParityCancellationPreservesSourceAndAllowsRetry(t *testing.T) {
	f := newRepairFixture(t, 5000)
	bad := bytes.Clone(f.originals[0])
	bad[len(bad)-1] ^= 1
	if err := os.WriteFile(f.paths[0], bad, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fs := &IO{Hook: func(point string) error {
		if point == "repair.sync:after" {
			cancel()
		}
		return nil
	}}
	if err := RepairParity(ctx, f.dir, fs); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled repair: %v", err)
	}
	if got, err := os.ReadFile(f.paths[0]); err != nil || !bytes.Equal(got, bad) {
		t.Fatalf("changed original: %v", err)
	}
	if err := RepairParity(t.Context(), f.dir, nil); err != nil {
		t.Fatal(err)
	}
}
