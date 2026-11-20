package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
	"github.com/anaregdesign/tarp/internal/testfixture"
)

func TestBackupCanceledWaitingForWriterSkipsDestinationIO(t *testing.T) {
	var backupIO []string
	fs := &storage.IO{Hook: func(point string) error {
		if strings.HasPrefix(point, "backup.") {
			backupIO = append(backupIO, point)
		}
		return nil
	}}
	db := newTestDBWithHooks(t, testOptions(), fs, &Hooks{CleanerStart: make(chan struct{})})
	mustPut(t, db, "key", "value")
	parent := t.TempDir()
	dest := filepath.Join(parent, "backup")
	cancelMaintenanceWaitingForWriter(t, db, db.backup.Load, func(ctx context.Context) error {
		return db.Backup(ctx, dest)
	})
	if len(backupIO) != 0 {
		t.Fatalf("canceled Backup performed I/O: %v", backupIO)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled Backup left entries %v: %v", entries, err)
	}
	if err := db.Backup(t.Context(), dest); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyBackup(t.Context(), dest); err != nil {
		t.Fatal(err)
	}
}

func makeRestoreBackup(t *testing.T) string {
	t.Helper()
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "key", "acknowledged")
	source := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	return source
}

func writeFixtureFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func checkBackupUnchanged(t *testing.T, source string, before map[string]string) {
	t.Helper()
	entries, err := os.ReadDir(source)
	if err != nil || len(entries) != len(before) || !maps.Equal(directoryBytes(t, source), before) {
		t.Fatalf("Restore changed backup %s: entries=%v err=%v", source, entries, err)
	}
}

func TestRestoreRejectsInconsistentBackupImage(t *testing.T) {
	original := directoryBytes(t, makeRestoreBackup(t))
	for _, damage := range []string{"sequence", "cutoff", "active suffix", "segment identity", "inner signature",
		"missing selected file", "unlisted file", "listed unselected file", "duplicate entry", "repeated record sequence", "file digest"} {
		t.Run(damage, func(t *testing.T) {
			files := maps.Clone(original)
			meta, err := format.DecodeBackup([]byte(files["BACKUP"]))
			if err != nil {
				t.Fatal(err)
			}
			current, err := format.DecodeFrontier([]byte(files["CURRENT"]))
			if err != nil {
				t.Fatal(err)
			}
			active := storage.DataName(current.UUID, current.ActiveID)
			wantLocation := ""
			// Keep the outer hash valid so that Restore must validate the image.
			replaceFile := func(name string, data []byte) {
				t.Helper()
				for i := range meta.Files {
					if meta.Files[i].Name == name {
						files[name] = string(data)
						meta.Files[i].Bytes = int64(len(data))
						meta.Files[i].Hash = sha256.Sum256(data)
						return
					}
				}
				t.Fatalf("fixture does not select %q", name)
			}
			switch damage {
			case "sequence":
				meta.Sequence++
			case "cutoff":
				meta.Cutoff = meta.Sequence
			case "active suffix":
				replaceFile(active, append([]byte(files[active]), 0))
			case "segment identity":
				data := []byte(files[active])
				copy(data, format.EncodeSegment(format.Segment{UUID: current.UUID, ID: current.ActiveID + 1, Kind: format.Log}))
				replaceFile(active, data)
			case "inner signature":
				data := []byte(files["CURRENT"])
				copy(data[:8], "PREVIOUS")
				replaceFile("CURRENT", data)
			case "missing selected file":
				delete(files, active)
			case "unlisted file":
				files["extra"] = ""
			case "listed unselected file":
				name := storage.DataName(current.UUID, current.ManifestID+100)
				files[name] = ""
				meta.Files = append(meta.Files, format.BackupFile{Name: name, Hash: sha256.Sum256(nil)})
			case "duplicate entry":
				meta.Files = append(meta.Files, meta.Files[0])
			case "repeated record sequence":
				firstEnd := current.ActiveEnd
				data := append([]byte(files[active]), format.EncodeRecord(format.Put, current.Sequence, []byte("next"), nil)...)
				replaceFile(active, data)
				current.Sequence++
				meta.Sequence = current.Sequence
				current.ActiveEnd = int64(len(data))
				replaceFile("CURRENT", format.EncodeFrontier(current))
				wantLocation = fmt.Sprintf("%s: record at offset %d", active, firstEnd)
			case "file digest":
				data := []byte(files[active])
				data[len(data)-1] ^= 1
				files[active] = string(data)
				wantLocation = active
			}
			files["BACKUP"] = string(format.EncodeBackup(meta))
			source, parent := t.TempDir(), t.TempDir()
			for name, data := range files {
				writeFixtureFile(t, filepath.Join(source, name), []byte(data))
			}
			info, err := VerifyBackup(t.Context(), source)
			if !errors.Is(err, ErrCorrupt) || info != (storage.BackupInfo{}) || !strings.Contains(err.Error(), wantLocation) {
				t.Fatalf("VerifyBackup returned %+v, %v; want zero report and corruption at %q", info, err, wantLocation)
			}
			checkBackupUnchanged(t, source, files)
			err = Restore(t.Context(), RestoreRequest{
				Source:      source,
				Destination: filepath.Join(parent, "restored"),
				// Even when target limits are insufficient, verify the entire source first.
				Limits: RestoreLimits{CacheBytes: 129, MaxRecordBytes: 49},
			})
			var op *OpError
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrUnsupportedFormat) || !errors.As(err, &op) || op.Outcome != NotApplied ||
				!strings.Contains(err.Error(), wantLocation) {
				t.Fatalf("Restore returned %v, want unapplied corruption", err)
			}
			if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
				t.Fatalf("failed Restore left output: %v, %v", entries, err)
			}
			checkBackupUnchanged(t, source, files)
		})
	}
}

func TestRestoreValidatesStagedBytesBeforePublication(t *testing.T) {
	source := makeRestoreBackup(t)
	before := directoryBytes(t, source)
	for _, interrupted := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", interrupted), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var damagedFile string
			var synced int
			fs := &storage.IO{
				WriteAt: func(fd *os.File, b []byte, off int64) (int, error) {
					if damagedFile == "" && strings.HasPrefix(filepath.Base(fd.Name()), "DATA-") {
						damagedFile = filepath.Base(fd.Name())
						b = bytes.Clone(b)
						b[len(b)-1] ^= 1
					}
					return fd.WriteAt(b, off)
				},
				Hook: func(point string) error {
					if point == "restore.file.sync:after" {
						synced++
						// Every listed file has been copied, leaving staged validation next.
						if interrupted && synced == len(before)-1 {
							cancel()
						}
					}
					return nil
				},
			}
			parent := t.TempDir()
			req := RestoreRequest{
				Source:      source,
				Destination: filepath.Join(parent, "restored"),
				Limits:      testOptions().RestoreLimits(),
			}
			err := restoreWithIO(ctx, req, fs)
			var op *OpError
			if damagedFile == "" || !errors.As(err, &op) || op.Outcome != NotApplied ||
				errors.Is(err, ErrCorrupt) == interrupted || errors.Is(err, context.Canceled) != interrupted {
				t.Fatalf("damaged=%q canceled=%t, Restore returned %v", damagedFile, interrupted, err)
			}
			if !interrupted && !strings.Contains(err.Error(), fmt.Sprintf("%s: record at offset %d", damagedFile, format.SegmentHeaderBytes)) {
				t.Fatalf("staged corruption lost its location: %v", err)
			}
			if entries, err := os.ReadDir(parent); err != nil || len(entries) != 0 {
				t.Fatalf("failed staged validation left output: %v, %v", entries, err)
			}
			checkBackupUnchanged(t, source, before)
		})
	}
}

func TestRestoreReportsPublicationOutcome(t *testing.T) {
	source := makeRestoreBackup(t)
	before := directoryBytes(t, source)
	for _, tc := range []struct {
		point     string
		cause     error
		outcome   Outcome
		published bool
	}{
		{"restore.copy:after", syscall.EDQUOT, NotApplied, false},
		{"restore.file.sync:before", syscall.ENOSPC, NotApplied, false},
		{"restore.stage.sync:before", syscall.EIO, NotApplied, false},
		{"restore.stage.sync:after", context.Canceled, NotApplied, false},
		{"restore.rename:before", syscall.EIO, Unknown, false},
		{"restore.rename:after", syscall.EIO, Unknown, true},
		{"restore.parent.sync:before", syscall.EIO, Unknown, true},
		{"restore.parent.sync:after", syscall.EIO, Unknown, true},
	} {
		t.Run(tc.point, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := false
			fs := &storage.IO{Hook: func(point string) error {
				if point != tc.point || injected {
					return nil
				}
				injected = true
				if tc.cause == context.Canceled {
					cancel()
					return nil
				}
				return tc.cause
			}}
			parent := t.TempDir()
			dst := filepath.Join(parent, "restored")
			err := restoreWithIO(ctx, RestoreRequest{
				Source:      source,
				Destination: dst,
				Limits:      testOptions().RestoreLimits(),
			}, fs)
			var op *OpError
			if !injected || !errors.As(err, &op) || op.Operation != "restore" || op.Outcome != tc.outcome ||
				!errors.Is(err, tc.cause) || errors.Is(err, ErrCommitUncertain) != (tc.outcome == Unknown) ||
				errors.Is(err, ErrNoSpace) != (tc.cause == syscall.ENOSPC || tc.cause == syscall.EDQUOT) {
				t.Fatalf("Restore returned %v, want %s with %v", err, tc.outcome, tc.cause)
			}
			entries, err := os.ReadDir(parent)
			if err != nil {
				t.Fatal(err)
			}
			if tc.published {
				if len(entries) != 1 || entries[0].Name() != "restored" {
					t.Fatalf("expected only the published destination, got %v", entries)
				}
			} else {
				if len(entries) != 0 {
					t.Fatalf("unpublished Restore left output: %v", entries)
				}
				if err := Restore(t.Context(), RestoreRequest{
					Source:      source,
					Destination: dst,
					Limits:      testOptions().RestoreLimits(),
				}); err != nil {
					t.Fatalf("restore retry: %v", err)
				}
			}
			restored, err := Open(dst, testOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := restored.Close(); err != nil {
					t.Error(err)
				}
			})
			wantValue(t, restored, "key", "acknowledged")
			checkBackupUnchanged(t, source, before)
		})
	}
}

func TestBackupBatchesRecordsAcrossBlockBoundaries(t *testing.T) {
	var reads, writes int
	var copied int64
	fs := &storage.IO{Hook: func(point string) error {
		if point == "backup.read:after" {
			reads++
		}
		return nil
	},
		Wrote: func(point string, n int) {
			if point == "backup.copy" {
				writes++
				copied += int64(n)
			}
		}}
	engineHooks := &Hooks{CleanerStart: make(chan struct{})}
	db := newTestDBWithHooks(t, Options{}, fs, engineHooks)
	// This file crosses multiple buffer boundaries inside headers, keys and
	// values. Its physical tail may grow while Backup reads its captured prefix.
	keys := [][]byte{[]byte("first"), bytes.Repeat([]byte("k"), 33<<10), []byte("last")}
	values := [][]byte{bytes.Repeat([]byte("a"), (32<<10)-len(keys[0])-format.RecordHeaderBytes-8),
		bytes.Repeat([]byte("b"), 65<<10), []byte("last value")}
	for i, k := range keys {
		if err := db.Put(k, values[i]); err != nil {
			t.Fatal(err)
		}
	}
	end := db.store.Current.ActiveEnd
	seq := db.store.Current.Sequence
	// An uncommitted active-file suffix is outside the permitted read window.
	fd, err := os.OpenFile(db.index["last"].File.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fd.WriteAt(bytes.Repeat([]byte("tail"), 9000), end); err != nil {
		t.Fatal(err)
	}
	if err := fd.Close(); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if copied != end || reads == 0 || reads != writes-1 || reads >= 10 {
		t.Fatalf("captured end %d, copied %d with %d payload reads and %d writes", end, copied, reads, writes)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(t.Context(), RestoreRequest{Source: target, Destination: restored}); err != nil {
		t.Fatal(err)
	}
	clone, err := Open(restored, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer clone.Close()
	if clone.store.Current.Sequence != seq {
		t.Fatalf("restored sequence %d, want %d", clone.store.Current.Sequence, seq)
	}
	for i, k := range keys {
		v, err := clone.Get(k)
		if err != nil || !bytes.Equal(v, values[i]) {
			t.Fatalf("restored value %d mismatch: %v", i, err)
		}
	}
}

func TestBackupPayloadWriteFailureLeavesSourceHealthy(t *testing.T) {
	writes := 0
	db := newTestDB(t, Options{}, &storage.IO{Hook: func(point string) error {
		if point == "backup.copy:before" {
			writes++
			if writes == 2 { // The segment header has reached the staging file.
				return syscall.ENOSPC
			}
		}
		return nil
	}})
	mustPut(t, db, "key", "value")
	target := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), target); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("payload write returned %v", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup publication after failed payload write: %v", err)
	}
	mustPut(t, db, "key", "new")
}

func TestBackupRejectsCorruptionInFinalBlock(t *testing.T) {
	db := newTestDB(t, Options{}, nil)
	mustPut(t, db, "multi-block", strings.Repeat("v", 80<<10))
	file := db.index["multi-block"].File
	fd, err := os.OpenFile(file.Path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fd.WriteAt([]byte("!"), db.store.Current.ActiveEnd-1)
	if err := errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), target); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupted final block returned %v", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup publication after checksum failure: %v", err)
	}
	if _, err := db.Get([]byte("multi-block")); !errors.Is(err, ErrUnhealthy) {
		t.Fatalf("expected unhealthy source after checksum failure, got %v", err)
	}
}

func TestConcurrentPublishersNeverReplace(t *testing.T) {
	a := newTestDB(t, testOptions(), nil)
	b := newTestDB(t, testOptions(), nil)
	mustPut(t, a, "k", "a")
	mustPut(t, b, "k", "b")
	target := filepath.Join(t.TempDir(), "backup")
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, db := range []*DB{a, b} {
		wg.Go(func() {
			<-start
			results <- db.Backup(context.Background(), target)
		})
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else if !errors.Is(err, os.ErrExist) {
			t.Fatal(err)
		}
	}
	if success != 1 {
		t.Fatalf("publish successes: %d", success)
	}
	if err := Restore(context.Background(), RestoreRequest{
		Source:      target,
		Destination: filepath.Join(t.TempDir(), "restore"),
		Limits:      testOptions().RestoreLimits(),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMixedStoreCheckpointAndBackup(t *testing.T) {
	if _, err := Open(testfixture.WriteMixedStore(t, 7), Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("redundant Delete beyond pruning boundary: %v", err)
	}
	dir := testfixture.WriteMixedStore(t, 8)
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := Restore(context.Background(), RestoreRequest{Source: backup, Destination: target}); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(target, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { restored.Close() })
	for _, image := range []*DB{db, restored} {
		if _, err := image.Compact(context.Background()); err != nil {
			t.Fatal(err)
		}
		for key, value := range map[string]string{"checkpoint": "kept", "tail": "new", "empty": ""} {
			b, err := image.Get([]byte(key))
			if err != nil || !bytes.Equal(b, []byte(value)) {
				t.Fatalf("Get(%q) = %q, %v", key, b, err)
			}
		}
		for _, key := range []string{"pruned-snapshot-key", "pruned-put"} {
			if _, err := image.Get([]byte(key)); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Get(%q): %v", key, err)
			}
		}
		mustPut(t, image, "next", "write")
		image.writer.Lock()
		seq, cutoff, prune := image.store.Current.Sequence, image.store.Catalog.Cutoff, image.store.Catalog.PrunedThrough
		image.writer.Unlock()
		if seq != 10 || cutoff != 9 || prune != cutoff {
			t.Fatalf("checkpoint sequence/cutoff/pruning %d/%d/%d", seq, cutoff, prune)
		}
	}
}

func TestBackupPinsPointInTimeAcrossWritesAndCompaction(t *testing.T) {
	var armed atomic.Bool
	started, release := make(chan struct{}), make(chan struct{})
	fs := &storage.IO{Hook: func(p string) error {
		if p == "backup.copy:before" && armed.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return nil
	}}
	db := newTestDB(t, testOptions(), fs)
	mustPut(t, db, "a", "old")
	mustPut(t, db, "d", "keep-in-backup")
	target := filepath.Join(t.TempDir(), "backup")
	pinnedFile := db.index["a"].File
	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- db.Backup(context.Background(), target) }()
	<-started
	if err := db.Backup(context.Background(), filepath.Join(t.TempDir(), "other")); !errors.Is(err, ErrBusy) {
		t.Fatalf("second backup: %v", err)
	}
	mustPut(t, db, "a", "new")
	if err := db.Delete([]byte("d")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		mustPut(t, db, fmt.Sprint(i), "tail")
	}
	result, err := db.Compact(context.Background())
	if err != nil || result.PinnedBytes == 0 {
		t.Fatalf("pinned compaction: %+v %v", result, err)
	}
	wantValue(t, db, "a", "new")
	beforeRelease := db.MaintenanceStatus().UnlinkedBytes
	if _, err := os.Stat(pinnedFile.Path); err != nil {
		t.Fatalf("pinned source disappeared: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Backup release wakes the default worker even on an otherwise idle DB.
	waitForUnlink(t, db, pinnedFile.Path)
	restored := filepath.Join(t.TempDir(), "restored")
	if err := Restore(context.Background(), RestoreRequest{
		Source:      target,
		Destination: restored,
		Limits:      testOptions().RestoreLimits(),
	}); err != nil {
		t.Fatal(err)
	}
	image, err := Open(restored, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	wantValue(t, image, "a", "old")
	wantValue(t, image, "d", "keep-in-backup")
	if _, err := image.Get([]byte("0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("backup included later tail: %v", err)
	}
	mustPut(t, image, "restored-write", "ok")
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}
	result, err = db.Compact(context.Background())
	status := db.MaintenanceStatus()
	if err != nil || result.PinnedBytes != 0 || status.UnlinkedBytes-beforeRelease < uint64(pinnedFile.Size.Load()) {
		t.Fatalf("deferred cleanup: manual %+v automatic %+v error %v", result, status, err)
	}
}

func TestBackupDestinationFailuresDoNotPoisonSource(t *testing.T) {
	for _, point := range []string{"backup.copy:before", "backup.file.sync:before", "backup.rename:after", "backup.parent.sync:before"} {
		t.Run(point, func(t *testing.T) {
			var armed atomic.Bool
			db := newTestDB(t, testOptions(), &storage.IO{Hook: func(p string) error {
				if p == point && armed.CompareAndSwap(true, false) {
					return syscall.EIO
				}
				return nil
			}})
			mustPut(t, db, "a", "v")
			target := filepath.Join(t.TempDir(), "backup")
			armed.Store(true)
			err := db.Backup(context.Background(), target)
			if !errors.Is(err, syscall.EIO) {
				t.Fatal(err)
			}
			uncertain := point == "backup.rename:after" || point == "backup.parent.sync:before"
			if errors.Is(err, ErrCommitUncertain) != uncertain {
				t.Fatalf("publication outcome: %v", err)
			}
			wantValue(t, db, "a", "v")
			mustPut(t, db, "a", "still-healthy")
			if uncertain {
				if err := Restore(context.Background(), RestoreRequest{
					Source:      target,
					Destination: filepath.Join(t.TempDir(), "restored"),
					Limits:      testOptions().RestoreLimits(),
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRestoreValidationAndNoReplace(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "a", strings.Repeat("v", 50))
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(context.Background(), backup); !errors.Is(err, os.ErrExist) {
		t.Fatalf("backup replacement: %v", err)
	}
	if err := db.Backup(context.Background(), filepath.Join(db.store.Dir, "nested")); err == nil {
		t.Fatal("accepted nested backup")
	}
	destination := t.TempDir()
	before := directoryBytes(t, backup)
	if err := Restore(context.Background(), RestoreRequest{
		Source:      backup,
		Destination: destination,
		Limits:      testOptions().RestoreLimits(),
	}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("restored over empty existing directory: %v", err)
	}
	opts := testOptions()
	opts.CacheBytes = 140
	if err := Restore(context.Background(), RestoreRequest{
		Source:      backup,
		Destination: filepath.Join(t.TempDir(), "small"),
		Limits:      opts.RestoreLimits(),
	}); !errors.Is(err, ErrIncompatibleOptions) {
		t.Fatalf("restore smaller cache: %v", err)
	}
	if !reflect.DeepEqual(before, directoryBytes(t, backup)) {
		t.Fatal("restore changed backup")
	}
	for name := range before {
		if strings.HasPrefix(name, "DATA-") {
			path := filepath.Join(backup, name)
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			b[len(b)-1] ^= 0xff
			if err := os.WriteFile(path, b, 0600); err != nil {
				t.Fatal(err)
			}
			break
		}
	}
	if err := Restore(context.Background(), RestoreRequest{
		Source:      backup,
		Destination: filepath.Join(t.TempDir(), "corrupt"),
		Limits:      testOptions().RestoreLimits(),
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("restore accepted corruption: %v", err)
	}
}
