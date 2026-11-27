package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func TestIncrementalBackupRetryReusesFilesAfterInterruptedPublication(t *testing.T) {
	injected := errors.New("stop publication")
	var stopOnce sync.Once
	io := &storage.IO{Hook: func(point string) error {
		if point != "incremental-backup.manifest.write:before" {
			return nil
		}
		var err error
		stopOnce.Do(func() { err = injected })
		return err
	}}
	db := newTestDB(t, testOptions(), io)
	mustPut(t, db, "key", "value")
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	repository := t.TempDir()
	request := IncrementalBackupRequest{Repository: repository, Name: "retry"}
	result, err := db.BackupIncremental(t.Context(), request)
	var op *OpError
	if !errors.Is(err, injected) || !errors.As(err, &op) || op.Outcome != NotApplied || result != (IncrementalBackupResult{}) {
		t.Fatalf("interrupted backup returned %+v, %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(repository, "retry")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("interrupted backup destination lookup: %v", err)
	}
	result, err = db.BackupIncremental(t.Context(), request)
	if err != nil || result.LastUserSequence != 1 || result.ReusedDataBytes == 0 || result.CopiedDataBytes != format.SegmentHeaderBytes {
		t.Fatalf("retried backup returned %+v, %v", result, err)
	}
	if _, err := VerifyBackup(t.Context(), filepath.Join(repository, "retry")); err != nil {
		t.Fatal(err)
	}
}

func TestIncrementalBackupKeepsTheCommittedPrefixWhileWritesContinue(t *testing.T) {
	reached, resume := make(chan struct{}), make(chan struct{})
	var park sync.Once
	db := newTestDB(t, testOptions(), &storage.IO{Hook: func(point string) error {
		if point == "incremental-backup.metadata.write:before" {
			park.Do(func() {
				close(reached)
				select {
				case <-resume:
				case <-t.Context().Done():
				}
			})
		}
		return nil
	}})
	mustPut(t, db, "early", "value")
	repository := t.TempDir()
	backupErr := make(chan error, 1)
	go func() {
		_, err := db.BackupIncremental(t.Context(), IncrementalBackupRequest{Repository: repository, Name: "before-append"})
		backupErr <- err
	}()
	select {
	case <-reached:
	case err := <-backupErr:
		t.Fatalf("backup failed before capture: %v", err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	mustPut(t, db, "late", "value")
	close(resume)
	if err := <-backupErr; err != nil {
		t.Fatal(err)
	}
	info, err := VerifyBackup(t.Context(), filepath.Join(repository, "before-append"))
	if err != nil || info.LastUserSequence != 1 || info.LiveKeys != 1 {
		t.Fatalf("backup with concurrent append returned %+v, %v", info, err)
	}
}

func TestIncrementalBackupRehashRejectsDamagedRepositoryData(t *testing.T) {
	db := newTestDB(t, testOptions(), nil)
	mustPut(t, db, "key", "value")
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	repository := t.TempDir()
	req := IncrementalBackupRequest{Repository: repository, Name: "original"}
	if _, err := db.BackupIncremental(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(repository, req.Name)
	currentBytes, err := os.ReadFile(filepath.Join(backup, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	current, err := format.DecodeFrontier(currentBytes)
	if err != nil {
		t.Fatal(err)
	}
	activePath := filepath.Join(backup, storage.DataName(current.UUID, current.ActiveID))
	files, err := filepath.Glob(filepath.Join(backup, "DATA-*"))
	if err != nil || len(files) != 2 {
		t.Fatalf("capture has %d DATA files, %v", len(files), err)
	}
	for _, path := range files {
		if path == activePath {
			continue
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)-1] ^= 1
		if err := os.WriteFile(path, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	req.Name = "rehash"
	req.VerifyReusedData = true
	if _, err := db.BackupIncremental(t.Context(), req); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("rehashing damaged object returned %v", err)
	}
	if _, err := os.Stat(filepath.Join(repository, req.Name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("bad destination lookup returned %v", err)
	}
	if _, err := VerifyBackup(t.Context(), backup); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verifying damaged generation returned %v", err)
	}
	if value, err := db.Get([]byte("key")); err != nil || string(value) != "value" {
		t.Fatalf("healthy source Get returned %q, %v", value, err)
	}
}
