package tarp_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

type compactor interface {
	Compact(context.Context, ...tarp.CompactOption) (tarp.CompactResult, error)
}

var (
	_ compactor = (*tarp.DB)(nil)
	_ compactor = (*tarp.TypedDB[int16, string])(nil)
)

func openPublicDB(t *testing.T, dir string, opts tarp.Options) *tarp.DB {
	t.Helper()
	db, err := tarp.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestOpenExistingRequiresInitializedDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if db, err := tarp.OpenExisting(t.Context(), missing, tarp.Options{}); db != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent database: %v, %v", db, err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("absent directory after open: %v", err)
	}
	for _, tc := range []struct {
		name string
		file string
		want error
	}{
		{"empty", "", os.ErrNotExist},
		{"lock-only", "LOCK", os.ErrNotExist},
		{"partial", "unpublished-data", tarp.ErrCorrupt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.file != "" {
				if err := os.WriteFile(filepath.Join(dir, tc.file), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if db, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{}); db != nil || !errors.Is(err, tc.want) {
				t.Fatalf("open: %v, %v", db, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if tc.file == "" && len(entries) != 0 || tc.file != "" && (len(entries) != 1 || entries[0].Name() != tc.file) {
				t.Fatalf("directory after failed open: %v", entries)
			}
		})
	}
}

func TestOpenRejectsUninitializedNonemptyDirectoryWithoutCreatingLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "notes")
	if err := os.WriteFile(path, []byte("preserve me"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if db, err := tarp.Open(dir, tarp.Options{}); db != nil || !errors.Is(err, tarp.ErrCorrupt) {
			t.Fatalf("open: %v, %v", db, err)
		}
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) != 1 || entries[0].Name() != "notes" {
			t.Fatalf("directory after failed Open: %v, %v", entries, err)
		}
		if data, err := os.ReadFile(path); err != nil || string(data) != "preserve me" {
			t.Fatalf("notes after failed Open: %q, %v", data, err)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenExistingAcceptsRestoredDatabaseWithoutLockFile(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	req := tarp.RestoreRequest{Source: filepath.Join(parent, "backup"), Destination: filepath.Join(parent, "restored")}
	if err := db.Backup(t.Context(), req.Source); err != nil {
		t.Fatal(err)
	}
	if err := tarp.Restore(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(req.Destination, "LOCK")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restored LOCK before open: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	restored, err := tarp.OpenExisting(ctx, req.Destination, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := restored.Close(); err != nil {
			t.Error(err)
		}
	}()
	cancel() // A successful handle has its own lifetime.
	if other, err := tarp.OpenExisting(t.Context(), req.Destination, tarp.Options{}); other != nil || !errors.Is(err, tarp.ErrLocked) {
		t.Fatalf("second owner: %v, %v", other, err)
	}
	if got, err := restored.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("restored value: %q, %v", got, err)
	}
}

func TestBackupOperationsRejectSourceDirectoryAliases(t *testing.T) {
	for _, aliasKind := range []string{"symlink", "case"} {
		t.Run(aliasKind, func(t *testing.T) {
			root := t.TempDir()
			parent := filepath.Join(root, "Source")
			if err := os.Mkdir(parent, 0700); err != nil {
				t.Fatal(err)
			}
			alias := filepath.Join(root, "SOURCE")
			if aliasKind == "symlink" {
				alias = filepath.Join(root, "alias")
				if err := os.Symlink(parent, alias); err != nil {
					t.Fatal(err)
				}
			} else if _, err := os.Stat(alias); errors.Is(err, os.ErrNotExist) {
				t.Skip("filesystem is case-sensitive")
			} else if err != nil {
				t.Fatal(err)
			}

			ctx := t.Context()
			dir := filepath.Join(parent, "db")
			db := openPublicDB(t, dir, tarp.Options{})
			if err := db.Put([]byte("k"), []byte("v")); err != nil {
				t.Fatal(err)
			}
			if err := db.Backup(ctx, filepath.Join(alias, "db", "nested")); err == nil {
				t.Error("Backup accepted a destination inside its source")
			}
			backup := filepath.Join(parent, "backup")
			if err := db.Backup(ctx, backup); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			backupAlias := filepath.Join(alias, "backup")
			if _, err := tarp.VerifyBackupWithScratch(ctx, backup, backupAlias); !errors.Is(err, os.ErrInvalid) {
				t.Errorf("verification with scratch inside source: %v", err)
			}
			if err := tarp.Restore(ctx, tarp.RestoreRequest{Source: backup, Destination: filepath.Join(backupAlias, "nested")}); err == nil {
				t.Error("Restore accepted a destination inside its source")
			}
			if info, err := tarp.VerifyBackup(ctx, backup); err != nil || info.LiveKeys != 1 {
				t.Errorf("backup after rejected operations = %+v, %v", info, err)
			}
			reopened := openPublicDB(t, dir, tarp.Options{})
			if got, err := reopened.Get([]byte("k")); err != nil || string(got) != "v" {
				t.Fatalf("reopened value = %q, %v", got, err)
			}
		})
	}
}

func TestOpenRejectsBackupWithoutChangingItsFiles(t *testing.T) {
	source := openPublicDB(t, t.TempDir(), tarp.Options{})
	if err := source.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"Open", "OpenExisting"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			req := tarp.RestoreRequest{Source: filepath.Join(dir, "backup"), Destination: filepath.Join(dir, "restored")}
			if err := source.Backup(ctx, req.Source); err != nil {
				t.Fatal(err)
			}
			manifest := filepath.Join(req.Source, "BACKUP")
			before, err := os.ReadFile(manifest)
			if err != nil {
				t.Fatal(err)
			}
			var db *tarp.DB
			if mode == "Open" {
				db, err = tarp.Open(req.Source, tarp.Options{})
			} else {
				db, err = tarp.OpenExisting(ctx, req.Source, tarp.Options{})
			}
			if db != nil {
				db.Close()
				t.Fatal("opened a backup as a database")
			}
			if !errors.Is(err, tarp.ErrBackupDirectory) || errors.Is(err, tarp.ErrCorrupt) || !strings.Contains(err.Error(), "Restore") {
				t.Errorf("opening backup: %v", err)
			}
			after, err := os.ReadFile(manifest)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("backup manifest changed after open: %v", err)
			}
			// The manifest authenticates the exact inventory and bytes of all other files.
			if _, err := tarp.VerifyBackup(ctx, req.Source); err != nil {
				t.Fatalf("verify after open: %v", err)
			}
			if err := tarp.Restore(ctx, req); err != nil {
				t.Fatalf("restore after open: %v", err)
			}
			restored := openPublicDB(t, req.Destination, tarp.Options{})
			if got, err := restored.Get([]byte("k")); err != nil || string(got) != "v" {
				t.Fatalf("restored value: %q, %v", got, err)
			}
		})
	}
}

func TestOpenDetectsBackupMarkerBeforeVerifyingItsContents(t *testing.T) {
	source := openPublicDB(t, t.TempDir(), tarp.Options{})
	backup := filepath.Join(t.TempDir(), "backup")
	if err := source.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(backup, "BACKUP"), 0); err != nil {
		t.Fatal(err)
	}
	db, err := tarp.OpenExisting(t.Context(), backup, tarp.Options{})
	if db != nil {
		db.Close()
		t.Fatal("opened a backup as a database")
	}
	if !errors.Is(err, tarp.ErrBackupDirectory) || errors.Is(err, tarp.ErrCorrupt) {
		t.Fatalf("open with damaged BACKUP: %v", err)
	}
	if _, err := tarp.VerifyBackup(t.Context(), backup); !errors.Is(err, tarp.ErrCorrupt) || errors.Is(err, tarp.ErrBackupDirectory) {
		t.Fatalf("verify with damaged BACKUP: %v", err)
	}
}

func TestOpenPreservesIncompleteBackup(t *testing.T) {
	ctx := t.Context()
	source := openPublicDB(t, t.TempDir(), tarp.Options{})
	for _, tc := range []struct {
		damage string
		want   error
	}{
		{"missing CURRENT", tarp.ErrBackupDirectory},
		{"truncated CURRENT", tarp.ErrBackupDirectory},
		{"foreign CURRENT", tarp.ErrUnsupportedFormat},
	} {
		t.Run(tc.damage, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "backup")
			if err := source.Backup(ctx, dir); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "CURRENT")
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.damage {
			case "missing CURRENT":
				err = os.Remove(path)
			case "truncated CURRENT":
				err = os.Truncate(path, 8)
			case "foreign CURRENT":
				err = os.WriteFile(path, []byte("FOREIGN!"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, open := range []func(context.Context, string, tarp.Options) (*tarp.DB, error){tarp.OpenContext, tarp.OpenExisting} {
				db, err := open(ctx, dir, tarp.Options{})
				if db != nil {
					db.Close()
					t.Fatal("opened an incomplete backup")
				}
				if !errors.Is(err, tc.want) {
					t.Errorf("open returned %v, want %v", err, tc.want)
				}
			}
			if _, err := os.Lstat(filepath.Join(dir, "LOCK")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("LOCK after opening the backup: %v", err)
			}
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := tarp.VerifyBackup(ctx, dir); err != nil {
				t.Fatalf("backup after restoring original CURRENT: %v", err)
			}
		})
	}
}

func TestUnsupportedBackupSignaturePreventsVerificationAndRestore(t *testing.T) {
	ctx := t.Context()
	source := t.TempDir()
	marker := filepath.Join(source, "BACKUP")
	const previousSignature = "TARPBACK"
	if err := os.WriteFile(marker, []byte(previousSignature), 0600); err != nil {
		t.Fatal(err)
	}
	if info, err := tarp.VerifyBackup(ctx, source); !errors.Is(err, tarp.ErrUnsupportedFormat) || info != (tarp.BackupInfo{}) {
		t.Fatalf("verify returned %+v, %v", info, err)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if err := tarp.Restore(ctx, tarp.RestoreRequest{Source: source, Destination: dest}); !errors.Is(err, tarp.ErrUnsupportedFormat) {
		t.Fatalf("restore returned %v", err)
	}
	if _, err := os.Stat(dest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("destination after rejection: %v", err)
	}
	if got, err := os.ReadFile(marker); err != nil || string(got) != previousSignature {
		t.Fatalf("source after rejection: %q, %v", got, err)
	}
}

func TestVerifiedBackupChecksDestinationLimitsBeforeRestore(t *testing.T) {
	// A record larger than the default limit must still be inspectable.
	const valueBytes = 1 << 20
	db := openPublicDB(t, t.TempDir(), tarp.Options{MaxRecordBytes: 2 << 20})
	if err := db.Put([]byte("k"), make([]byte, valueBytes)); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	req := tarp.RestoreRequest{Source: filepath.Join(dir, "backup"), Destination: filepath.Join(dir, "restored")}
	if err := db.Backup(t.Context(), req.Source); err != nil {
		t.Fatal(err)
	}
	info, err := tarp.VerifyBackup(t.Context(), req.Source)
	wantLimits := tarp.RestoreLimits{ValueCacheBudgetBytes: 128 + 1 + valueBytes, MaxRecordBytes: 48 + 1 + valueBytes}
	if err != nil || info.LastUserSequence != 1 || info.LiveKeys != 1 || info.DatabaseID == ([16]byte{}) ||
		info.LargestLiveCacheChargeBytes != wantLimits.ValueCacheBudgetBytes || info.LargestLiveRecordBytes != wantLimits.MaxRecordBytes {
		t.Fatalf("verified backup = %+v, %v", info, err)
	}
	if _, err := os.Lstat(filepath.Join(req.Source, "LOCK")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("verified backup should have no lock file: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := tarp.VerifyBackup(ctx, req.Source); !errors.Is(err, context.Canceled) || got != (tarp.BackupInfo{}) {
		t.Fatalf("canceled verification: %+v, %v", got, err)
	}
	req.Limits.MaxRecordBytes = -1
	if err := info.CheckLimits(req.Limits); !errors.Is(err, tarp.ErrInvalidOptions) {
		t.Fatalf("check invalid restore limit: %v", err)
	}
	if err := tarp.Restore(t.Context(), req); !errors.Is(err, tarp.ErrInvalidOptions) {
		t.Fatalf("invalid restore limit: %v", err)
	}
	for _, limits := range []tarp.RestoreLimits{
		{}, // The default record limit is too small.
		{ValueCacheBudgetBytes: wantLimits.ValueCacheBudgetBytes - 1, MaxRecordBytes: wantLimits.MaxRecordBytes},
	} {
		req.Limits = limits
		for _, err := range []error{info.CheckLimits(limits), tarp.Restore(t.Context(), req)} {
			var incompatible *tarp.CompatibilityError
			if !errors.Is(err, tarp.ErrIncompatibleOptions) || !errors.As(err, &incompatible) ||
				incompatible.RequiredValueCacheBudgetBytes != wantLimits.ValueCacheBudgetBytes || incompatible.RequiredMaxRecordBytes != wantLimits.MaxRecordBytes {
				t.Fatalf("checking limits %+v: %v", limits, err)
			}
		}
		if _, err := os.Lstat(req.Destination); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("incompatible restore destination: %v", err)
		}
	}
	opts := tarp.Options{ValueCacheBudgetBytes: wantLimits.ValueCacheBudgetBytes, MaxRecordBytes: wantLimits.MaxRecordBytes, ReadFileHandles: -1, TargetSegmentBytes: -1, BatchGetReadConcurrency: -1}
	req.Limits = opts.RestoreLimits()
	if err := info.CheckLimits(req.Limits); err != nil {
		t.Fatalf("check exact live record limits: %v", err)
	}
	if err := tarp.Restore(t.Context(), req); err != nil {
		t.Fatalf("restore with exact live record limits: %v", err)
	}
	restored := openPublicDB(t, req.Destination, tarp.Options{MaxRecordBytes: wantLimits.MaxRecordBytes})
	if err := restored.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty-backup")
	if err := restored.Backup(t.Context(), empty); err != nil {
		t.Fatal(err)
	}
	got, err := tarp.VerifyBackup(t.Context(), empty)
	if err != nil || got != (tarp.BackupInfo{DatabaseID: info.DatabaseID, LastUserSequence: 2}) {
		t.Fatalf("verified empty backup of restored database = %+v, %v", got, err)
	}
}

func TestBatchGetReadConcurrencyOnReopen(t *testing.T) {
	dir := t.TempDir()
	seed := openPublicDB(t, dir, tarp.Options{})
	a, b := []byte("a"), []byte("b")
	if err := seed.BatchPut([]tarp.Entry{{Key: a, Value: a}, {Key: b, Value: b}}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err := tarp.Open(dir, tarp.Options{BatchGetReadConcurrency: -1}); db != nil || !errors.Is(err, tarp.ErrInvalidOptions) {
		if db != nil {
			db.Close()
		}
		t.Fatalf("Open with negative BatchGetReadConcurrency returned %p, %v", db, err)
	}
	for _, limit := range []int{0, 1, 3} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			db := openPublicDB(t, dir, tarp.Options{BatchGetReadConcurrency: limit})
			keys := [][]byte{b, a, b}
			results, err := db.BatchGet(keys)
			if err != nil || len(results) != len(keys) {
				t.Fatalf("BatchGet returned %+v, %v", results, err)
			}
			for i, r := range results {
				if !r.Found || string(r.Value) != string(keys[i]) {
					t.Fatalf("result %d = %+v, want %q", i, r, keys[i])
				}
			}
		})
	}
}

func TestPublicDBContextErrorsLeaveDatabaseUsable(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	if err := db.Put([]byte("k"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	backup := filepath.Join(parent, "source")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	expired, stop := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer stop()
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		cause error
	}{
		{"nil", nil, nil},
		{"canceled", canceled, context.Canceled},
		{"expired", expired, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := filepath.Join(parent, tc.name)
			check := func(operation string, err error) {
				t.Helper()
				var op *tarp.OpError
				if !errors.As(err, &op) || op.Operation != operation || op.Phase == "" || op.Outcome != tarp.NotApplied ||
					errors.Is(err, tarp.ErrCommitUncertain) || tc.cause != nil && !errors.Is(err, tc.cause) {
					t.Fatalf("%s returned %v", operation, err)
				}
			}
			opened, err := tarp.OpenContext(tc.ctx, target, tarp.Options{})
			check("open", err)
			if opened != nil {
				t.Fatal("OpenContext returned a handle")
			}
			result, err := db.Compact(tc.ctx)
			check("compact", err)
			if result != (tarp.CompactResult{}) {
				t.Fatalf("Compact returned %+v", result)
			}
			result, err = db.Compact(tc.ctx, tarp.WithKeyOrder())
			check("compact", err)
			if result != (tarp.CompactResult{}) {
				t.Fatalf("Compact with key order returned %+v", result)
			}
			check("backup", db.Backup(tc.ctx, target))
			info, err := tarp.VerifyBackup(tc.ctx, backup)
			check("verify-backup", err)
			if info != (tarp.BackupInfo{}) {
				t.Fatalf("VerifyBackup returned %+v on error", info)
			}
			check("restore", tarp.Restore(tc.ctx, tarp.RestoreRequest{Source: backup, Destination: target}))
			check("restore", tarp.Restore(tc.ctx, tarp.RestoreRequest{Source: filepath.Join(parent, "missing-source"), Destination: target}))
			check("restore", tarp.Restore(tc.ctx, tarp.RestoreRequest{Source: backup, Destination: parent}))
			if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected destination: %v", err)
			}
			count := 0
			for entry, err := range db.Scan(tc.ctx) {
				check("scan", err)
				count++
				if entry.Key != nil || entry.Value != nil {
					t.Fatalf("Scan yielded %+v with error", entry)
				}
			}
			if count != 1 {
				t.Fatalf("Scan yielded %d times; want one error", count)
			}
		})
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 1 || entries[0].Name() != "source" {
		t.Fatalf("unexpected restore output: %v, %v", entries, err)
	}
	if _, err := tarp.VerifyBackup(t.Context(), backup); err != nil {
		t.Fatalf("verify source after canceled restores: %v", err)
	}
	if value, err := db.Get([]byte("k")); string(value) != "value" || err != nil {
		t.Fatalf("Get after context errors = %q, %v", value, err)
	}
	if err := db.Put([]byte("k"), []byte("next")); err != nil {
		t.Fatalf("Put after context errors: %v", err)
	}
}

func TestPublicDBClosedTakesPrecedenceOverInvalidInput(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	seq := db.Scan(t.Context())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, getErr := db.Get(nil)
	_, compactErr := db.Compact(nil)
	_, orderedErr := db.Compact(nil, tarp.WithKeyOrder())
	for name, err := range map[string]error{
		"get": getErr, "put": db.Put(nil, nil), "delete": db.Delete(nil),
		"backup": db.Backup(nil, ""), "compact": compactErr, "compact with key order": orderedErr,
	} {
		if !errors.Is(err, tarp.ErrClosed) {
			t.Errorf("%s after Close = %v", name, err)
		}
	}
	yields := 0
	for entry, err := range seq {
		yields++
		if !errors.Is(err, tarp.ErrClosed) || entry.Key != nil || entry.Value != nil {
			t.Fatalf("Scan started after Close yielded %+v, %v", entry, err)
		}
	}
	if yields != 1 {
		t.Fatalf("Scan after Close yielded %d times; want one error", yields)
	}
}

func TestOpenIsExclusiveAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("key"), []byte("acknowledged")); err != nil {
		t.Fatal(err)
	}
	runChild := func(state string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOpenFromChildProcess$")
		cmd.Env = append(os.Environ(), "TARP_OWNERSHIP_DB="+alias, "TARP_OWNERSHIP_STATE="+state)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child Open with parent %s: %v\n%s", state, err, out)
		}
	}
	runChild("open")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	runChild("closed")
}

func TestOpenFromChildProcess(t *testing.T) {
	dir := os.Getenv("TARP_OWNERSHIP_DB")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	db, err := tarp.Open(dir, tarp.Options{})
	if db != nil {
		defer db.Close()
	}
	if os.Getenv("TARP_OWNERSHIP_STATE") == "open" {
		if db != nil || !errors.Is(err, tarp.ErrLocked) {
			t.Fatalf("Open while parent holds the database: db=%p err=%v", db, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("Open after parent closed the database: %v", err)
	}
	got, err := db.Get([]byte("key"))
	if err != nil || string(got) != "acknowledged" {
		t.Fatalf("Get after ownership transfer: %q, %v", got, err)
	}
}

func FuzzDBHistory(f *testing.F) {
	// Low three bits select the operation, the next two select one of four keys.
	const (
		put byte = iota
		putEmpty
		get
		removeKey
		compact
		reopen
		backup
		restore
	)
	const otherKey = 1 << 3
	f.Add([]byte{})
	f.Add([]byte{put, put | otherKey, backup, put, removeKey | otherKey, compact, reopen, restore, get, get | otherKey})
	f.Add([]byte{put | 0x80, backup, reopen, restore, compact, get})
	f.Add([]byte{get, removeKey, putEmpty, get, backup, removeKey, reopen, restore, put, get})
	f.Add([]byte{backup, put, restore, backup, putEmpty, restore, compact})

	f.Fuzz(func(t *testing.T, steps []byte) {
		// A small target rotates on the second Put. All values fit admission.
		opts := tarp.Options{
			IndexMode: tarp.IndexMemory, ValueCacheBudgetBytes: 144,
			MaxRecordBytes: 64, TargetSegmentBytes: 128, ReadFileHandles: 1,
		}
		if len(steps) > 0 && steps[0]&0x80 != 0 {
			opts.IndexMode = tarp.IndexBTree
		}
		dir := filepath.Join(t.TempDir(), "live")
		db := openPublicDB(t, dir, opts)
		live := make(map[string]string)
		var backupPath string
		var snapshot map[string]string
		checkScan := func(db *tarp.DB, want map[string]string) {
			t.Helper()
			got := make(map[string]string)
			for entry, err := range db.Scan(t.Context()) {
				if err != nil {
					t.Fatal(err)
				}
				key := string(entry.Key)
				if _, exists := got[key]; exists {
					t.Fatalf("Scan repeated key %x", key)
				}
				got[key] = string(entry.Value)
			}
			if !maps.Equal(got, want) {
				t.Fatalf("Scan = %q, want %q", got, want)
			}
		}
		checkScan(db, live)
		for i, step := range steps[:min(len(steps), 12)] {
			key := []byte{(step >> 3) & 3}
			want, present := live[string(key)]
			switch step & 7 {
			case put, putEmpty:
				var value []byte
				if step&7 == put {
					value = []byte{byte(i), step >> 5, 0xff}
				}
				if err := db.Put(key, value); err != nil {
					t.Fatalf("step %d Put(%x): %v", i, key, err)
				}
				live[string(key)] = string(value)
			case get:
				got, err := db.Get(key)
				if present && (err != nil || string(got) != want) || !present && !errors.Is(err, tarp.ErrNotFound) {
					t.Fatalf("step %d Get(%x) = %x, %v; expected value %x, present=%t", i, key, got, err, want, present)
				}
			case removeKey:
				err := db.Delete(key)
				if present && err != nil || !present && !errors.Is(err, tarp.ErrNotFound) {
					t.Fatalf("step %d Delete(%x): %v, present=%t", i, key, err, present)
				}
				delete(live, string(key))
			case compact:
				if _, err := db.Compact(t.Context()); err != nil {
					t.Fatalf("step %d Compact: %v", i, err)
				}
			case reopen:
				if err := db.Close(); err != nil {
					t.Fatalf("step %d Close: %v", i, err)
				}
				if opts.IndexMode == tarp.IndexBTree {
					opts.IndexMode = tarp.IndexMemory
				} else {
					opts.IndexMode = tarp.IndexBTree
				}
				db = openPublicDB(t, dir, opts)
			case backup:
				backupPath = filepath.Join(t.TempDir(), "backup")
				if err := db.Backup(t.Context(), backupPath); err != nil {
					t.Fatalf("step %d Backup: %v", i, err)
				}
				snapshot = maps.Clone(live)
			case restore:
				if backupPath != "" {
					dst := filepath.Join(t.TempDir(), "restored")
					if err := tarp.Restore(t.Context(), tarp.RestoreRequest{
						Source:      backupPath,
						Destination: dst,
						Limits:      opts.RestoreLimits(),
					}); err != nil {
						t.Fatalf("step %d Restore: %v", i, err)
					}
					image := openPublicDB(t, dst, opts)
					checkScan(image, snapshot)
					if err := image.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			checkScan(db, live)
		}
	})
}

func TestIndexModesShareDataSnapshotsAndBackups(t *testing.T) {
	for _, source := range []struct {
		name         string
		source, dest tarp.IndexMode
	}{
		{"memory to B+tree", tarp.IndexMemory, tarp.IndexBTree},
		{"B+tree to memory", tarp.IndexBTree, tarp.IndexMemory},
	} {
		t.Run(source.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "source")
			db := openPublicDB(t, dir, tarp.Options{IndexMode: source.source})
			key := append(bytes.Repeat([]byte{0, 0x80, 0xff}, 4000), 1)
			if err := db.BatchPut([]tarp.Entry{{Key: key, Value: []byte("first")}, {Key: []byte("empty")}, {Key: key, Value: []byte("last")}}); err != nil {
				t.Fatal(err)
			}
			check := func(db *tarp.DB) {
				t.Helper()
				got, err := db.BatchGet([][]byte{key, []byte("absent"), []byte("empty"), key})
				if err != nil || len(got) != 4 || string(got[0].Value) != "last" || got[1].Found || !got[2].Found || len(got[2].Value) != 0 || string(got[3].Value) != "last" {
					t.Fatalf("batch = %v, %v", got, err)
				}
			}
			check(db)
			backup := filepath.Join(t.TempDir(), "backup")
			if err := db.Backup(t.Context(), backup); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			other, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{IndexMode: source.dest})
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			check(other)
			info, err := tarp.VerifyBackup(t.Context(), backup)
			if err != nil {
				t.Fatal(err)
			}
			scratch := t.TempDir()
			withScratch, err := tarp.VerifyBackupWithScratch(t.Context(), backup, scratch)
			if err != nil || info != withScratch || info.LiveKeys != 2 {
				t.Fatalf("verify = %+v %+v, %v", info, withScratch, err)
			}
			files, err := os.ReadDir(scratch)
			if err != nil || len(files) != 0 {
				t.Fatalf("scratch: %v %v", files, err)
			}
			restored := filepath.Join(t.TempDir(), "restored")
			if err := tarp.Restore(t.Context(), tarp.RestoreRequest{Source: backup, Destination: restored}); err != nil {
				t.Fatal(err)
			}
			cloned, err := tarp.OpenExisting(t.Context(), restored, tarp.Options{IndexMode: source.dest})
			if err != nil {
				t.Fatal(err)
			}
			defer cloned.Close()
			check(cloned)
		})
	}
}

func TestBTreeWorkspaceFollowsDatabaseDirectory(t *testing.T) {
	parent := t.TempDir()
	t.Setenv("TMPDIR", filepath.Join(parent, "missing-temp"))
	dir := filepath.Join(parent, "live")
	opts := tarp.Options{IndexMode: tarp.IndexBTree, BTreeMemoryBudgetBytes: 1 << 20}
	db := openPublicDB(t, dir, opts)
	if info, err := os.Lstat(filepath.Join(dir, ".tarp-work")); err != nil || !info.IsDir() {
		t.Fatalf("database workspace: %v, %v", info, err)
	}

	// This key history exceeds the replay sort budget; checkpointing also spills relocations.
	suffix := strings.Repeat("k", 2048)
	entries := make([]tarp.Entry, 64)
	for i := range entries {
		entries[i] = tarp.Entry{Key: []byte(fmt.Sprintf("%02d%s", i, suffix)), Value: []byte{byte(i)}}
	}
	if err := db.BatchPut(entries); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	backup := filepath.Join(parent, "backup")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a process exit between CreateTemp and unlink on the live DB.
	workspace := filepath.Join(dir, ".tarp-work")
	if err := os.MkdirAll(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	leftover := filepath.Join(workspace, "tarp-index-123")
	if err := os.WriteFile(leftover, []byte("incomplete page"), 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := tarp.OpenExisting(t.Context(), backup, opts); !errors.Is(err, tarp.ErrBackupDirectory) || other != nil {
		t.Fatalf("OpenExisting on backup: %v, %v", other, err)
	}
	// VerifyBackup checks the exact inventory, including after the refused Open.
	if info, err := tarp.VerifyBackup(t.Context(), backup); err != nil || info.LiveKeys != uint64(len(entries)) {
		t.Fatalf("VerifyBackup: %+v, %v", info, err)
	}
	restored := filepath.Join(parent, "restored")
	if err := tarp.Restore(t.Context(), tarp.RestoreRequest{Source: backup, Destination: restored}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, restored} {
		image, err := tarp.OpenExisting(t.Context(), path, opts)
		if err != nil {
			t.Fatal(err)
		}
		defer image.Close()
		for _, entry := range entries {
			if got, err := image.Get(entry.Key); err != nil || !bytes.Equal(got, entry.Value) {
				t.Fatalf("%s: Get key %q: %x, %v", path, entry.Key[:2], got, err)
			}
		}
	}
	if _, err := os.Lstat(leftover); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch file after reopen: %v", err)
	}
}

func TestBTreeOpenRespectsCurrentOwnerBeforeCreatingWorkspace(t *testing.T) {
	dir := t.TempDir()
	openPublicDB(t, dir, tarp.Options{IndexMode: tarp.IndexMemory})
	t.Setenv("TMPDIR", filepath.Join(dir, "missing-temp"))
	other, err := tarp.Open(dir, tarp.Options{IndexMode: tarp.IndexBTree})
	if other != nil || !errors.Is(err, tarp.ErrLocked) {
		t.Fatalf("locked Open: %v, %v", other, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, ".tarp-work")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace after rejected Open: %v", err)
	}
}

func TestIndexOptionErrorsLeaveNewDirectoryAbsent(t *testing.T) {
	for _, o := range []tarp.Options{
		{IndexMode: 2}, // Outside the two supported modes.
		{IndexMode: tarp.IndexMemory, BTreeMemoryBudgetBytes: 1 << 20},
		{IndexMode: tarp.IndexBTree, BTreeMemoryBudgetBytes: 1},
		{IndexMode: tarp.IndexBTree, BTreeMemoryBudgetBytes: -1},
	} {
		dir := filepath.Join(t.TempDir(), "new")
		if db, err := tarp.Open(dir, o); db != nil || !errors.Is(err, tarp.ErrInvalidOptions) {
			t.Fatalf("Open(%+v) = %v, %v", o, db, err)
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
}

func ExampleOpen_indexMode() {
	dir, err := os.MkdirTemp("", "tarp-index-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		panic(err)
	}
	if err := db.Put([]byte("city"), []byte("Tokyo")); err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}

	db, err = tarp.OpenExisting(context.Background(), dir, tarp.Options{IndexMode: tarp.IndexMemory})
	if err != nil {
		panic(err)
	}
	defer db.Close()
	value, err := db.Get([]byte("city"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(value))
	// Output: Tokyo
}

func TestDefaultIndexCheckpointsOnCloseAndReusesExactCheckpoint(t *testing.T) {
	dir := t.TempDir()
	db := openPublicDB(t, dir, tarp.Options{})
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "INDEX-*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("index files %v, %v", files, err)
	}
	frontier := func() []byte {
		b, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	before := frontier()
	opts := tarp.Options{OpenValidation: tarp.Indexed}
	db = openPublicDB(t, dir, opts)
	if got, err := db.Get([]byte("key")); err != nil || string(got) != "value" {
		t.Fatalf("get %q, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, frontier()) {
		t.Fatal("an exact index was republished on Close")
	}
	if err := os.Truncate(files[0], 1); err != nil {
		t.Fatal(err)
	}
	db = openPublicDB(t, dir, opts)
	if got, err := db.Get([]byte("key")); err != nil || string(got) != "value" {
		t.Fatalf("fallback %q, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, frontier()) {
		t.Fatal("Close retained a corrupt checkpoint")
	}
}

func TestCheckpointIndexBackupRestoresWithoutIndex(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	if err := db.Put([]byte("prefix"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointIndex(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("tail"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if files, err := filepath.Glob(filepath.Join(backup, "INDEX-*")); err != nil || len(files) != 0 {
		t.Fatalf("backup index files %v, %v", files, err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := tarp.Restore(t.Context(), tarp.RestoreRequest{Source: backup, Destination: target}); err != nil {
		t.Fatal(err)
	}
	db = openPublicDB(t, target, tarp.Options{IndexMode: tarp.IndexBTree, OpenValidation: tarp.Indexed})
	for _, key := range []string{"prefix", "tail"} {
		if got, err := db.Get([]byte(key)); err != nil || string(got) != "v" {
			t.Fatalf("restore %s: %q, %v", key, got, err)
		}
	}
	if err := db.CheckpointIndex(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestIndexedOpenRepairsCoveredDataOnFirstValueRead(t *testing.T) {
	dir := t.TempDir()
	key := []byte("covered")
	value := bytes.Repeat([]byte("checked payload"), 550)
	db := openPublicDB(t, dir, tarp.Options{IndexMode: tarp.IndexBTree})
	if err := db.Put(key, value); err != nil {
		t.Fatal(err)
	}
	// A healthy B+tree Close checkpoints the covered DATA and completes parity.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("PARITY sidecars %v, %v", files, err)
	}
	path := filepath.Join(dir, strings.Replace(filepath.Base(files[0]), "PARITY-", "DATA-", 1))
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	damaged := bytes.Clone(original)
	damaged[len(damaged)-1] ^= 1
	if err := os.WriteFile(path, damaged, 0600); err != nil {
		t.Fatal(err)
	}
	frontier, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	db = openPublicDB(t, dir, tarp.Options{IndexMode: tarp.IndexBTree, OpenValidation: tarp.Indexed})
	if before, err := os.ReadFile(path); err != nil || !bytes.Equal(before, damaged) {
		t.Fatalf("indexed open changed covered DATA: %v", err)
	}
	if got, err := db.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("cold read after indexed reopen: %v, matched=%t", err, bytes.Equal(got, value))
	}
	if repaired, err := os.ReadFile(path); err != nil || !bytes.Equal(repaired, original) {
		t.Fatalf("DATA differs after automatic repair: %v", err)
	}
	if now, err := os.ReadFile(filepath.Join(dir, "CURRENT")); err != nil || !bytes.Equal(now, frontier) {
		t.Fatalf("physical repair changed the commit frontier: %v", err)
	}
	if stats := db.Stats(); stats.WriteCommits != 0 || stats.WrittenRecords != 0 {
		t.Fatalf("repair counted as a user write: %+v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openPublicDB(t, dir, tarp.Options{OpenValidation: tarp.Full})
	if got, err := reopened.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("full reopen after repair: %v", err)
	}
}

func TestIndexedReopenReplaysWritesMadeWithUnavailableSealedData(t *testing.T) {
	dir := t.TempDir()
	ctx := t.Context()
	seed := openPublicDB(t, dir, tarp.Options{})
	badKey := []byte("unreadable")
	want := bytes.Repeat([]byte("v"), 9000)
	if err := seed.BatchPut([]tarp.Entry{
		{Key: badKey, Value: want},
		{Key: []byte("remove"), Value: []byte("old")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.CheckpointIndex(ctx); err != nil {
		t.Fatal(err)
	}
	if err := seed.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	path, parity := sealedTestPaths(t, dir)
	wire, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(wire)
	db, err := tarp.OpenExisting(ctx, dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Keep this fixture unavailable across Close and Open; the later tail
	// must replay even when online compaction cannot prove irrecoverable loss.
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	flipSealedTestBytes(t, path, wire, 200, 4096+200)
	if err := os.Remove(parity); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get(badKey); !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatalf("damaged read: %v", err)
	}
	if err := db.Put([]byte("new"), []byte("committed during damage")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("remove")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, tarp.ErrDataUnavailable) || errors.Is(err, tarp.ErrUnhealthy) {
		t.Fatalf("close with selected damage: %v", err)
	}
	if strict, err := tarp.OpenExisting(ctx, dir, tarp.Options{}); strict != nil || !errors.Is(err, tarp.ErrCorrupt) {
		t.Fatalf("full recovery accepted damaged DATA: %v", err)
	}
	checkTail := func(db *tarp.DB) {
		t.Helper()
		if got, err := db.Get([]byte("new")); err != nil || string(got) != "committed during damage" {
			t.Fatalf("lost acknowledged Put: %q, %v", got, err)
		}
		if _, err := db.Get([]byte("remove")); !errors.Is(err, tarp.ErrNotFound) {
			t.Fatalf("lost acknowledged Delete: %v", err)
		}
	}
	indexed, err := tarp.OpenExisting(ctx, dir, tarp.Options{OpenValidation: tarp.Indexed})
	if err != nil {
		t.Fatal(err)
	}
	defer indexed.Close()
	if _, err := indexed.Get(badKey); !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatalf("covered key after indexed recovery: %v", err)
	}
	checkTail(indexed)
	replacement := []byte("replacement for unreadable value")
	if err := indexed.Put(badKey, replacement); err != nil {
		t.Fatalf("Put over unreadable value: %v", err)
	}
	if err := indexed.Close(); !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatal(err)
	}
	// Reopen with damage still present. The later Put shadows the damaged
	// record even with an empty cache and a checkpoint older than that Put.
	latest, err := tarp.OpenExisting(ctx, dir, tarp.Options{OpenValidation: tarp.Indexed})
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Close()
	if got, err := latest.Get(badKey); err != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement after indexed reopen: %q, %v", got, err)
	}
	if err := latest.Close(); err != nil && !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatal(err)
	}
	// Restoring the exact original bytes allows full replay as well. The old
	// value must stay superseded even after those bytes are readable again.
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	recovered := openPublicDB(t, dir, tarp.Options{})
	checkTail(recovered)
	if got, err := recovered.Get(badKey); err != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement after full recovery: %q, %v", got, err)
	}
}

func TestCompactPreservesUnavailableKeysThroughReopenAndReplacement(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := tarp.Options{}
	seed := openPublicDB(t, dir, opts)
	key, cursor := []byte("graph"), []byte("cursor")
	if err := seed.BatchPut([]tarp.Entry{
		{Key: key, Value: bytes.Repeat([]byte("v"), 10000)},
		{Key: cursor, Value: []byte("42")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := tarp.OpenExisting(ctx, dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	data, _ := sealedTestPaths(t, dir)
	wire, err := os.ReadFile(data)
	if err != nil {
		t.Fatal(err)
	}
	flipSealedTestBytes(t, data, wire, 300, 4096+300)
	for _, order := range [][]tarp.CompactOption{nil, {tarp.WithKeyOrder()}} {
		result, err := db.Compact(ctx, order...)
		var op *tarp.OpError
		var unavailable *tarp.UnavailableDataError
		if !errors.As(err, &op) || op.Outcome != tarp.NotApplied ||
			!errors.Is(err, tarp.ErrDataUnavailable) || !errors.As(err, &unavailable) || unavailable.FileID == 0 {
			t.Fatalf("compacting damaged DATA: %+v, %v", result, err)
		}
	}
	if err := db.Close(); !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatalf("close with selected damage: %v", err)
	}
	if full, err := tarp.OpenExisting(ctx, dir, opts); full != nil || !errors.Is(err, tarp.ErrCorrupt) {
		t.Fatalf("full reopen with selected damage: %v", err)
	}
	opts.OpenValidation = tarp.Indexed
	indexed := openPublicDB(t, dir, opts)
	if _, err := indexed.Get(key); !errors.Is(err, tarp.ErrDataUnavailable) || errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("indexed reopen lost evidence of the damaged key: %v", err)
	}
	if got, err := indexed.Get(cursor); err != nil || string(got) != "42" {
		t.Fatalf("cursor after refusing checkpoint: %q, %v", got, err)
	}
	if err := indexed.Put(key, []byte("restored")); err != nil {
		t.Fatal(err)
	}
	if result, err := indexed.Compact(ctx); err != nil {
		t.Fatalf("compacting after replacement: %+v, %v", result, err)
	}
	if err := indexed.Close(); err != nil {
		t.Fatal(err)
	}
	opts.OpenValidation = tarp.Full
	reopened := openPublicDB(t, dir, opts)
	if got, err := reopened.Get(key); err != nil || string(got) != "restored" {
		t.Fatalf("full reopen after recovery: %q, %v", got, err)
	}
}

func TestCompactRecoversEveryLiveKeyAfterExplicitDelete(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode tarp.IndexMode
	}{{"memory", tarp.IndexMemory}, {"btree", tarp.IndexBTree}} {
		t.Run(mode.name, func(t *testing.T) {
			ctx := t.Context()
			dir := t.TempDir()
			opts := tarp.Options{IndexMode: mode.mode}
			seed := openPublicDB(t, dir, opts)
			put := func(db *tarp.DB, key string, value []byte) {
				t.Helper()
				if err := db.Put([]byte(key), value); err != nil {
					t.Fatal(err)
				}
			}
			put(seed, "lost", []byte("previous version must stay hidden"))
			put(seed, "neighbor", []byte("intact in the first damaged block"))
			put(seed, "lost", []byte("lost-current-record"))
			put(seed, "hot", []byte("cached-current-record"))
			put(seed, "deleted", []byte("deleted-record"))
			put(seed, "updated", bytes.Repeat([]byte{'o'}, 5000))
			// This live record begins in the second bad block, crosses a stripe
			// boundary, and has one byte that parity can correct in the next stripe.
			crossing := bytes.Repeat([]byte{'v'}, 68<<10)
			put(seed, "crossing", crossing)
			put(seed, "updated", []byte("newer intact version"))
			if err := seed.Delete([]byte("deleted")); err != nil {
				t.Fatal(err)
			}
			if err := seed.ProtectParity(ctx); err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			data, _ := sealedTestPaths(t, dir)
			db := openPublicDB(t, dir, opts)
			if err := db.ProtectParity(ctx); err != nil {
				t.Fatal(err)
			}
			if got, err := db.Get([]byte("hot")); err != nil || string(got) != "cached-current-record" {
				t.Fatalf("prime cache: %q, %v", got, err)
			}
			wire, err := os.ReadFile(data)
			if err != nil {
				t.Fatal(err)
			}
			var offsets []int
			for _, value := range []string{"lost-current-record", "cached-current-record", "deleted-record"} {
				at := bytes.Index(wire, []byte(value))
				if at < 0 || at >= 4096 {
					t.Fatalf("fixture %q in wrong block at %d", value, at)
				}
				offsets = append(offsets, at)
			}
			if wire[4300] != 'o' || wire[66<<10] != 'v' {
				t.Fatal("large records at unexpected offsets")
			}
			flipSealedTestBytes(t, data, wire, append(offsets, 4300, 66<<10)...)
			if _, err := db.Compact(ctx); !errors.Is(err, tarp.ErrDataUnavailable) {
				t.Fatalf("checkpoint with an unreadable live record: %v", err)
			}
			if err := db.Delete([]byte("lost")); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Compact(ctx); err != nil {
				t.Fatalf("checkpoint after explicit delete: %v", err)
			}
			check := func(db *tarp.DB) {
				t.Helper()
				for key, value := range map[string][]byte{
					"neighbor": []byte("intact in the first damaged block"),
					"hot":      []byte("cached-current-record"),
					"updated":  []byte("newer intact version"),
					"crossing": crossing,
				} {
					if got, err := db.Get([]byte(key)); err != nil || !bytes.Equal(got, value) {
						t.Fatalf("Get(%s): got %d bytes, want %d, err=%v", key, len(got), len(value), err)
					}
				}
				for _, key := range []string{"lost", "deleted"} {
					if _, err := db.Get([]byte(key)); !errors.Is(err, tarp.ErrNotFound) {
						t.Fatalf("Get(%s) after checkpoint: %v", key, err)
					}
				}
				keys := make(map[string]bool)
				for key, err := range db.Keys(ctx) {
					if err != nil {
						t.Fatal(err)
					}
					keys[string(key)] = true
				}
				if !maps.Equal(keys, map[string]bool{"hot": true, "neighbor": true, "updated": true, "crossing": true}) {
					t.Fatalf("Keys after checkpoint: %v", keys)
				}
			}
			check(db)
			backup := filepath.Join(t.TempDir(), "backup")
			if err := db.Backup(ctx, backup); err != nil {
				t.Fatal(err)
			}
			if report, err := tarp.VerifyBackup(ctx, backup); err != nil || report.LiveKeys != 4 {
				t.Fatalf("backup: %+v, %v", report, err)
			}
			if err := db.CheckpointIndex(ctx); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			for _, validation := range []tarp.OpenValidation{tarp.Indexed, tarp.Full} {
				opts.OpenValidation = validation
				reopened := openPublicDB(t, dir, opts)
				check(reopened)
				if validation == tarp.Full {
					put(reopened, "lost", []byte("written after recovery"))
					if got, err := reopened.Get([]byte("lost")); err != nil || string(got) != "written after recovery" {
						t.Fatalf("Put after recovery: %q, %v", got, err)
					}
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestScanPrefixTakesFreshSnapshotsAndReleasesOnBreak(t *testing.T) {
	for name, mode := range map[string]tarp.IndexMode{"btree": tarp.IndexBTree, "memory": tarp.IndexMemory} {
		t.Run(name, func(t *testing.T) {
			// The PARITY worker can also pin the sealed log, and the cleaner
			// can reclaim it before the second explicit Compact.
			parked := make(chan struct{})
			core, err := engine.OpenWithHooks(t.TempDir(), engine.Options{IndexMode: mode}, nil, &engine.Hooks{
				CleanerStart: parked, ParityStart: parked,
			})
			if err != nil {
				t.Fatal(err)
			}
			db := (*tarp.DB)(core)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			prefix := []byte("a/")
			seq := db.ScanPrefix(t.Context(), prefix)
			clear(prefix)
			if err := db.BatchPut([]tarp.Entry{
				{Key: []byte("a/1"), Value: []byte("old")},
				{Key: []byte("a/2"), Value: []byte("old")},
				{Key: []byte("b/1"), Value: []byte("other")},
			}); err != nil {
				t.Fatal(err)
			}
			got := map[string]string{}
			for entry, err := range seq {
				if err != nil {
					t.Fatal(err)
				}
				if len(got) == 0 {
					if err := db.BatchPut([]tarp.Entry{
						{Key: []byte("a/1"), Value: []byte("new")},
						{Key: []byte("a/2"), Value: []byte("new")},
						{Key: []byte("a/3"), Value: []byte("new")},
					}); err != nil {
						t.Fatal(err)
					}
				}
				got[string(entry.Key)] = string(entry.Value)
				clear(entry.Key)
				clear(entry.Value)
			}
			if !maps.Equal(got, map[string]string{"a/1": "old", "a/2": "old"}) {
				t.Fatalf("first snapshot = %v", got)
			}
			clear(got)
			for entry, err := range seq {
				if err != nil {
					t.Fatal(err)
				}
				got[string(entry.Key)] = string(entry.Value)
			}
			if !maps.Equal(got, map[string]string{"a/1": "new", "a/2": "new", "a/3": "new"}) {
				t.Fatalf("fresh snapshot = %v", got)
			}
			var pinned int64
			for _, err := range seq {
				if err != nil {
					t.Fatal(err)
				}
				result, err := db.Compact(t.Context())
				if err != nil || result.PinnedRetiredDataBytes == 0 {
					t.Fatalf("compact during scan = %+v, %v", result, err)
				}
				pinned = result.PinnedRetiredDataBytes
				break
			}
			if pinned == 0 {
				t.Fatal("scan yielded no entries before early break")
			}
			if result, err := db.Compact(t.Context()); err != nil || result.PinnedRetiredDataBytes != 0 || result.UnlinkedBytes < pinned {
				t.Fatalf("compact after early break = %+v, %v; want to reclaim %d bytes of formerly pinned DATA", result, err, pinned)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			count := 0
			for entry, err := range db.ScanPrefix(ctx, []byte("missing")) {
				if !errors.Is(err, context.Canceled) || entry.Key != nil || entry.Value != nil {
					t.Fatalf("canceled scan = %+v, %v", entry, err)
				}
				count++
			}
			if count != 1 {
				t.Fatalf("canceled scan yielded %d times", count)
			}
		})
	}
}

func TestCompactOptionsSelectSnapshotOrder(t *testing.T) {
	source := []string{"z", "\x00", "a"}
	ordered := []string{"\x00", "a", "z"}
	for _, tc := range []struct {
		name string
		opts []tarp.CompactOption
		want []string
	}{
		{"default", nil, source},
		{"zero_option", []tarp.CompactOption{{}}, source},
		{"key_order", []tarp.CompactOption{tarp.WithKeyOrder()}, ordered},
		{"mixed_options", []tarp.CompactOption{{}, tarp.WithKeyOrder(), tarp.WithKeyOrder(), {}}, ordered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db := openPublicDB(t, dir, tarp.Options{})
			for _, key := range source {
				if err := db.Put([]byte(key), []byte("value")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Compact(t.Context(), tc.opts...); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if keys := snapshotDataKeys(t, dir); !slices.Equal(keys, tc.want) {
				t.Fatalf("snapshot DATA keys = %q; want %q", keys, tc.want)
			}
		})
	}
}

// Read the selected snapshot in catalog and file-offset order, not iterator order.
func snapshotDataKeys(t *testing.T, dir string) []string {
	t.Helper()
	image, err := storage.Validate(t.Context(), dir)
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, entry := range image.Catalog.Entries {
		if entry.Kind != format.SnapshotKind {
			break
		}
		f, err := os.Open(image.Segments[entry.ID].Path)
		if err != nil {
			t.Fatal(err)
		}
		err = format.Scan(t.Context(), f, entry.Bytes, func(_ int64, _ format.RecordHeader, key string) error {
			keys = append(keys, key)
			return nil
		})
		if err := errors.Join(err, f.Close()); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}

func TestCompactWithKeyOrderPreservesLatestValuesAcrossBackupAndReopen(t *testing.T) {
	dir := t.TempDir()
	db := openPublicDB(t, dir, tarp.Options{IndexMode: tarp.IndexBTree})
	// Share a long binary prefix so the B+tree uses overflow keys.
	prefix := bytes.Repeat([]byte{0xff, 0}, 4096)
	left := append(bytes.Clone(prefix), 1)
	right := append(bytes.Clone(prefix), 2)
	if err := db.BatchPut([]tarp.Entry{{Key: right, Value: []byte("right")}, {Key: left, Value: []byte("old")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(t.Context(), tarp.WithKeyOrder()); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(right); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(left, []byte("left")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(t.Context(), tarp.WithKeyOrder()); err != nil {
		t.Fatal(err)
	}
	if err := db.CheckpointIndex(t.Context()); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if info, err := tarp.VerifyBackup(t.Context(), backup); err != nil || info.LiveKeys != 1 {
		t.Fatalf("VerifyBackup = %+v, %v", info, err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if err := tarp.Restore(t.Context(), tarp.RestoreRequest{Source: backup, Destination: restored}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		dir  string
		opts tarp.Options
	}{
		{dir, tarp.Options{IndexMode: tarp.IndexBTree, OpenValidation: tarp.Indexed}},
		{restored, tarp.Options{IndexMode: tarp.IndexMemory, OpenValidation: tarp.Full}},
	} {
		image := openPublicDB(t, tc.dir, tc.opts)
		if _, err := image.Compact(t.Context(), tarp.WithKeyOrder()); err != nil {
			t.Fatal(err)
		}
		if got, err := image.Get(left); string(got) != "left" || err != nil {
			t.Fatalf("latest live value = %q, %v", got, err)
		}
		if _, err := image.Get(right); !errors.Is(err, tarp.ErrNotFound) {
			t.Fatalf("deleted key in reopened image: %v", err)
		}
	}
}
