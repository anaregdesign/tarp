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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/anaregdesign/tarp"
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
	if err != nil || info.Sequence != 1 || info.LiveKeys != 1 || info.DatabaseID == ([16]byte{}) ||
		info.RequiredLimits != wantLimits {
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
				incompatible.RequiredValueCacheBudgetBytes != wantLimits.ValueCacheBudgetBytes || incompatible.RequiredRecordBytes != wantLimits.MaxRecordBytes {
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
	if err != nil || got != (tarp.BackupInfo{DatabaseID: info.DatabaseID, Sequence: 2}) {
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
	for name, err := range map[string]error{
		"get": getErr, "put": db.Put(nil, nil), "delete": db.Delete(nil),
		"backup": db.Backup(nil, ""), "compact": compactErr,
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
		opts := tarp.Options{ValueCacheBudgetBytes: 144, MaxRecordBytes: 64, TargetSegmentBytes: 128, ReadFileHandles: 1}
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
		{"memory to disk", tarp.IndexMemory, tarp.IndexBTree}, {"disk to memory", tarp.IndexBTree, tarp.IndexMemory},
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
		{IndexMode: 99},
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

	db, err := tarp.Open(dir, tarp.Options{
		IndexMode:              tarp.IndexBTree,
		BTreeMemoryBudgetBytes: 16 << 20,
		ValueCacheBudgetBytes:  64 << 20,
	})
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
