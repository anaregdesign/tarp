package tarp_test

import (
	"bytes"
	"context"
	"errors"
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
			if !errors.Is(err, tarp.ErrCorrupt) || !strings.Contains(err.Error(), "Restore") {
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
	wantLimits := tarp.RestoreLimits{CacheBytes: 128 + 1 + valueBytes, MaxRecordBytes: 48 + 1 + valueBytes}
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
		{CacheBytes: wantLimits.CacheBytes - 1, MaxRecordBytes: wantLimits.MaxRecordBytes},
	} {
		req.Limits = limits
		for _, err := range []error{info.CheckLimits(limits), tarp.Restore(t.Context(), req)} {
			var incompatible *tarp.CompatibilityError
			if !errors.Is(err, tarp.ErrIncompatibleOptions) || !errors.As(err, &incompatible) ||
				incompatible.RequiredCacheBytes != wantLimits.CacheBytes || incompatible.RequiredRecordBytes != wantLimits.MaxRecordBytes {
				t.Fatalf("checking limits %+v: %v", limits, err)
			}
		}
		if _, err := os.Lstat(req.Destination); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("incompatible restore destination: %v", err)
		}
	}
	opts := tarp.Options{CacheBytes: wantLimits.CacheBytes, MaxRecordBytes: wantLimits.MaxRecordBytes, ReadFileHandles: -1, TargetSegmentBytes: -1, BatchReadConcurrency: -1}
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

func TestBatchReadConcurrencyOnReopen(t *testing.T) {
	dir := t.TempDir()
	seed := openPublicDB(t, dir, tarp.Options{})
	a, b := []byte("a"), []byte("b")
	if err := seed.BatchPut([]tarp.Entry{{Key: a, Value: a}, {Key: b, Value: b}}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err := tarp.Open(dir, tarp.Options{BatchReadConcurrency: -1}); db != nil || !errors.Is(err, tarp.ErrInvalidOptions) {
		if db != nil {
			db.Close()
		}
		t.Fatalf("Open with negative BatchReadConcurrency returned %p, %v", db, err)
	}
	for _, limit := range []int{0, 1, 3} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			db := openPublicDB(t, dir, tarp.Options{BatchReadConcurrency: limit})
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
	f.Add([]byte{get, removeKey, putEmpty, get, backup, removeKey, reopen, restore, put, get})
	f.Add([]byte{backup, put, restore, backup, putEmpty, restore, compact})

	f.Fuzz(func(t *testing.T, steps []byte) {
		// A small target rotates on the second Put. All values fit admission.
		opts := tarp.Options{CacheBytes: 144, MaxRecordBytes: 64, TargetSegmentBytes: 128, ReadFileHandles: 1}
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
