package tarp_test

import (
	"context"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

func TestRestoreVerifiesOnlySelectedLimits(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	req := tarp.RestoreRequest{Source: filepath.Join(dir, "backup"), Destination: filepath.Join(dir, "restored")}
	if err := db.Backup(t.Context(), req.Source); err != nil {
		t.Fatal(err)
	}
	req.Limits.MaxRecordBytes = -1
	if err := tarp.Restore(t.Context(), req); !errors.Is(err, tarp.ErrInvalidOptions) {
		t.Fatalf("invalid restore limit: %v", err)
	}
	req.Limits = tarp.RestoreLimits{CacheBytes: 129}
	if err := tarp.Restore(t.Context(), req); !errors.Is(err, tarp.ErrIncompatibleOptions) {
		t.Fatalf("incompatible cache limit: %v", err)
	}
	opts := tarp.Options{CacheBytes: 130, MaxRecordBytes: 50, ReadFileHandles: -1, TargetSegmentBytes: -1, BatchReadConcurrency: -1}
	req.Limits = opts.RestoreLimits()
	if err := tarp.Restore(t.Context(), req); err != nil {
		t.Fatalf("restore with exact live record limits: %v", err)
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
			result, err := db.Compact(tc.ctx)
			check("compact", err)
			if result != (tarp.CompactResult{}) {
				t.Fatalf("Compact returned %+v", result)
			}
			check("backup", db.Backup(tc.ctx, target))
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
