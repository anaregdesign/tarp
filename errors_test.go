package tarp_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestRequiredImageFilesDistinguishAccessFailureFromCorruption(t *testing.T) {
	dir := t.TempDir()
	db := openPublicDB(t, dir, tarp.Options{})
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, dir string
		files     []string
		check     func() error
	}{
		{"OpenExisting", dir, []string{"CURRENT", "MANIFEST-*", "DATA-*"}, func() error {
			handle, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{})
			if err != nil {
				return err
			}
			return handle.Close()
		}},
		{"VerifyBackup", backup, []string{"BACKUP", "CURRENT", "MANIFEST-*", "DATA-*"}, func() error {
			_, err := tarp.VerifyBackup(t.Context(), backup)
			return err
		}},
	} {
		for _, pattern := range tc.files {
			t.Run(tc.name+"/"+pattern, func(t *testing.T) {
				files, err := filepath.Glob(filepath.Join(tc.dir, pattern))
				if err != nil || len(files) != 1 {
					t.Fatalf("required file %q: %v, %v", pattern, files, err)
				}
				path := files[0]
				t.Run("permission", func(t *testing.T) {
					if os.Geteuid() == 0 {
						t.Skip("root can read mode-000 files")
					}
					if err := os.Chmod(path, 0); err != nil {
						t.Fatal(err)
					}
					checkErr := tc.check()
					if err := os.Chmod(path, 0600); err != nil {
						t.Fatal(err)
					}
					var pathErr *os.PathError
					if !errors.Is(checkErr, os.ErrPermission) || errors.Is(checkErr, tarp.ErrCorrupt) ||
						!errors.As(checkErr, &pathErr) || filepath.Base(pathErr.Path) != filepath.Base(path) {
						t.Fatalf("unreadable %q: %v", path, checkErr)
					}
				})
				// Move the object outside the image to simulate a missing required file.
				moved := filepath.Join(t.TempDir(), "saved")
				if err := os.Rename(path, moved); err != nil {
					t.Fatal(err)
				}
				checkErr := tc.check()
				if err := os.Rename(moved, path); err != nil {
					t.Fatal(err)
				}
				if !errors.Is(checkErr, os.ErrNotExist) || !errors.Is(checkErr, tarp.ErrCorrupt) {
					t.Fatalf("missing %q: %v", path, checkErr)
				}
				if err := tc.check(); err != nil {
					t.Fatalf("retry after restoring %q: %v", path, err)
				}
			})
		}
	}
}

func TestLiveReadsReportCorruptRecordLocation(t *testing.T) {
	for _, op := range []string{"get", "batch-get", "scan"} {
		t.Run(op, func(t *testing.T) {
			dir := t.TempDir()
			// Keep this DATA active across Close so a parity repair cannot mask the read error.
			db := openPublicDB(t, dir, tarp.Options{IndexMode: tarp.IndexMemory})
			key := []byte("k")
			if err := db.Put(key, []byte("first")); err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(dir, "DATA-*"))
			if err != nil || len(files) != 1 {
				t.Fatalf("expected one DATA file, got %v, %v", files, err)
			}
			path := files[0]
			st, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			// The second Put appends the live record at the previous end of DATA.
			want := fmt.Sprintf("%s: record at offset %d:", filepath.Base(path), st.Size())
			if err := db.Put(key, []byte("second")); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			cold, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer cold.Close()

			// Damage the live value after recovery, while the cache is cold.
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)-1] ^= 1
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			switch op {
			case "get":
				_, err = cold.Get(key)
			case "batch-get":
				_, err = cold.BatchGet([][]byte{key})
			case "scan":
				for _, scanErr := range cold.Scan(t.Context()) {
					if scanErr != nil {
						err = scanErr
						break
					}
				}
			}
			var failure *tarp.OpError
			if !errors.Is(err, tarp.ErrCorrupt) || !errors.As(err, &failure) || failure.Operation != op ||
				failure.Phase != "read" || failure.Outcome != tarp.NotApplied || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected a read error at %q, got %v", want, err)
			}
			if _, err := cold.Get(key); !errors.Is(err, tarp.ErrUnhealthy) || !errors.Is(err, tarp.ErrCorrupt) || !strings.Contains(err.Error(), want) {
				t.Fatalf("expected unhealthy DB to retain the read error, got %v", err)
			}
		})
	}
}

func TestPublicDBReportsSizeAndReopenLimits(t *testing.T) {
	// A one-byte key and a three-byte value occupy a 52-byte record
	// and have a 132-byte logical cache charge.
	for _, tc := range []struct {
		name          string
		cache, record int64
	}{
		{"cache", 132, 64},
		{"record", 256, 52},
		{"both", 132, 52},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := tarp.Options{ValueCacheBudgetBytes: tc.cache, MaxRecordBytes: tc.record}
			db := openPublicDB(t, dir, opts)
			key := []byte("k")
			if err := db.Put(key, []byte("old")); err != nil {
				t.Fatal(err)
			}
			err := db.Put(key, []byte("over"))
			var size *tarp.SizeError
			if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &size) {
				t.Fatalf("oversized Put = %v", err)
			}
			if size.KeyBytes != 1 || size.ValueBytes != 4 || size.RecordBytes != 53 || size.ValueCacheChargeBytes != 133 ||
				size.ValueCacheBudgetBytes != tc.cache || size.MaxRecordBytes != tc.record || size.Overflow {
				t.Fatalf("incorrect admission costs or limits: %+v", size)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			opts.ValueCacheBudgetBytes--
			opts.MaxRecordBytes--
			failed, err := tarp.Open(dir, opts)
			var required *tarp.CompatibilityError
			if failed != nil || !errors.Is(err, tarp.ErrIncompatibleOptions) || !errors.As(err, &required) {
				t.Fatalf("Open with smaller limits = %v, %v", failed, err)
			}
			if required.RequiredValueCacheBudgetBytes != 132 || required.RequiredRecordBytes != 52 {
				t.Fatalf("wrong reopen minima: %+v", required)
			}
			exact := tarp.Options{ValueCacheBudgetBytes: required.RequiredValueCacheBudgetBytes, MaxRecordBytes: required.RequiredRecordBytes}
			if value, err := openPublicDB(t, dir, exact).Get(key); string(value) != "old" || err != nil {
				t.Fatalf("Get after reopen at reported minima = %q, %v", value, err)
			}
		})
	}
}

func TestBatchLimitsReportOriginalInputAndPreserveSizeCause(t *testing.T) {
	opts := tarp.Options{ValueCacheBudgetBytes: 132, MaxRecordBytes: 52, TargetSegmentBytes: 164}
	db := openPublicDB(t, t.TempDir(), opts) // 100-byte segment payload
	view, err := tarp.Bind(db, tarp.StringCodec{}, tarp.StringCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if err := view.Put("a", "old"); err != nil {
		t.Fatal(err)
	}
	initial := db.Stats()
	count := tarp.MaxBatchEntries + 1
	for _, tc := range []struct {
		name   string
		run    func() error
		kind   tarp.BatchLimitKind
		index  int
		actual uint64
		limit  uint64
	}{
		{"raw get count", func() error { _, err := db.BatchGet(make([][]byte, count)); return err }, tarp.BatchInputCountLimit, -1, uint64(count), tarp.MaxBatchEntries},
		{"typed put count", func() error { return view.BatchPut(make([]tarp.TypedEntry[string, string], count)) }, tarp.BatchInputCountLimit, -1, uint64(count), tarp.MaxBatchEntries},
		{"raw put entry", func() error {
			return db.BatchPut([]tarp.Entry{{Key: []byte("a")}, {Key: []byte("a"), Value: []byte("long")}})
		}, tarp.BatchEntrySizeLimit, 1, 0, 0},
		{"typed put entry", func() error {
			return view.BatchPut([]tarp.TypedEntry[string, string]{{Key: "a"}, {Key: "a", Value: "long"}})
		}, tarp.BatchEntrySizeLimit, 1, 0, 0},
		{"repeated put payload", func() error {
			return view.BatchPut([]tarp.TypedEntry[string, string]{{Key: "a"}, {Key: "a"}, {Key: "a"}})
		}, tarp.BatchPayloadLimit, 2, 147, 100},
		{"distinct delete payload", func() error { return view.BatchDelete([]string{"a", "a", "b", "c", "d"}) }, tarp.BatchPayloadLimit, 3, 147, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			var limit *tarp.BatchLimitError
			if !errors.Is(err, tarp.ErrTooLarge) || !errors.As(err, &limit) || limit.Kind != tc.kind ||
				limit.Index != tc.index || limit.Actual != tc.actual || limit.Limit != tc.limit {
				t.Fatalf("incorrect batch limit: %v (%+v)", err, limit)
			}
			var entry *tarp.SizeError
			if tc.kind == tarp.BatchEntrySizeLimit {
				if !errors.As(err, &entry) || entry != limit.Entry || entry.KeyBytes != 1 || entry.ValueBytes != 4 ||
					entry.RecordBytes != 53 || entry.ValueCacheChargeBytes != 133 || entry.MaxRecordBytes != 52 || entry.ValueCacheBudgetBytes != 132 {
					t.Fatalf("lost entry size details: %+v", entry)
				}
			} else if errors.As(err, &entry) || limit.Entry != nil {
				t.Fatalf("unexpected size error: %+v", entry)
			}
			if got := db.Stats(); got != initial {
				t.Fatalf("rejected batch changed Stats: %+v", got)
			}
		})
	}
	if value, err := view.Get("a"); value != "old" || err != nil {
		t.Fatalf("rejected batch changed value: %q, %v", value, err)
	}
}
