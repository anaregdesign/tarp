package tarp_test

import (
	"bytes"
	"errors"
	"iter"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestRepairDataFromParityRecoversSealedDATAWhenOpenFails(t *testing.T) {
	dir := t.TempDir()
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("history-"), 1200)
	if err := db.Put([]byte("k"), value); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "db")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if err := tarp.RepairDataFromParity(t.Context(), alias); !errors.Is(err, tarp.ErrLocked) {
		t.Fatalf("repair under alias while Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	sidecars, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
	if err != nil || len(sidecars) != 1 {
		t.Fatalf("PARITY sidecars %v: %v", sidecars, err)
	}
	path := filepath.Join(dir, "DATA-"+strings.TrimPrefix(filepath.Base(sidecars[0]), "PARITY-"))
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(original)
	bad[0] ^= 1
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{}); got != nil || !errors.Is(err, tarp.ErrCorrupt) {
		t.Fatalf("Open with corrupt sealed header: %v", err)
	}
	if err := tarp.RepairDataFromParity(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, original) {
		t.Fatalf("bytes after repair: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := tarp.RepairDataFromParity(t.Context(), dir); err != nil {
		t.Fatal(err)
	}
	again, err := os.Stat(path)
	if err != nil || !os.SameFile(info, again) {
		t.Fatalf("healthy DATA replaced: %v", err)
	}
	reopened, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, err := reopened.Get([]byte("k")); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("Get after offline repair: %v", err)
	}
}

func TestParityRebuildsAfterRestoreAndCompaction(t *testing.T) {
	ctx := t.Context()
	parityFiles := func(dir string) []string {
		t.Helper()
		files, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
		if err != nil {
			t.Fatal(err)
		}
		return files
	}
	onlyParity := func(dir string) string {
		t.Helper()
		files := parityFiles(dir)
		if len(files) != 1 {
			t.Fatalf("PARITY sidecars in %s: %v", dir, files)
		}
		return files[0]
	}
	source := t.TempDir()
	db := openPublicDB(t, source, tarp.Options{})
	if err := db.Put([]byte("sealed"), []byte("protected")); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("active"), []byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if indexes, err := filepath.Glob(filepath.Join(source, "INDEX-*")); err != nil || len(indexes) != 0 {
		t.Fatalf("explicit seal created an index: %v, %v", indexes, err)
	}
	originals := make(map[string][]byte)
	for _, path := range parityFiles(source) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		originals[filepath.Base(path)] = b
	}
	if len(originals) != 2 {
		t.Fatalf("explicit seals protected %d files, want two", len(originals))
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if err := db.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	if files := parityFiles(backup); len(files) != 0 {
		t.Fatalf("file backup copied a PARITY sidecar: %v", files)
	}
	if info, err := tarp.VerifyBackup(ctx, backup); err != nil || info.LiveKeys != 2 {
		t.Fatalf("verify backup: %+v, %v", info, err)
	}

	restore := filepath.Join(t.TempDir(), "restored")
	if err := tarp.Restore(ctx, tarp.RestoreRequest{Source: backup, Destination: restore}); err != nil {
		t.Fatal(err)
	}
	if files := parityFiles(restore); len(files) != 0 {
		t.Fatalf("closed restored DB contains unexpected parity: %v", files)
	}
	restored := openPublicDB(t, restore, tarp.Options{})
	if err := restored.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	recreated := parityFiles(restore)
	if len(recreated) != len(originals) {
		t.Fatalf("regenerated %d PARITY sidecars, want %d", len(recreated), len(originals))
	}
	for _, path := range recreated {
		want, found := originals[filepath.Base(path)]
		if got, err := os.ReadFile(path); err != nil || !found || !bytes.Equal(got, want) {
			t.Fatalf("regenerated parity differs: %s, %v", path, err)
		}
	}

	result, err := restored.Compact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.ProtectParity(ctx); err != nil {
		t.Fatal(err)
	}
	if result.PinnedRetiredDataBytes > 0 {
		// ProtectParity has now joined any work on the old selection.
		if _, err := restored.Compact(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, old := range recreated {
		data := "DATA-" + strings.TrimPrefix(filepath.Base(old), "PARITY-")
		for _, path := range []string{old, filepath.Join(restore, data)} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retired object %s: %v", path, err)
			}
		}
	}
	newSidecar := onlyParity(restore)
	if _, reused := originals[filepath.Base(newSidecar)]; reused {
		t.Fatal("compacted snapshot reused retired parity")
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(restore, "DATA-"+strings.TrimPrefix(filepath.Base(newSidecar), "PARITY-"))
	if err := os.Truncate(snapshot, 0); err != nil {
		t.Fatal(err)
	}
	if err := tarp.RepairDataFromParity(ctx, restore); err != nil {
		t.Fatal(err)
	}
	repaired := openPublicDB(t, restore, tarp.Options{})
	for k, v := range map[string]string{"sealed": "protected", "active": "tail"} {
		if got, err := repaired.Get([]byte(k)); err != nil || string(got) != v {
			t.Fatalf("Get(%q) after recovering compacted snapshot: %q, %v", k, got, err)
		}
	}
}

func TestScanRepairsPinnedDataAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	db := openPublicDB(t, dir, tarp.Options{})
	old := map[string][]byte{"a": bytes.Repeat([]byte("first value"), 600), "b": bytes.Repeat([]byte("second value"), 600)}
	if err := db.BatchPut([]tarp.Entry{{Key: []byte("a"), Value: old["a"]}, {Key: []byte("b"), Value: old["b"]}}); err != nil {
		t.Fatal(err)
	}
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	parity, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
	if err != nil || len(parity) != 1 {
		t.Fatalf("expected one protected file: %v, %v", parity, err)
	}
	path := filepath.Join(dir, strings.Replace(filepath.Base(parity[0]), "PARITY-", "DATA-", 1))
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	next, stop := iter.Pull2(db.Scan(t.Context()))
	defer stop()
	first, err, ok := next()
	if err != nil || !ok {
		t.Fatalf("first record: %v, %t", err, ok)
	}
	key := "a"
	if key == string(first.Key) {
		key = "b"
	}
	newValue := []byte("replacement")
	if err := db.Put([]byte(key), newValue); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The scan owns a historical layout. Its old path and parity must remain
	// repairable even though a new snapshot is already serving current reads.
	at := bytes.Index(original, old[key])
	if at < 0 {
		t.Fatal("old record not in original file")
	}
	fd, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = fd.WriteAt([]byte{original[at] ^ 1}, int64(at))
	if err = errors.Join(err, fd.Close()); err != nil {
		t.Fatal(err)
	}
	second, err, ok := next()
	if !ok || err != nil || string(second.Key) != key || !bytes.Equal(second.Value, old[key]) {
		t.Fatalf("historical read returned key %q, ok=%t, err=%v", second.Key, ok, err)
	}
	if _, err, ok = next(); ok || err != nil {
		t.Fatalf("unexpected third record: %v", err)
	}
	if value, err := db.Get([]byte(key)); err != nil || !bytes.Equal(value, newValue) {
		t.Fatalf("live record after historical repair: %q, %v", value, err)
	}
}

func TestForegroundReadsRepairSealedDATA(t *testing.T) {
	for _, mode := range []struct {
		name string
		mode tarp.IndexMode
	}{{"memory", tarp.IndexMemory}, {"btree", tarp.IndexBTree}} {
		for _, method := range []string{"Get", "BatchGetSerial", "BatchGetParallel", "BatchGetContiguous", "Scan"} {
			t.Run(mode.name+"/"+method, func(t *testing.T) {
				dir := t.TempDir()
				opts := tarp.Options{IndexMode: mode.mode, BatchGetReadConcurrency: 4}
				if method == "BatchGetSerial" {
					opts.BatchGetReadConcurrency = 1
				}
				seed := openPublicDB(t, dir, opts)
				want := map[string][]byte{
					"a":     []byte("before"),
					"b":     []byte("damaged record value"),
					"c":     []byte("after"),
					"large": bytes.Repeat([]byte{'v'}, 5000),
					"warm":  []byte("cached"),
				}
				var entries []tarp.Entry
				for _, k := range []string{"a", "b", "c", "large", "warm"} {
					entries = append(entries, tarp.Entry{Key: []byte(k), Value: want[k]})
				}
				if err := seed.BatchPut(entries); err != nil {
					t.Fatal(err)
				}
				if err := seed.ProtectParity(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := seed.Close(); err != nil {
					t.Fatal(err)
				}

				db := openPublicDB(t, dir, opts)
				// Prime a reusable descriptor on the inode that will be damaged.
				if got, err := db.Get([]byte("warm")); err != nil || !bytes.Equal(got, want["warm"]) {
					t.Fatalf("priming Get: %q, %v", got, err)
				}
				data, _ := sealedTestPaths(t, dir)
				original, err := os.ReadFile(data)
				if err != nil {
					t.Fatal(err)
				}
				offset := bytes.Index(original, want["b"])
				if offset < 0 {
					t.Fatal("missing fixture record")
				}
				flipSealedTestBytes(t, data, bytes.Clone(original), offset)

				switch method {
				case "Get":
					if got, err := db.Get([]byte("b")); err != nil || !bytes.Equal(got, want["b"]) {
						t.Fatalf("Get: %q, %v", got, err)
					}
				case "Scan":
					seen := make(map[string]bool)
					for e, err := range db.Scan(t.Context()) {
						k := string(e.Key)
						if err != nil || seen[k] || !bytes.Equal(e.Value, want[k]) {
							t.Fatalf("Scan key %q, value length %d, error %v", k, len(e.Value), err)
						}
						seen[k] = true
					}
					if len(seen) != len(want) {
						t.Fatalf("scanned %d keys, want %d", len(seen), len(want))
					}
				default:
					// The backwards jump precludes coalescing. The 5 KB
					// record bypasses the adaptive small-read serial path.
					keys := [][]byte{[]byte("large"), []byte("b"), []byte("a"), []byte("b")}
					if method == "BatchGetContiguous" {
						keys = [][]byte{[]byte("a"), []byte("b"), []byte("c")}
					}
					got, err := db.BatchGet(keys)
					if err != nil || len(got) != len(keys) {
						t.Fatalf("BatchGet returned %d entries: %v", len(got), err)
					}
					for i, key := range keys {
						if !got[i].Found || !bytes.Equal(got[i].Value, want[string(key)]) {
							t.Fatalf("BatchGet slot %d did not return %q", i, key)
						}
					}
				}
				if got, err := os.ReadFile(data); err != nil || !bytes.Equal(got, original) {
					t.Fatalf("sealed DATA differs after %s: %v", method, err)
				}
				if err := db.Put([]byte("live"), []byte("next commit")); err != nil {
					t.Fatalf("Put after %s: %v", method, err)
				}
			})
		}
	}
}

func sealedTestPaths(t *testing.T, dir string) (data, parity string) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "PARITY-*"))
	if err != nil || len(files) != 1 {
		t.Fatalf("expected one protected file, got %v, %v", files, err)
	}
	name := strings.Replace(filepath.Base(files[0]), "PARITY-", "DATA-", 1)
	return filepath.Join(dir, name), files[0]
}

// Keep the file and the expected bytes in sync without ever truncating DATA.
func flipSealedTestBytes(t *testing.T, path string, wire []byte, offsets ...int) {
	t.Helper()
	fd, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fd.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, offset := range offsets {
		wire[offset] ^= 0xff
		if _, err := fd.WriteAt(wire[offset:offset+1], int64(offset)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestForegroundUnrepairableDATAIsolatedWhileHealthyRecordsRemainAvailable(t *testing.T) {
	for _, damage := range []string{"two lost blocks", "missing sidecar", "invalid sidecar"} {
		t.Run(damage, func(t *testing.T) {
			dir := t.TempDir()
			seed := openPublicDB(t, dir, tarp.Options{})
			size := 1000
			if damage == "two lost blocks" {
				size = 9000
			}
			entries := []tarp.Entry{
				{Key: []byte("bad"), Value: bytes.Repeat([]byte{'x'}, size)},
				{Key: []byte("survivor"), Value: []byte("healthy record")},
			}
			if err := seed.BatchPut(entries); err != nil {
				t.Fatal(err)
			}
			if err := seed.ProtectParity(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := tarp.Open(dir, tarp.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.ProtectParity(t.Context()); err != nil {
				t.Fatal(err)
			}
			data, parity := sealedTestPaths(t, dir)
			damaged, err := os.ReadFile(data)
			if err != nil {
				t.Fatal(err)
			}
			flipSealedTestBytes(t, data, damaged, 200)
			var wantCause error
			var offlineParity string
			switch damage {
			case "two lost blocks":
				flipSealedTestBytes(t, data, damaged, 4096+200)
				// This test exercises continued service before recovery, then
				// offline salvage. Keep the worker from publishing a checkpoint
				// between the foreground assertions.
				offlineParity = filepath.Join(t.TempDir(), "parity")
				err = os.Rename(parity, offlineParity)
				wantCause = os.ErrNotExist
			case "missing sidecar":
				wantCause = os.ErrNotExist
				err = os.Remove(parity)
			case "invalid sidecar":
				wantCause = tarp.ErrInvalidParity
				err = os.Truncate(parity, 0)
			}
			if err != nil {
				t.Fatal(err)
			}
			// Short records use a single contiguous read. The large record
			// is read independently in a batch with a verified neighbor.
			keys := [][]byte{entries[0].Key, entries[1].Key}
			if damage == "two lost blocks" {
				keys[0], keys[1] = keys[1], keys[0]
			}
			results, err := db.BatchGet(keys)
			var unavailable *tarp.UnavailableDataError
			if results != nil || !errors.Is(err, tarp.ErrDataUnavailable) || !errors.Is(err, tarp.ErrCorrupt) ||
				!errors.As(err, &unavailable) || (wantCause != nil && !errors.Is(err, wantCause)) {
				t.Fatalf("failed BatchGet returned %d results, %v", len(results), err)
			}
			id, err := strconv.ParseUint(data[strings.LastIndexByte(data, '-')+1:], 16, 64)
			if err != nil || unavailable.FileID != id {
				t.Fatalf("reported DATA ID %d for %s: %v", unavailable.FileID, data, err)
			}
			if value, err := db.Get(entries[0].Key); value != nil || !errors.Is(err, tarp.ErrDataUnavailable) || errors.Is(err, tarp.ErrNotFound) {
				t.Fatalf("Get unavailable key: %q, %v", value, err)
			}
			view, err := tarp.Bind(db, tarp.StringCodec{}, tarp.StringCodec{})
			if err != nil {
				t.Fatal(err)
			}
			if value, err := view.Get("bad"); value != "" || !errors.Is(err, tarp.ErrDataUnavailable) {
				t.Fatalf("typed Get returned %q, %v", value, err)
			}
			if values, err := view.BatchGet([]string{"survivor", "bad"}); values != nil || !errors.Is(err, tarp.ErrDataUnavailable) {
				t.Fatalf("typed BatchGet returned %+v, %v", values, err)
			}
			var terminal bool
			for entry, err := range db.Scan(t.Context()) {
				if terminal {
					t.Fatal("Scan yielded an entry after its terminal error")
				}
				if err != nil {
					if !errors.Is(err, tarp.ErrDataUnavailable) || entry.Key != nil || entry.Value != nil {
						t.Fatalf("Scan: %+v, %v", entry, err)
					}
					terminal = true
				}
			}
			if !terminal {
				t.Fatal("Scan hid an unavailable value")
			}
			if value, err := db.Get(entries[1].Key); err != nil || !bytes.Equal(value, entries[1].Value) {
				t.Fatalf("healthy record in the same sealed file: %q, %v", value, err)
			}
			var keyCount int
			for _, err := range db.Keys(t.Context()) {
				if err != nil {
					t.Fatal(err)
				}
				keyCount++
			}
			if keyCount != len(entries) {
				t.Fatalf("Keys returned %d keys, want %d", keyCount, len(entries))
			}
			if err := db.Put([]byte("next"), []byte("new value")); err != nil {
				t.Fatal(err)
			}
			if damage == "two lost blocks" {
				if err := db.Delete(entries[0].Key); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Get(entries[0].Key); !errors.Is(err, tarp.ErrNotFound) {
					t.Fatalf("Get after deleting unavailable key: %v", err)
				}
			}
			if err := db.Put(entries[0].Key, []byte("replacement")); err != nil {
				t.Fatal(err)
			}
			results, err = db.BatchGet([][]byte{entries[0].Key, entries[1].Key, []byte("next")})
			if err != nil || len(results) != 3 || string(results[0].Value) != "replacement" ||
				!bytes.Equal(results[1].Value, entries[1].Value) || string(results[2].Value) != "new value" {
				t.Fatalf("BatchGet after replacing unavailable key: %+v, %v", results, err)
			}
			if err := db.Close(); !errors.Is(err, tarp.ErrDataUnavailable) || errors.Is(err, tarp.ErrUnhealthy) {
				t.Fatalf("Close: %v", err)
			}
			if got, err := os.ReadFile(data); err != nil || !bytes.Equal(got, damaged) {
				t.Fatalf("failed repair changed DATA: %v", err)
			}
			if damage != "two lost blocks" {
				return
			}
			if err := os.Rename(offlineParity, parity); err != nil {
				t.Fatal(err)
			}
			var records []tarp.SalvagedRecord
			report, err := tarp.SalvageData(t.Context(), dir, unavailable.FileID, 0, func(r tarp.SalvagedRecord) error {
				records = append(records, r)
				return nil
			})
			if err != nil || len(records) != 1 || !bytes.Equal(records[0].Key, entries[1].Key) ||
				!bytes.Equal(records[0].Value, entries[1].Value) || report.Records != 1 ||
				len(report.UnrecoverableByteRanges) != 1 || report.UnrecoverableByteRanges[0] != (tarp.DataByteRange{Start: 0, End: 8192}) {
				t.Fatalf("salvage returned %d records, report %+v, error %v", len(records), report, err)
			}
		})
	}
}

func TestUnavailableForeignDATACannotSupplyAValidLookingRecord(t *testing.T) {
	// Equal record lengths and identical commit sequences give the foreign
	// records valid checksums and the exact addresses expected by this index.
	seed := func(dir, value string) string {
		t.Helper()
		db := openPublicDB(t, dir, tarp.Options{})
		var entries []tarp.Entry
		for _, key := range []string{"hot", "a", "b"} {
			entries = append(entries, tarp.Entry{Key: []byte(key), Value: []byte(value)})
		}
		if err := db.BatchPut(entries); err != nil {
			t.Fatal(err)
		}
		if err := db.ProtectParity(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		data, _ := sealedTestPaths(t, dir)
		return data
	}
	dir := t.TempDir()
	path := seed(dir, "local")
	foreign := seed(t.TempDir(), "other")
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.ProtectParity(t.Context()); err != nil {
		t.Fatal(err)
	}
	if value, err := db.Get([]byte("hot")); err != nil || string(value) != "local" {
		t.Fatalf("priming verified cache: %q, %v", value, err)
	}
	_, parity := sealedTestPaths(t, dir)
	if err := os.Remove(parity); err != nil {
		t.Fatal(err)
	}
	wrong, err := os.ReadFile(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, wrong, 0600); err != nil {
		t.Fatal(err)
	}
	if err := db.Backup(t.Context(), filepath.Join(t.TempDir(), "backup")); !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatalf("Backup did not detect foreign DATA: %v", err)
	}
	if value, err := db.Get([]byte("a")); value != nil || !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatalf("Get accepted a foreign record: %q, %v", value, err)
	}
	if values, err := db.BatchGet([][]byte{[]byte("a"), []byte("b")}); values != nil || !errors.Is(err, tarp.ErrDataUnavailable) {
		t.Fatalf("contiguous BatchGet accepted foreign records: %+v, %v", values, err)
	}
	failures := 0
	for entry, err := range db.Scan(t.Context()) {
		if !errors.Is(err, tarp.ErrDataUnavailable) || entry.Key != nil || entry.Value != nil {
			t.Fatalf("Scan accepted a foreign record: %+v, %v", entry, err)
		}
		failures++
	}
	if failures != 1 {
		t.Fatalf("Scan reported %d failures, want one", failures)
	}
	if value, err := db.Get([]byte("hot")); err != nil || string(value) != "local" {
		t.Fatalf("cached local value: %q, %v", value, err)
	}
}
