package benchmark

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func newTestDB(t *testing.T, opts engine.Options, fs *storage.IO) *engine.DB {
	t.Helper()
	return newTestDBWithHooks(t, opts, fs, nil)
}

func newTestDBWithHooks(t *testing.T, opts engine.Options, fs *storage.IO, hooks *engine.Hooks) *engine.DB {
	t.Helper()
	db, err := engine.OpenWithHooks(filepath.Join(t.TempDir(), "db"), opts, fs, hooks)
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

func mustPut(t *testing.T, db *engine.DB, key, value string) {
	t.Helper()
	if err := db.Put([]byte(key), []byte(value)); err != nil {
		t.Fatal(err)
	}
}

// Only closed, private, flat test fixtures may use this preparation copy.
// It is not an online backup mechanism.
func measurementCloneClosed(t testing.TB, source, target string) {
	t.Helper()
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("fixture is not a flat regular-file directory: %s %v", entry.Name(), err)
		}
		src, err := os.Open(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		dst, err := os.OpenFile(filepath.Join(target, entry.Name()), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(errors.Join(err, src.Close()))
		}
		n, copyErr := io.Copy(dst, src)
		if err := errors.Join(copyErr, src.Close(), dst.Sync(), dst.Close()); err != nil {
			t.Fatal(err)
		}
		if n != info.Size() {
			t.Fatal("closed fixture changed during its preparation copy")
		}
	}
	fd, err := os.Open(target)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(fd.Sync(), fd.Close()); err != nil {
		t.Fatal(err)
	}
}

func measurementPrepareFixture(t *testing.T, source, target string, opts engine.Options, keys [][]byte, value []byte) {
	t.Helper()
	measurementCloneClosed(t, source, target)
	db, err := engine.OpenWithHooks(target, opts, nil, &engine.Hooks{CleanerStart: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	for _, key := range keys[:min(1024, len(keys))] {
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range keys[:min(256, len(keys)/4)] {
		if err := db.Delete(key); err != nil {
			t.Fatal(err)
		}
		if err := db.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
}

func measurementOpenClone(t *testing.T, source string, opts engine.Options) (*engine.DB, *measurementHooks) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	measurementCloneClosed(t, source, path)
	hooks := &measurementHooks{}
	fs, engineHooks := hooks.interferenceHooks()
	db, err := engine.OpenWithHooks(path, opts, fs, engineHooks)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, hooks
}

func measurementState(t testing.TB, db *engine.DB) (string, int) {
	t.Helper()
	hash := sha256.New()
	var length [8]byte
	count := 0
	record := func(key []byte) {
		value, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(length[:], uint64(len(key)))
		hash.Write(length[:])
		hash.Write(key)
		binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
		hash.Write(length[:])
		hash.Write(value)
		count++
	}
	// The historical measurement digest orders the encoded keys. The B+tree
	// cursor already traverses ordered leaves, so stream it without a key list.
	// Ordering is a property of this backend, not part of the public Keys API.
	if engine.Observe(db).Options().IndexMode == engine.IndexBTree {
		var last []byte
		for key, err := range db.Keys(t.Context()) {
			if err != nil {
				t.Fatal(err)
			}
			if last != nil && bytes.Compare(key, last) <= 0 {
				t.Fatal("unordered B+tree keys in measurement digest")
			}
			record(key)
			last = key
		}
	} else {
		var keys []string
		for key, err := range db.Keys(t.Context()) {
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, string(key))
		}
		sort.Strings(keys)
		for _, key := range keys {
			record([]byte(key))
		}
	}
	return hex.EncodeToString(hash.Sum(nil)), count
}

type measurementCapture struct {
	parent   string
	uuid     [16]byte
	stage    string
	frontier format.Frontier
}

func (c *measurementCapture) observe() error {
	entries, err := os.ReadDir(c.parent)
	if err != nil {
		return err
	}
	stage := ""
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".tarp-backup-") {
			if stage != "" {
				return errors.New("measurement saw multiple backup stages")
			}
			stage = filepath.Join(c.parent, entry.Name())
		}
	}
	if stage == "" {
		return errors.New("measurement missed the backup capture stage")
	}
	data, err := os.ReadFile(filepath.Join(stage, "CURRENT"))
	if err != nil {
		return err
	}
	current, err := format.DecodeFrontier(data)
	if err != nil {
		return err
	}
	if current.UUID != c.uuid {
		return errors.New("measurement captured a different fixture UUID")
	}
	c.stage, c.frontier = stage, current
	return nil
}

// Only the pin witness needs the captured catalog. Read it while the backup
// is still parked and its staging directory has not been renamed.
func (c *measurementCapture) dataIDs() ([]uint64, error) {
	if c.stage == "" {
		return nil, errors.New("measurement missed the backup capture stage")
	}
	manifest := storage.ManifestName(c.frontier.UUID, c.frontier.ManifestID)
	data, err := storage.ReadRegular(filepath.Join(c.stage, manifest))
	if err != nil {
		return nil, fmt.Errorf("read captured catalog: %w", err)
	}
	if sha256.Sum256(data) != c.frontier.ManifestDigest {
		return nil, errors.New("captured catalog digest does not match staged CURRENT")
	}
	catalog, err := format.DecodeCatalog(data)
	if err != nil {
		return nil, fmt.Errorf("decode captured catalog: %w", err)
	}
	active, err := catalog.ActiveEntry(c.frontier)
	if err != nil {
		return nil, fmt.Errorf("match captured catalog to staged CURRENT: %w", err)
	}
	ids := make([]uint64, 0, len(catalog.Entries)+1)
	for _, entry := range catalog.Entries {
		if entry.HasPhysicalFile() {
			ids = append(ids, entry.ID)
		}
	}
	return append(ids, active.ID), nil
}
