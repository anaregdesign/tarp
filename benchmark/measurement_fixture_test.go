package benchmark

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp/internal/cache"
	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

func newTestDB(t *testing.T, opts engine.Options, fs *storage.IO) *engine.DB {
	t.Helper()
	db, err := engine.OpenWithIO(filepath.Join(t.TempDir(), "db"), opts, fs)
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
	db, err := engine.OpenWithIO(target, opts, &storage.IO{CleanerStart: make(chan struct{})})
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
	db, err := engine.OpenWithIO(path, opts, hooks.interferenceIO())
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
	var keys []string
	engine.Observe(db).State(func(index map[string]storage.Location, _ *cache.Cache) {
		keys = make([]string, 0, len(index))
		for key := range index {
			keys = append(keys, key)
		}
	})
	sort.Strings(keys)
	hash := sha256.New()
	var length [8]byte
	for _, key := range keys {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatal(err)
		}
		binary.LittleEndian.PutUint64(length[:], uint64(len(key)))
		hash.Write(length[:])
		io.WriteString(hash, key)
		binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
		hash.Write(length[:])
		hash.Write(value)
	}
	return hex.EncodeToString(hash.Sum(nil)), len(keys)
}

type measurementCapture struct {
	parent   string
	uuid     [16]byte
	sequence uint64
	reached  bool
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
	c.sequence, c.reached = current.Sequence, true
	return nil
}
