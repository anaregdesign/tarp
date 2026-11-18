package tarp

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

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/storage"
)

// Only closed, private, flat test fixtures may use this preparation copy.
// It is not an online backup mechanism.
func measurementCloneClosed(t *testing.T, source, target string) {
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

func measurementPrepareFixture(t *testing.T, source, target string, opts Options, keys [][]byte, value []byte) {
	t.Helper()
	measurementCloneClosed(t, source, target)
	db, err := Open(target, opts)
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

func measurementOpenClone(t *testing.T, source string, opts Options) (*DB, *measurementHooks) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	measurementCloneClosed(t, source, path)
	hooks := &measurementHooks{}
	db, err := openWithIO(path, opts, &storage.IO{Hook: hooks.hook})
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

// These probes run with an active cleaner, even in fixtures with no foreground writers.
func observedFrontier(db *DB) format.Frontier {
	db.writer.Lock()
	defer db.writer.Unlock()
	return db.store.Current
}

func observedLiveKeys(db *DB) int {
	db.state.Lock()
	defer db.state.Unlock()
	return len(db.index)
}

func measurementState(t *testing.T, db *DB) (string, int) {
	t.Helper()
	db.state.Lock()
	keys := make([]string, 0, len(db.index))
	for key := range db.index {
		keys = append(keys, key)
	}
	db.state.Unlock()
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
