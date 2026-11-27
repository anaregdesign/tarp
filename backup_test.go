package tarp_test

import (
	"errors"
	"maps"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestTypedBackupWithSnapshotWritesSameImageAsBackup(t *testing.T) {
	db, err := tarp.OpenTyped(t.TempDir(), tarp.Options{}, tarp.BytesCodec{}, tarp.Uint16Codec{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("key"), 0x1234); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	plain, withSnapshot := filepath.Join(dir, "plain"), filepath.Join(dir, "with-snapshot")
	ctx := t.Context()
	if err := db.Backup(ctx, plain); err != nil {
		t.Fatal(err)
	}
	var borrowed *tarp.TypedSnapshot[[]byte, uint16]
	err = db.BackupWithSnapshot(ctx, withSnapshot, func(s *tarp.TypedSnapshot[[]byte, uint16]) error {
		borrowed = s
		value, err := s.Get([]byte("key"))
		if value != 0x1234 || err != nil || s.Sequence() != 1 {
			t.Errorf("typed snapshot: value %#x, sequence %d, error %v", value, s.Sequence(), err)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if borrowed == nil {
		t.Fatal("callback did not run")
	}
	if _, err := borrowed.Get([]byte("key")); !errors.Is(err, tarp.ErrClosed) {
		t.Fatalf("typed borrowed snapshot still readable: %v", err)
	}
	if !maps.Equal(inspectInventory(t, plain, ""), inspectInventory(t, withSnapshot, "")) {
		t.Fatal("supplying a snapshot changed the files or bytes in the native image")
	}
}
