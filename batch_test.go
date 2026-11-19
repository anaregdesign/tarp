package tarp_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestBatchRoundTripAcrossCheckpointBackupAndReopen(t *testing.T) {
	parent := t.TempDir()
	dir := filepath.Join(parent, "db")
	opts := tarp.Options{MaxRecordBytes: 100, TargetSegmentBytes: 250}
	open := func(path string) *tarp.DB {
		t.Helper()
		db, err := tarp.Open(path, opts)
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
	db := open(dir)
	a, b := []byte("a"), []byte("b")
	v := []byte("latest")
	keys := [][]byte{a, b, []byte("missing"), a}
	check := func(db *tarp.DB) {
		t.Helper()
		r, err := db.BatchGet(keys)
		if err != nil || len(r) != 4 {
			t.Fatalf("BatchGet = %+v, %v", r, err)
		}
		if !r[0].Found || string(r[0].Value) != "latest" || !r[1].Found || len(r[1].Value) != 0 ||
			r[2].Found || !r[3].Found || !bytes.Equal(r[0].Value, r[3].Value) {
			t.Fatalf("BatchGet results = %+v", r)
		}
		r[0].Value[0] = 'X'
		if string(r[3].Value) != "latest" {
			t.Fatal("duplicate results share backing bytes")
		}
	}
	if err := db.BatchPut([]tarp.Entry{{Key: a, Value: []byte("old")}, {Key: b}, {Key: a, Value: v}}); err != nil {
		t.Fatal(err)
	}
	v[0] = 'x'
	if err := db.BatchDelete(keys); !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("delete with missing key = %v", err)
	}
	check(db)
	if _, err := db.Compact(t.Context()); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(parent, "backup")
	if err := db.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchDelete([][]byte{b, a, b}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(dir)
	if r, err := reopened.BatchGet(keys[:2]); err != nil || len(r) != 2 || r[0].Found || r[1].Found {
		t.Fatalf("deleted keys after reopen = %+v, %v", r, err)
	}
	restored := filepath.Join(parent, "restored")
	if err := tarp.Restore(t.Context(), tarp.RestoreRequest{
		Source:      backup,
		Destination: restored,
		Limits:      opts.RestoreLimits(),
	}); err != nil {
		t.Fatal(err)
	}
	check(open(restored))
}
