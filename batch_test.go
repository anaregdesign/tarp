package tarp_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/anaregdesign/tarp"
)

// The final state identifies the order of the two overlapping commits. Every
// earlier BatchGet must then fit that same order, including after recovery.
func TestBatchSnapshotsAgreeWithRecoveredCommitOrder(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	a, b, c := []byte("a"), []byte("b"), []byte("c")
	keys := [][]byte{a, b, c, b}
	initial := [3]string{"a0", "b0", "c0"}
	if err := db.BatchPut([]tarp.Entry{{Key: a, Value: []byte("a0")}, {Key: b, Value: []byte("b0")}, {Key: c, Value: []byte("c0")}}); err != nil {
		t.Fatal(err)
	}
	read := func(reader *tarp.DB) [3]string {
		t.Helper()
		rows, err := reader.BatchGet(keys)
		if err != nil || len(rows) != len(keys) {
			t.Fatalf("BatchGet: %v, %v", rows, err)
		}
		if rows[1].Found != rows[3].Found || !bytes.Equal(rows[1].Value, rows[3].Value) {
			t.Fatalf("duplicate key changed within one snapshot: %v", rows)
		}
		var state [3]string
		for i := range state {
			if rows[i].Found {
				if len(rows[i].Value) == 0 {
					t.Fatalf("unexpected empty value at %d", i)
				}
				state[i] = string(rows[i].Value)
			}
		}
		return state
	}
	seen := [][3]string{read(db)}
	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		done <- db.BatchPut([]tarp.Entry{{Key: b, Value: []byte("transient")}, {Key: a, Value: []byte("a1")}, {Key: b, Value: []byte("b1")}})
	}()
	go func() {
		<-start
		done <- db.BatchDelete([][]byte{b, c, b})
	}()
	close(start)
	for completed := 0; completed < 2; {
		state := read(db)
		if seen[len(seen)-1] != state {
			seen = append(seen, state)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			completed++
		default:
		}
	}
	// c is absent in both final states. A failed delete must leave a intact.
	if err := db.BatchDelete([][]byte{a, c}); !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("delete including absent c: %v", err)
	}
	live := read(db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := tarp.OpenExisting(t.Context(), dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if got := read(reopened); got != live {
		t.Fatalf("durable state %v, last live state %v", got, live)
	}
	orders := [2][3][3]string{
		{initial, {"a1", "b1", "c0"}, {"a1", "", ""}},
		{initial, {"a0", "", ""}, {"a1", "b1", ""}},
	}
	for _, order := range orders {
		if order[2] != live {
			continue
		}
		path := order[:]
		for _, state := range seen {
			at := slices.Index(path, state)
			if at < 0 {
				t.Fatalf("snapshot history %v contradicts recovered order %v", seen, order)
			}
			path = path[at+1:]
		}
		return
	}
	t.Fatalf("no whole-commit order yields %v", live)
}

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
