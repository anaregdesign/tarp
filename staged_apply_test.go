package tarp_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"iter"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func assertStagedValues(t *testing.T, db *tarp.DB, want map[string]string, missing ...string) {
	t.Helper()
	for key, value := range want {
		got, err := db.Get([]byte(key))
		if err != nil || string(got) != value {
			t.Fatalf("Get(%q) = %q, %v; want %q", key, got, err, value)
		}
	}
	for _, key := range missing {
		if _, err := db.Get([]byte(key)); !errors.Is(err, tarp.ErrNotFound) {
			t.Fatalf("Get(%q) after deletion: %v", key, err)
		}
	}
}

func assertStagedSequence(t *testing.T, db *tarp.DB, want uint64) {
	t.Helper()
	snapshot, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if got := snapshot.Sequence(); got != want {
		t.Fatalf("snapshot sequence = %d; want %d", got, want)
	}
}

func TestApplyStagedPublishesMixedChangesAcrossSegments(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	opts := tarp.Options{MaxRecordBytes: 128, TargetSegmentBytes: 192}
	db := openPublicDB(t, dir, opts)
	for _, row := range []struct{ key, value string }{{"a", "old-a"}, {"b", "old-b"}, {"z", "keep"}} {
		if err := db.Put([]byte(row.key), []byte(row.value)); err != nil {
			t.Fatal(err)
		}
	}
	prior, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer prior.Close()
	assertStagedValues(t, db, map[string]string{"a": "old-a", "b": "old-b"})
	before := db.Stats()
	mutations := func(yield func(tarp.StagedMutation, error) bool) {
		key, value := make([]byte, 1), make([]byte, 60)
		for _, current := range []byte("abcdef") {
			key[0] = current
			for index := range value {
				value[index] = current
			}
			if !yield(tarp.StagedMutation{Key: key, Value: value, Delete: current == 'a' || current == 'c'}, nil) {
				return
			}
			// The producer may immediately reuse both buffers.
			key[0], value[0] = '-', '-'
		}
	}
	if err := db.ApplyStaged(ctx, mutations); err != nil {
		t.Fatal(err)
	}
	after := db.Stats()
	const liveBytes = 5*(48+1) + 4*60 + uint64(len("keep"))
	if after.LiveKeys != 5 || after.LiveRecordBytes != liveBytes || after.SelectedSegments < 4 ||
		after.WrittenRecords != before.WrittenRecords+5 || after.WriteCommits != before.WriteCommits+1 {
		t.Fatalf("staged stats before %+v, after %+v", before, after)
	}
	assertStagedSequence(t, db, 8)
	want := map[string]string{"z": "keep"}
	for _, key := range []string{"b", "d", "e", "f"} {
		want[key] = strings.Repeat(key, 60)
	}
	assertStagedValues(t, db, want, "a", "c")
	if prior.Sequence() != 3 {
		t.Fatalf("old snapshot sequence advanced to %d", prior.Sequence())
	}
	for key, value := range map[string]string{"a": "old-a", "b": "old-b", "z": "keep"} {
		if got, err := prior.Get([]byte(key)); err != nil || string(got) != value {
			t.Fatalf("old snapshot Get(%q) = %q, %v; want %q", key, got, err, value)
		}
	}
	if _, err := prior.Get([]byte("d")); !errors.Is(err, tarp.ErrNotFound) {
		t.Fatalf("old snapshot observed staged insert: %v", err)
	}
	if err := prior.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []struct {
		name  string
		value tarp.OpenValidation
	}{{"Full", tarp.Full}, {"Indexed", tarp.Indexed}} {
		t.Run(mode.name, func(t *testing.T) {
			opts.OpenValidation = mode.value
			reopened, err := tarp.OpenExisting(t.Context(), dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			assertStagedSequence(t, reopened, 8)
			assertStagedValues(t, reopened, want, "a", "c")
			if got := reopened.Stats(); got.LiveKeys != 5 || got.LiveRecordBytes != liveBytes {
				t.Fatalf("reopened stats = %+v", got)
			}
		})
	}
}

func TestApplyStagedCrossesOrdinaryWriteBatchCountLimit(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{MaxRecordBytes: 1024, TargetSegmentBytes: 32 << 10})
	const count = tarp.MaxWriteBatchEntries + 1
	if err := db.ApplyStaged(t.Context(), func(yield func(tarp.StagedMutation, error) bool) {
		var key [4]byte
		for i := range count {
			binary.BigEndian.PutUint32(key[:], uint32(i))
			if !yield(tarp.StagedMutation{Key: key[:], Value: key[1:]}, nil) {
				return
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	if got := db.Stats(); got.LiveKeys != count || got.WrittenRecords != count || got.WriteCommits != 1 || got.SelectedSegments < 2 {
		t.Fatalf("staged stats = %+v", got)
	}
	assertStagedSequence(t, db, count)
	var scanned int
	for row, err := range db.ScanRange(t.Context(), nil, nil) {
		if err != nil {
			t.Fatal(err)
		}
		if len(row.Key) != 4 || binary.BigEndian.Uint32(row.Key) != uint32(scanned) || !bytes.Equal(row.Value, row.Key[1:]) {
			t.Fatalf("staged row %d has key %x and value %x", scanned, row.Key, row.Value)
		}
		scanned++
	}
	if scanned != count {
		t.Fatalf("staged scan returned %d keys, want %d", scanned, count)
	}
}

func TestApplyStagedRejectsIncompleteOrUnorderedStream(t *testing.T) {
	db := openPublicDB(t, t.TempDir(), tarp.Options{})
	if err := db.Put([]byte("a"), []byte("old")); err != nil {
		t.Fatal(err)
	}
	producerError := errors.New("producer failed")
	for _, tc := range []struct {
		name, first, next string
		fail              error
		cancel            bool
	}{
		{name: "producer error", first: "a", fail: producerError},
		{name: "duplicate", first: "a", next: "a"},
		{name: "out of order", first: "b", next: "a"},
		{name: "context canceled", first: "a", fail: context.Canceled, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			input := iter.Seq2[tarp.StagedMutation, error](func(yield func(tarp.StagedMutation, error) bool) {
				if !yield(tarp.StagedMutation{Key: []byte(tc.first), Value: []byte("tentative")}, nil) {
					return
				}
				if tc.cancel {
					cancel()
					return
				}
				yield(tarp.StagedMutation{Key: []byte(tc.next), Value: []byte("tentative")}, tc.fail)
			})
			err := db.ApplyStaged(ctx, input)
			var op *tarp.OpError
			if !errors.As(err, &op) || op.Outcome != tarp.NotApplied {
				t.Fatalf("staged stream failure = %v; want NotApplied", err)
			}
			if tc.fail != nil && !errors.Is(err, tc.fail) || tc.fail == nil && !strings.Contains(err.Error(), "keys must be strictly increasing") {
				t.Fatalf("wrong staged stream failure: %v", err)
			}
			assertStagedSequence(t, db, 1)
			assertStagedValues(t, db, map[string]string{"a": "old"}, "b")
			if got := db.Stats(); got.WriteCommits != 1 || got.WrittenRecords != 1 || got.LiveKeys != 1 {
				t.Fatalf("rejected staged stats = %+v", got)
			}
		})
	}
}
