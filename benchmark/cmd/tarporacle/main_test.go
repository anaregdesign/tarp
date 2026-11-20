package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

func TestVerifyRequiresAnInitializedRecoveredDatabase(t *testing.T) {
	root := t.TempDir()
	log := filepath.Join(root, "oracle.jsonl")
	input := "TARP_ACK 6b 76\nTARP_ACK_DELETE 6b\nTARP_PHASE current.rename:after\n"
	if err := run([]string{"-record", log}, strings.NewReader(input), io.Discard); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(root, "db")
	verify := func() error {
		return run([]string{"-verify", log, "-dir", dir}, nil, io.Discard)
	}
	if err := verify(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing database: %v", err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := verify(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("uninitialized database: %v", err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatalf("directory after verifying: %v, %v", entries, err)
	}

	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := verify(); err != nil {
		t.Fatalf("initialized database after the acknowledged Delete: %v", err)
	}
}

func TestExternalOracleAllowsOnlyWholePendingPut(t *testing.T) {
	for _, pending := range []string{"", "possible", "partial"} {
		t.Run(pending, func(t *testing.T) {
			root := t.TempDir()
			log := filepath.Join(root, "oracle.jsonl")
			var output bytes.Buffer
			input := "=== RUN guest\nTARP_ACK 6f7261636c65 61636b6e6f776c6564676564\nTARP_INTENT 70656e64696e67 706f737369626c65\nTARP_PHASE current.rename:after\n"
			if err := run([]string{"-record", log}, strings.NewReader(input), &output); err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(root, "db")
			db, err := tarp.Open(dir, tarp.Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Put([]byte("oracle"), []byte("acknowledged")); err != nil {
				t.Fatal(err)
			}
			if pending != "" {
				if err := db.Put([]byte("pending"), []byte(pending)); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			err = run([]string{"-verify", log, "-dir", dir}, nil, &output)
			if (err != nil) != (pending == "partial") {
				t.Fatalf("pending %q: %v", pending, err)
			}
			if err := run([]string{"-record", log}, strings.NewReader(input), &output); !os.IsExist(err) {
				t.Fatalf("oracle overwritten: %v", err)
			}
		})
	}
}

func TestOracleRejectsTruncationAndMalformedProtocol(t *testing.T) {
	if _, err := readOracle(strings.NewReader(`{"type":"ack"}`)); err == nil {
		t.Fatal("truncated final line accepted")
	}
	for _, input := range []string{"TARP_ACK 00", "TARP_ACK zz 01", "TARP_UNKNOWN 00 01"} {
		if _, _, err := protocol(input); err == nil {
			t.Fatalf("malformed protocol accepted: %s", input)
		}
	}
}

func TestExternalOracleChecksEntireLiveSet(t *testing.T) {
	dir := t.TempDir()
	db, err := tarp.Open(dir, tarp.Options{})
	if err != nil {
		t.Fatal(err)
	}
	err = db.BatchPut([]tarp.Entry{{Key: []byte("k"), Value: []byte("v")}, {Key: []byte("extra")}})
	if closeErr := db.Close(); err != nil || closeErr != nil {
		t.Fatalf("prepare database: %v, close: %v", err, closeErr)
	}

	base := entry{Type: "ack", Key: []byte("k"), Value: []byte("v")}
	extra := entry{Type: "ack", Key: []byte("extra")}
	pendingInsert := entry{Type: "intent", Key: []byte("new")}
	pendingDelete := entry{Type: "intent_delete", Key: []byte("removed")}
	removed := entry{Type: "ack", Key: pendingDelete.Key}
	for _, tc := range []struct {
		name    string
		history []entry
		wantErr bool
	}{
		{"acknowledged only", []entry{base}, true},
		{"pending insert absent", []entry{base, pendingInsert}, true},
		{"pending insert present", []entry{base, {Type: "intent", Key: extra.Key}}, false},
		{"pending update applied", []entry{base, {Type: "ack", Key: extra.Key, Value: []byte("old")}, {Type: "intent", Key: extra.Key}}, false},
		{"pending update unapplied", []entry{base, extra, {Type: "intent", Key: extra.Key, Value: []byte("new")}}, false},
		{"pending delete applied with extra key", []entry{base, removed, pendingDelete}, true},
		{"pending delete applied", []entry{base, extra, removed, pendingDelete}, false},
		{"pending delete unapplied", []entry{base, extra, {Type: "intent_delete", Key: extra.Key}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events := append(tc.history, entry{Type: "phase", Phase: "cut"})
			err := verifyDatabase(dir, events)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "live index cardinality") {
					t.Fatalf("expected a live key count mismatch, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
