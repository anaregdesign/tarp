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
	"github.com/anaregdesign/tarp/benchmark/qualification"
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
	if _, err := qualification.ReadOracle(strings.NewReader(`{"type":"ack"}`)); err == nil {
		t.Fatal("truncated final line accepted")
	}
	for _, input := range []string{"TARP_ACK 00", "TARP_ACK zz 01", "TARP_UNKNOWN 00 01"} {
		if _, _, err := qualification.ParseOracle(input); err == nil {
			t.Fatalf("malformed protocol accepted: %s", input)
		}
	}
}

func TestExternalOracleChecksRecoveredSequenceAndLiveSet(t *testing.T) {
	type history []qualification.OracleEvent
	k, extra := []byte("k"), []byte("extra")
	base := qualification.OracleEvent{Type: "ack", Key: k, Value: []byte("v")}
	added := qualification.OracleEvent{Type: "ack", Key: extra}
	pending := qualification.OracleEvent{Type: "intent", Key: extra}
	removed := qualification.OracleEvent{Type: "intent_delete", Key: extra}
	old := qualification.OracleEvent{Type: "ack", Key: extra, Value: []byte("old")}
	for _, tc := range []struct {
		name      string
		witness   history
		writes    history
		wantError bool
	}{
		{"acknowledged only", history{base}, history{base}, false},
		{"unwitnessed identical write", history{base}, history{base, base}, true},
		{"unwitnessed key at expected sequence", history{base, base}, history{base, added}, true},
		{"pending insert unapplied", history{base, pending}, history{base}, false},
		{"pending insert applied", history{base, pending}, history{base, added}, false},
		{"pending update applied", history{base, old, pending}, history{base, old, added}, false},
		{"pending update unapplied", history{base, old, pending}, history{base, old}, false},
		{"pending value at prior sequence", history{base, old, pending}, history{base, added}, true},
		{"prior value at pending sequence", history{base, old, pending}, history{base, old, old}, true},
		{"pending delete applied", history{base, added, removed}, history{base, added, {Type: "ack_delete", Key: extra}}, false},
		{"absent delete unapplied", history{base, removed}, history{base}, false},
		{"pending delete unapplied", history{base, added, removed}, history{base, added}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			db, err := tarp.Open(dir, tarp.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, write := range tc.writes {
				if write.Type == "ack_delete" {
					err = db.Delete(write.Key)
				} else {
					err = db.Put(write.Key, write.Value)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			events := append(tc.witness, qualification.OracleEvent{Type: "phase", Phase: "cut"})
			if err := verifyDatabase(dir, events); (err != nil) != tc.wantError {
				t.Fatalf("verifyDatabase() = %v, want error %t", err, tc.wantError)
			}
		})
	}
}
