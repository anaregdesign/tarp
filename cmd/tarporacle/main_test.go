package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anaregdesign/tarp"
)

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
