package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestDestinationPathDistinguishesDescendantsFromSiblings(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "image")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		parent string
		inside bool
	}{
		{"descendant", filepath.Join(source, "child", "grandchild"), true},
		{"sibling", filepath.Join(root, "image-work", "child"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.MkdirAll(tc.parent, 0700); err != nil {
				t.Fatal(err)
			}
			_, err := destinationPath(source, filepath.Join(tc.parent, "output"))
			if (err != nil) != tc.inside {
				t.Fatalf("inside = %v, destination error = %v", tc.inside, err)
			}
		})
	}
}

func TestRestoreVerifierCarriesOnlySourceSpillDecisionToStage(t *testing.T) {
	cfg := Config{Handles: 1, TargetSegmentBytes: 1 << 20}
	healthy := func() error { return nil }
	source := t.TempDir()
	store, _, err := Open(source, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	puts := []Entry{{Key: []byte("a"), Value: []byte("old")}, {Key: []byte("b"), Value: []byte("longest value")}}
	if _, err := store.AppendBatch(format.Put, puts, healthy); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(format.Delete, puts[1].Key, nil, healthy); err != nil {
		t.Fatal(err)
	}
	want, err := Validate(t.Context(), source)
	if err != nil {
		t.Fatal(err)
	}
	var workErr *IndexWorkError
	if _, err := verifySorted(t.Context(), source, filepath.Join(source, "CURRENT"), verifyWindowBytes); !errors.As(err, &workErr) {
		t.Fatalf("small image accepted unusable scratch: %v", err)
	}

	stage := t.TempDir()
	staged, _, err := Open(stage, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	// This validly encoded record deletes a key absent from the staged history.
	if _, err := staged.Append(format.Delete, []byte("absent"), nil, healthy); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		limit int64
		spill bool
	}{{"fits", verifyWindowBytes, false}, {"overflow", replayKeyCharge, true}} {
		t.Run(tc.name, func(t *testing.T) {
			scratch := t.TempDir()
			verify := restoreVerifier(scratch, tc.limit)
			image, err := verify(t.Context(), source)
			if err != nil {
				t.Fatal(err)
			}
			if image.LiveKeys != want.LiveKeys || image.Required != want.Required || image.MaxValueBytes != want.MaxValueBytes {
				t.Fatalf("source: live=%d, limits=%+v maxValue=%d", image.LiveKeys, image.Required, image.MaxValueBytes)
			}
			if _, err := verify(t.Context(), stage); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("invalid staged history: %v", err)
			}
			// A fresh workspace must open before scanning even a one-record stage
			// when the source overflowed. A small source never needs scratch.
			if err := os.Remove(scratch); err != nil {
				t.Fatal(err)
			}
			wantErr := ErrCorrupt
			if tc.spill {
				wantErr = os.ErrNotExist
			}
			if _, err := verify(t.Context(), stage); !errors.Is(err, wantErr) {
				t.Fatalf("stage after scratch removal: got %v, want %v", err, wantErr)
			}
		})
	}
}
