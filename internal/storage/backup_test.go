package storage

import (
	"os"
	"path/filepath"
	"testing"
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
