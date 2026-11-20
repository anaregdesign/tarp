package platform

import (
	"errors"
	"os"
	"testing"
)

func TestOpenScratchRequiresDirectory(t *testing.T) {
	if f, err := OpenScratch("", "tarp-index-*"); err == nil {
		f.Close()
		t.Fatal("opened scratch without a directory")
	} else if !errors.Is(err, os.ErrInvalid) {
		t.Fatal(err)
	}
}
