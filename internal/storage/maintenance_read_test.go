package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/format"
)

func TestMaintenanceRetryRequiresOneLocalizedFailure(t *testing.T) {
	sealed := format.File{ID: 4, Digest: [32]byte{1}}
	read := selectedReadFailure(sealed, fmt.Errorf("%w: %w", ErrCorrupt, io.ErrUnexpectedEOF))
	if got := failedDataRead(errors.Join(fmt.Errorf("copy: %w", errors.Join(read)))); got == nil || got.id != sealed.ID {
		t.Fatalf("lost wrapped DATA failure: %v", got)
	}
	for _, err := range []error{
		selectedReadFailure(format.File{}, ErrCorrupt),
		&destinationError{syscall.EIO},
		&IndexWorkError{Err: syscall.EIO},
		errors.Join(read, syscall.EACCES),
		fmt.Errorf("cleanup: %w", errors.Join(read, context.DeadlineExceeded)),
	} {
		if got := failedDataRead(err); got != nil {
			t.Fatalf("mistook %v for repairable DATA", err)
		}
	}
}
