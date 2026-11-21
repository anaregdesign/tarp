package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
)

func TestMaintenanceRetryRequiresOneLocalizedFailure(t *testing.T) {
	const fileID = 4
	read := selectedReadFailure(fileID, fmt.Errorf("%w: %w", ErrCorrupt, io.ErrUnexpectedEOF))
	if got := failedDataRead(errors.Join(fmt.Errorf("copy: %w", errors.Join(read)))); got == nil || got.id != fileID {
		t.Fatalf("lost wrapped DATA failure: %v", got)
	}
	for _, err := range []error{
		selectedReadFailure(0, ErrCorrupt),
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
