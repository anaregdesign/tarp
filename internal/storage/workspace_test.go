package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/publication"
)

func TestWorkspaceConcurrentUsersSharePrivateDirectory(t *testing.T) {
	s := &Store{Dir: t.TempDir()}
	want := filepath.Join(s.Dir, workspaceName)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			<-start
			dir, err := s.WorkspaceDir()
			if err != nil || dir != want {
				t.Errorf("WorkspaceDir returned %q, %v; want %q", dir, err, want)
			}
		})
	}
	close(start)
	wg.Wait()
	info, err := os.Lstat(want)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		t.Fatalf("workspace mode %v", info.Mode())
	}
}

func TestCleanWorkspaceRemovesInterruptedScratchNames(t *testing.T) {
	s := &Store{Dir: t.TempDir()}
	dir, err := s.WorkspaceDir()
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"tarp-index-", "tarp-replay-", "tarp-relocations-"} {
		f, err := os.CreateTemp(dir, prefix+"*")
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.cleanWorkspace(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace after cleanup: %v", err)
	}
	if err := s.cleanWorkspace(); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
}

func TestCleanWorkspaceValidatesEveryEntryBeforeRemoval(t *testing.T) {
	for _, tc := range []struct{ name, kind string }{
		{"notes", "file"}, {"tarp-index-", "file"}, {"tarp-index-12a", "file"},
		{"tarp-unknown-123", "file"}, {"tarp-replay-456", "dir"}, {"tarp-index-789", "symlink"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Store{Dir: t.TempDir()}
			dir, err := s.WorkspaceDir()
			if err != nil {
				t.Fatal(err)
			}
			owned := filepath.Join(dir, "tarp-index-123")
			if err := os.WriteFile(owned, nil, 0600); err != nil {
				t.Fatal(err)
			}
			object := filepath.Join(dir, tc.name)
			switch tc.kind {
			case "file":
				err = os.WriteFile(object, nil, 0600)
			case "dir":
				err = os.Mkdir(object, 0700)
			case "symlink":
				err = os.Symlink(owned, object)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := s.cleanWorkspace(); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("cleanWorkspace returned %v", err)
			}
			for _, path := range []string{owned, object} {
				if _, err := os.Lstat(path); err != nil {
					t.Fatalf("cleanup changed %s: %v", path, err)
				}
			}
		})
	}
}

func TestWorkspaceRejectsRootFileAndSymlink(t *testing.T) {
	for _, kind := range []string{"file", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			s := &Store{Dir: t.TempDir()}
			dir := filepath.Join(s.Dir, workspaceName)
			external := t.TempDir()
			other := filepath.Join(external, "tarp-index-123")
			if err := os.WriteFile(other, nil, 0600); err != nil {
				t.Fatal(err)
			}
			var err error
			if kind == "file" {
				err = os.WriteFile(dir, nil, 0600)
			} else {
				err = os.Symlink(external, dir)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.WorkspaceDir(); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("WorkspaceDir returned %v", err)
			}
			if err := s.cleanWorkspace(); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("cleanWorkspace returned %v", err)
			}
			for _, path := range []string{dir, other} {
				if _, err := os.Lstat(path); err != nil {
					t.Fatalf("cleanup changed %s: %v", path, err)
				}
			}
		})
	}
}

func TestIndexCreationFailureReleasesWorkspaceAndReportsInitialization(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing bool
		want     publication.Outcome
	}{{"fresh", false, publication.Committed}, {"existing", true, publication.NotApplied}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			cfg := Config{Handles: 1, RecordBytes: 256, TargetSegmentBytes: 1024, ValueCacheBudgetBytes: 1024}
			if tc.existing {
				s, _, err := Open(dir, cfg, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			s, backend, err := OpenIndexed(t.Context(), dir, cfg, tc.existing, nil, func(s *Store) (index.Backend, error) {
				if _, err := s.WorkspaceDir(); err != nil {
					return nil, err
				}
				return nil, syscall.ENOSPC
			})
			var failure *Fault
			if s != nil || backend != nil || !errors.Is(err, syscall.ENOSPC) || !errors.As(err, &failure) ||
				failure.Phase != "index" || failure.Outcome != tc.want {
				t.Fatalf("OpenIndexed returned %v, %v, %v; want index/%s with ENOSPC", s, backend, err, tc.want)
			}
			if _, err := os.Lstat(filepath.Join(dir, workspaceName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("workspace after failed OpenIndexed: %v", err)
			}
			s, _, err = OpenContext(t.Context(), dir, cfg, true, nil)
			if err != nil {
				t.Fatalf("reopen existing database: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
