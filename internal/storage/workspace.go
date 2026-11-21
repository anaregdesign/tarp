package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const workspaceName = ".tarp-work"

// WorkspaceDir provides a private directory for scratch belonging to this
// open database. Backup and compaction may request it concurrently.
func (s *Store) WorkspaceDir() (string, error) {
	dir := filepath.Join(s.Dir, workspaceName)
	if err := os.Mkdir(dir, 0700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
		if err := checkWorkspaceDir(dir); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func checkWorkspaceDir(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: workspace %s is not a directory", ErrCorrupt, dir)
	}
	return nil
}

// cleanWorkspace runs under the DB lock with no active scratch users. A
// process can exit between creating a scratch file and unlinking its name.
func (s *Store) cleanWorkspace() error {
	return s.sweepWorkspace(true)
}

func (s *Store) sweepWorkspace(removeDir bool) error {
	dir := filepath.Join(s.Dir, workspaceName)
	if err := checkWorkspaceDir(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !workspaceFileName(entry.Name()) {
			return fmt.Errorf("%w: unexpected workspace object %s", ErrCorrupt, filepath.Join(dir, entry.Name()))
		}
	}
	for _, entry := range entries {
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	if removeDir {
		return os.Remove(dir)
	}
	return nil
}

func workspaceFileName(name string) bool {
	sep := strings.LastIndexByte(name, '-')
	if sep < 0 || sep == len(name)-1 {
		return false
	}
	switch name[:sep] {
	case "tarp-index", "tarp-replay", "tarp-relocations", "tarp-parity", "tarp-repair":
	default:
		return false
	}
	for _, digit := range name[sep+1:] {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}
