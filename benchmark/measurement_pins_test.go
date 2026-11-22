package benchmark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/anaregdesign/tarp/internal/engine"
	"github.com/anaregdesign/tarp/internal/storage"
)

const pinnedBackupMode = "pinned-backup-compact"

type pinnedBackupMeasurement struct {
	BackupGateWaitNS      int64 `json:"backup_gate_wait_ns"`
	RetiredFilesHeld      int   `json:"retired_files_held"`
	RetiredBytesHeld      int64 `json:"retired_bytes_held"`
	ReleasedFilesUnlinked int   `json:"released_files_unlinked"`
	ReleasedBytesUnlinked int64 `json:"released_bytes_unlinked"`
	WorkerReclaimNS       int64 `json:"worker_reclaim_ns"`
}

func retiredPinnedBytes(s *storage.Store, files map[uint64]int64) int64 {
	selected := map[uint64]bool{s.Current.ActiveID: true}
	for _, file := range s.Catalog.Entries {
		selected[file.ID] = true
	}
	var total int64
	for id, file := range s.Files {
		if !selected[id] && file.References() > 0 {
			size := file.Size.Load()
			total += size
			if files != nil {
				files[id] = size
			}
		}
	}
	return total
}

// The backup is still parked on its first DATA write. The checkpoint has already
// returned after publishing, remapping and trying to clean up its old sources.
func witnessBackupPins(probe engine.Observer, expected int64) (map[uint64]int64, error) {
	files := make(map[uint64]int64)
	var held int64
	probe.Store(func(s *storage.Store) { held = retiredPinnedBytes(s, files) })
	if len(files) == 0 || held != expected {
		return nil, fmt.Errorf("retired files %d hold %d bytes after checkpoint, Compact reported %d", len(files), held, expected)
	}
	return files, nil
}

// Run only after the foreground, backup, restore and measured timing have
// finished. Check the same retired file IDs on disk on either side of release
// of the parked automatic cleaner, before reopening the source could remove them.
func verifyBackupReclamation(ctx context.Context, probe engine.Observer, hooks *measurementHooks, backup string, files map[uint64]int64) (p pinnedBackupMeasurement, err error) {
	var references int64
	probe.Store(func(s *storage.Store) {
		for id := range files {
			if f := s.Files[id]; f != nil {
				references += f.References()
			} else {
				err = fmt.Errorf("retired DATA %d vanished before cleaner release", id)
			}
		}
	})
	if err != nil {
		return p, err
	}
	if references != 0 {
		return p, fmt.Errorf("completed backup left %d retired file references", references)
	}
	uuid := probe.Frontier().UUID
	for id, length := range files {
		name := storage.DataName(uuid, id)
		// A captured active file can contain further appends by the time
		// it is sealed. Restore has verified the captured prefix and digest.
		if _, err := os.Stat(filepath.Join(backup, name)); err != nil {
			return p, fmt.Errorf("retired DATA missing from validated backup: %w", err)
		}
		info, err := os.Stat(filepath.Join(probe.Dir(), name))
		if err != nil {
			return p, err
		}
		if info.Size() != length {
			return p, fmt.Errorf("retired DATA %d has %d bytes, want %d", id, info.Size(), length)
		}
		p.RetiredFilesHeld++
		p.RetiredBytesHeld += length
	}
	// Check inside the successful directory-sync hook while the cleaner still
	// holds the writer gate. A group can span several retirement batches.
	reclaimed := make(chan error, 1)
	hooks.afterRetirement = func() {
		var observation error
		for id := range files {
			_, err := os.Stat(filepath.Join(probe.Dir(), storage.DataName(uuid, id)))
			if err == nil {
				return
			}
			if !errors.Is(err, os.ErrNotExist) {
				observation = err
				break
			}
		}
		select {
		case reclaimed <- observation:
		default:
		}
	}
	start := time.Now()
	close(hooks.cleanerStart)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	select {
	case <-ctx.Done():
		return p, fmt.Errorf("waiting for retirement of %d DATA files: %w", len(files), ctx.Err())
	case <-probe.CleanerDone():
		return p, errors.New("cleaner stopped before retiring pinned backup sources")
	case err := <-reclaimed:
		if err != nil {
			return p, err
		}
	}
	p.WorkerReclaimNS = time.Since(start).Nanoseconds()
	p.ReleasedFilesUnlinked = len(files)
	for _, size := range files {
		p.ReleasedBytesUnlinked += size
	}
	return p, nil
}
