package benchmark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
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

func selectedDataIDs(s *storage.Store) map[uint64]bool {
	selected := map[uint64]bool{s.Current.ActiveID: true}
	for _, entry := range s.Catalog.Entries {
		selected[entry.ID] = true
	}
	return selected
}

func retiredPinnedBytes(s *storage.Store) int64 {
	selected := selectedDataIDs(s)
	var total int64
	for id, segment := range s.Segments {
		if !selected[id] && segment.References() > 0 {
			total += segment.Size.Load()
		}
	}
	return total
}

// The backup is still parked on its first DATA write. The checkpoint has already
// returned and the cleaner is parked, so the backup owns the only retention
// pins. Ordinary references (including PARITY pins) do not prove retention.
func witnessBackupPins(probe engine.Observer, capturedIDs []uint64) (map[uint64]int64, error) {
	var err error
	files := make(map[uint64]int64)
	probe.Store(func(s *storage.Store) {
		selected := selectedDataIDs(s)
		for _, id := range capturedIDs {
			if selected[id] {
				continue
			}
			f := s.Segments[id]
			if f == nil {
				err = fmt.Errorf("retired captured DATA %d vanished before backup release", id)
				return
			}
			if !f.HasRetentionPin() {
				err = fmt.Errorf("retired captured DATA %d has no retention pin", id)
				return
			}
			files[id] = f.Size.Load()
		}
	})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("checkpoint did not retire any captured DATA files")
	}
	return files, nil
}

// Run only after the foreground, backup, restore and measured timing have
// finished. Check the same retired file IDs on disk on either side of release
// of the parked automatic cleaner, before reopening the source could remove them.
func verifyBackupReclamation(ctx context.Context, probe engine.Observer, hooks *measurementHooks, backup string, files map[uint64]int64) (p pinnedBackupMeasurement, err error) {
	probe.Store(func(s *storage.Store) {
		for id := range files {
			f := s.Segments[id]
			if f == nil {
				err = fmt.Errorf("retired DATA %d vanished before cleaner release", id)
				return
			}
			if f.HasRetentionPin() {
				err = fmt.Errorf("completed backup left a retention pin on retired DATA %d", id)
				return
			}
		}
	})
	if err != nil {
		return p, err
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
	// Ordinary references may outlive the backup. Wait for these exact paths to
	// disappear at a successful retirement directory sync, across any batches.
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

func TestMeasurementBackupPinWitness(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name, parkedScan := "released-unrelated-parity-pin", 2
		if missing {
			name, parkedScan = "parity-does-not-mask-missing-backup-pin", 1
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			keys, value, opts := measurementInput(64, 2)
			opts = measurementWriteOptions(opts, len(keys[0]), len(value), true)
			hooks := &measurementHooks{}
			fs, engineHooks := hooks.interferenceHooks()
			engineHooks.ParityStart = make(chan struct{}) // Only explicit ProtectParity uses the hook.
			parityStarted, parityResume := make(chan struct{}), make(chan struct{})
			releaseParity := sync.OnceFunc(func() { close(parityResume) })
			scans := 0
			fs.Hook = func(point string) error {
				if point == "parity.scan:before" {
					scans++
					if scans == parkedScan {
						close(parityStarted)
						<-parityResume
					}
				}
				return hooks.hook(point)
			}
			db := newTestDBWithHooks(t, opts, fs, engineHooks)
			probe := engine.Observe(db)
			mustPut(t, db, string(keys[0]), string(value))
			digest, count := measurementState(t, db)
			cut := probe.Frontier() // Only this active DATA exists at capture.
			root := t.TempDir()
			target := filepath.Join(root, "backup")
			captured := &measurementCapture{parent: root, uuid: cut.UUID}
			copyStarted := make(chan struct{})
			hooks.capture.Store(captured)
			hooks.started.Store(&copyStarted)
			hooks.overlap.Store(&measurementOverlap{resume: make(chan struct{}), untilCompactReturns: true})
			backupDone := make(chan struct{})
			var backupErr error
			go func() {
				defer close(backupDone)
				backupErr = db.Backup(ctx, target)
			}()
			finishBackup := func() {
				hooks.releaseOverlap()
				<-backupDone
			}
			defer finishBackup()
			select {
			case <-copyStarted:
			case <-backupDone:
				t.Fatalf("Backup returned before capture gate: %v", backupErr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			capturedIDs, err := captured.dataIDs()
			if err != nil || len(capturedIDs) != 1 || capturedIDs[0] != cut.ActiveID {
				t.Fatalf("captured DATA %v, want just %d: %v", capturedIDs, cut.ActiveID, err)
			}
			mustPut(t, db, string(keys[1]), string(value))
			later := probe.Frontier()
			if later.ActiveID == cut.ActiveID {
				t.Fatal("post-capture Put did not start new DATA")
			}
			parityDone := make(chan struct{})
			var parityErr error
			go func() {
				defer close(parityDone)
				parityErr = db.ProtectParity(ctx) // Seals later DATA, then scans in catalog order.
			}()
			finishParity := func() {
				releaseParity()
				<-parityDone
			}
			defer finishParity()
			select {
			case <-parityStarted:
			case <-parityDone:
				t.Fatalf("ProtectParity returned before scan gate: %v", parityErr)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			result, err := db.Compact(ctx)
			if err != nil {
				t.Fatal(err)
			}
			wantBytes := cut.ActiveCommittedEndOffset
			wantCompactBytes := wantBytes
			parityID, wantReferences := cut.ActiveID, int64(1)
			if missing {
				finishBackup()
			} else {
				wantCompactBytes += later.ActiveCommittedEndOffset
				parityID, wantReferences = later.ActiveID, 0
				finishParity()
			}
			if result.PinnedRetiredDataBytes != wantCompactBytes {
				t.Fatalf("Compact reported %d pinned bytes, want %d", result.PinnedRetiredDataBytes, wantCompactBytes)
			}
			probe.Store(func(s *storage.Store) {
				f := s.Segments[parityID]
				if f == nil || f.References() != wantReferences || f.HasRetentionPin() {
					t.Fatalf("DATA %d should have only %d ordinary PARITY references at witness time", parityID, wantReferences)
				}
			})
			files, witnessErr := witnessBackupPins(probe, capturedIDs)
			if missing {
				if witnessErr == nil {
					t.Errorf("PARITY masked a missing backup pin: accepted %v", files)
				} else {
					t.Logf("missing backup pin rejected: %v", witnessErr)
				}
			} else if witnessErr != nil || len(files) != 1 || files[cut.ActiveID] != wantBytes {
				t.Errorf("correct parked backup rejected: captured DATA %d (%d bytes), files %v, error %v", cut.ActiveID, wantBytes, files, witnessErr)
			}

			finishBackup()
			if backupErr != nil {
				t.Fatal(backupErr)
			}
			restoredPath := filepath.Join(root, "restored")
			if err := engine.Restore(ctx, engine.RestoreRequest{
				Source: target, Destination: restoredPath, Limits: probe.Options().RestoreLimits(),
			}); err != nil {
				t.Fatal(err)
			}
			restored, err := engine.Open(restoredPath, probe.Options())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := restored.Close(); err != nil {
					t.Error(err)
				}
			})
			if sum, n := measurementState(t, restored); sum != digest || n != count ||
				engine.Observe(restored).Frontier().LastUserSequence != cut.LastUserSequence {
				t.Fatalf("restore did not match the capture: digest %s/%s, keys %d/%d", sum, digest, n, count)
			}
			if missing {
				// Keep the ordinary pin through the backup-release check, but not
				// through reclamation.
				go func() {
					select {
					case <-hooks.cleanerStart:
						releaseParity()
					case <-parityDone:
					}
				}()
			}
			// Verify this independently of the witness result, even on a false rejection.
			pins, err := verifyBackupReclamation(ctx, probe, hooks, target, map[uint64]int64{cut.ActiveID: wantBytes})
			if err != nil || pins.RetiredBytesHeld != wantBytes || pins.ReleasedBytesUnlinked != wantBytes ||
				pins.RetiredFilesHeld != 1 || pins.ReleasedFilesUnlinked != 1 {
				t.Fatalf("failed to verify captured DATA reclamation: %+v: %v", pins, err)
			}
			finishParity()
			if parityErr != nil {
				t.Fatal(parityErr)
			}
			t.Logf("Compact=%d bytes; observed PARITY DATA %d with %d refs; restored capture sequence %d and reclaimed DATA %d (%d bytes)",
				result.PinnedRetiredDataBytes, parityID, wantReferences, cut.LastUserSequence, cut.ActiveID, wantBytes)
		})
	}
}
