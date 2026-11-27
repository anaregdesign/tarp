package storage

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/publication"
)

const (
	incrementalObjectsDir = ".tarp-incremental-objects"
	incrementalStageDir   = ".tarp-incremental-stage"
	incrementalPoint      = "incremental-backup"
)

type IncrementalBackupRequest struct {
	Repository       string
	Name             string
	VerifyReusedData bool
}

// IncrementalBackupResult accounts for the captured DATA image. Metadata bytes
// and reads used to check already stored DATA are excluded.
type IncrementalBackupResult struct {
	LastUserSequence uint64
	CopiedDataBytes  uint64
	ReusedDataBytes  uint64
}

// IncrementalBackup publishes a native backup directory on a filesystem with
// hardlink support. Sealed DATA objects are immutable and shared by generations.
func IncrementalBackup(ctx context.Context, capture *Capture, cfg Config, fs *IO, req IncrementalBackupRequest, yield func() error) (result IncrementalBackupResult, returned error) {
	name := req.Name
	if !filepath.IsLocal(name) || name != filepath.Base(name) || strings.HasPrefix(name, ".") || strings.EqualFold(name, "LOCK") {
		return result, errors.New("storage: incremental backup name must be a single nonreserved path component")
	}
	repository, err := existingDirectoryPath(ctx, req.Repository)
	if err != nil {
		return result, err
	}
	source := filepath.Dir(capture.Segments[0].Segment.Path)
	target, err := destinationPath(source, filepath.Join(repository, name))
	if err != nil {
		return result, err
	}
	inside, err := directoryWithin(repository, source)
	if err != nil {
		return result, err
	}
	if inside {
		return result, errors.New("storage: incremental backup repository must not contain the source database")
	}
	lock, err := platform.Lock(repository)
	if err != nil {
		return result, err
	}
	defer func() {
		if err := lock.Close(); err != nil {
			if returned == nil {
				returned = fault(incrementalPoint+".unlock", publication.Committed, false, err)
				return
			}
			returned = errors.Join(returned, err)
		}
	}()
	objects := filepath.Join(repository, incrementalObjectsDir)
	if err := ensureBackupDirectory(objects); err != nil {
		return result, err
	}
	stage := filepath.Join(repository, incrementalStageDir)
	if err := discardIncrementalStage(stage, fs); err != nil {
		return result, err
	}
	if err := os.Mkdir(stage, 0700); err != nil {
		return result, err
	}
	published := false
	defer func() {
		if !published {
			returned = errors.Join(returned, cleanStage(fs, stage, incrementalPoint))
		}
	}()
	meta, err := writeBackupMetadata(capture, cfg, fs, stage, incrementalPoint+".metadata")
	if err != nil {
		return result, err
	}
	buffer := make([]byte, 32<<10)
	staging := incrementalStage{
		capture: capture, fs: fs, dir: stage, objects: objects, verifyReused: req.VerifyReusedData,
		buffer: buffer, stream: backupStream{reader: bufio.NewReaderSize(nil, len(buffer)), source: backupReaderAt{fs: fs, yield: yield}},
		buckets: make(map[string]struct{}),
	}
	for _, captured := range capture.Segments {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		entry, reused, err := staging.stageDATA(ctx, captured)
		if err != nil {
			return result, err
		}
		meta.Files = append(meta.Files, entry)
		if reused {
			result.ReusedDataBytes += uint64(entry.Bytes)
		} else {
			result.CopiedDataBytes += uint64(entry.Bytes)
		}
	}
	for bucket := range staging.buckets {
		if err := fs.SyncDir(bucket, incrementalPoint+".bucket.sync"); err != nil {
			return result, err
		}
	}
	if err := fs.SyncDir(objects, incrementalPoint+".objects.sync"); err != nil {
		return result, err
	}
	if err := fs.NewFile(filepath.Join(stage, "BACKUP"), format.EncodeBackup(meta), incrementalPoint+".manifest"); err != nil {
		return result, err
	}
	if err := publishDirectory(ctx, fs, stage, target, incrementalPoint); err != nil {
		return result, err
	}
	published = true
	result.LastUserSequence = capture.Current.LastUserSequence
	return result, nil
}

type incrementalStage struct {
	capture *Capture
	fs      *IO
	dir     string
	objects string
	buffer  []byte
	stream  backupStream
	buckets map[string]struct{}

	verifyReused bool
}

func (s *incrementalStage) stageDATA(ctx context.Context, captured CapturedSegment) (format.BackupFile, bool, error) {
	if captured.Entry.ID == s.capture.Current.ActiveID {
		entry, err := s.copySegment(ctx, captured)
		return entry, false, err
	}
	entry := format.BackupFile{Name: DataName(s.capture.Current.UUID, captured.Entry.ID), Bytes: captured.Entry.Bytes, Digest: captured.Entry.DataFileDigest}
	object := fmt.Sprintf("%x-%d", entry.Digest, entry.Bytes)
	bucket := filepath.Join(s.objects, object[:2])
	if _, ok := s.buckets[bucket]; !ok {
		if err := ensureBackupDirectory(bucket); err != nil {
			return entry, false, err
		}
		s.buckets[bucket] = struct{}{}
	}
	path := filepath.Join(bucket, object)
	staged := filepath.Join(s.dir, entry.Name)
	stored := entry
	stored.Name = object
	if err := s.checkObject(ctx, bucket, stored); err == nil {
		return entry, true, s.fs.Do(incrementalPoint+".object.reuse", func() error { return os.Link(path, staged) })
	} else if !errors.Is(err, os.ErrNotExist) {
		return entry, false, fmt.Errorf("%s: %w", object, err)
	}
	if _, err := s.copySegment(ctx, captured); err != nil {
		return entry, false, err
	}
	return entry, false, s.fs.Do(incrementalPoint+".object.add", func() error { return os.Link(staged, path) })
}

func (s *incrementalStage) copySegment(ctx context.Context, captured CapturedSegment) (format.BackupFile, error) {
	entry, err := copySegment(ctx, s.capture, captured, nil, s.dir, s.fs, &s.stream, s.buffer)
	if err != nil {
		return entry, err
	}
	// Only objects verified and synced as complete files enter the immutable
	// pool. A later full verification is available for latent media damage.
	return entry, verifyBackupFile(ctx, s.dir, entry, s.buffer)
}

func (s *incrementalStage) checkObject(ctx context.Context, dir string, entry format.BackupFile) error {
	if s.verifyReused {
		return verifyBackupFile(ctx, dir, entry, s.buffer)
	}
	info, err := os.Lstat(filepath.Join(dir, entry.Name))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != entry.Bytes {
		return fmt.Errorf("%w: incremental backup object type or length", ErrCorrupt)
	}
	return nil
}

func ensureBackupDirectory(path string) error {
	if err := os.Mkdir(path, 0700); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) {
		return err
	}
	return checkBackupDirectory(path)
}

func checkBackupDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: incremental backup path is not a directory: %s", ErrCorrupt, path)
	}
	return nil
}

// Operations hold the repository lock, so a private stage left by another
// operation belongs to an interrupted attempt. Check its names before removal.
func discardIncrementalStage(stage string, fs *IO) error {
	if err := checkBackupDirectory(stage); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	entries, err := os.ReadDir(stage)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !backupStagingName(entry.Name()) {
			return fmt.Errorf("%w: unexpected incremental backup staging file %s", ErrCorrupt, entry.Name())
		}
	}
	return cleanStage(fs, stage, incrementalPoint)
}

func backupStagingName(name string) bool {
	if name == "CURRENT" || name == "BACKUP" {
		return true
	}
	kind, rest, _ := strings.Cut(name, "-")
	if kind != "DATA" && kind != "MANIFEST" || len(rest) < 33 || !lowerHex(rest[:32]) || rest[32] != '-' {
		return false
	}
	_, ok := parseObjectID(rest, rest[:33])
	return ok
}

// PruneIncrementalBackupObjects reclaims cached objects once the last
// published generation that links to them has been removed.
func PruneIncrementalBackupObjects(ctx context.Context, repository string, fs *IO) (returned error) {
	repository, err := existingDirectoryPath(ctx, repository)
	if err != nil {
		return err
	}
	objects := filepath.Join(repository, incrementalObjectsDir)
	if err := checkBackupDirectory(objects); err != nil {
		return err
	}
	lock, err := platform.LockExisting(repository)
	if err != nil {
		return err
	}
	defer func() { returned = errors.Join(returned, lock.Close()) }()
	if err := discardIncrementalStage(filepath.Join(repository, incrementalStageDir), fs); err != nil {
		return err
	}
	buckets, err := os.ReadDir(objects)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		if len(bucket.Name()) != 2 || !lowerHex(bucket.Name()) || !bucket.IsDir() {
			return fmt.Errorf("%w: unexpected incremental backup object directory %s", ErrCorrupt, bucket.Name())
		}
		if err := pruneIncrementalBackupBucket(ctx, filepath.Join(objects, bucket.Name()), fs); err != nil {
			return err
		}
	}
	return fs.SyncDir(repository, incrementalPoint+".prune.parent.sync")
}

func pruneIncrementalBackupBucket(ctx context.Context, bucket string, fs *IO) error {
	entries, err := os.ReadDir(bucket)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, fs.SyncDir(bucket, incrementalPoint+".prune.sync"))
		}
		digest, sizeText, _ := strings.Cut(entry.Name(), "-")
		size, err := strconv.ParseInt(sizeText, 10, 64)
		if !entry.Type().IsRegular() || len(digest) != 64 || !strings.HasPrefix(digest, filepath.Base(bucket)) || !lowerHex(digest) || err != nil || size < format.SegmentHeaderBytes || strconv.FormatInt(size, 10) != sizeText {
			return fmt.Errorf("%w: unexpected incremental backup object %s", ErrCorrupt, entry.Name())
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		links, err := platform.FileLinkCount(info)
		if err != nil {
			return err
		}
		if links == 1 {
			if err := os.Remove(filepath.Join(bucket, entry.Name())); err != nil {
				return err
			}
		}
	}
	return fs.SyncDir(bucket, incrementalPoint+".prune.sync")
}

func lowerHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil && strings.ToLower(s) == s
}
