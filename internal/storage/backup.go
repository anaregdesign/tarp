package storage

import (
	"bufio"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/index"
	"github.com/anaregdesign/tarp/internal/platform"
	"github.com/anaregdesign/tarp/internal/publication"
)

func destinationPath(source, destination string) (string, error) {
	absolute, err := filepath.Abs(destination)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	absolute = filepath.Join(parent, filepath.Base(absolute))
	if _, err := os.Lstat(absolute); err == nil {
		return "", &os.PathError{Op: "publish", Path: absolute, Err: os.ErrExist}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	inside, err := directoryWithin(source, parent)
	if err != nil {
		return "", err
	}
	if inside {
		return "", fmt.Errorf("storage: destination must be outside source")
	}
	return absolute, nil
}

// Both paths exist and dir is absolute with symlinks resolved. Compare the
// actual directories because one directory can have multiple case spellings.
func directoryWithin(source, dir string) (bool, error) {
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return false, err
	}
	for {
		info, err := os.Stat(dir)
		if err != nil {
			return false, err
		}
		if os.SameFile(sourceInfo, info) {
			return true, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return false, nil
		}
		dir = parent
	}
}

func publishDirectory(ctx context.Context, fs *IO, stage, target, point string) error {
	if err := ctx.Err(); err != nil {
		return fault(point+".context", publication.NotApplied, false, err)
	}
	if err := fs.SyncDir(stage, point+".stage.sync"); err != nil {
		return fault(point+".stage.sync", publication.NotApplied, false, err)
	}
	if err := ctx.Err(); err != nil {
		return fault(point+".context", publication.NotApplied, false, err)
	}
	var renameErr error
	if err := fs.Do(point+".rename", func() error {
		renameErr = platform.RenameNoReplace(stage, target)
		return renameErr
	}); err != nil {
		outcome := publication.Unknown
		// Only EEXIST from the syscall proves that another destination won.
		// A hook can return the same error after a successful rename.
		if errors.Is(renameErr, syscall.EEXIST) {
			outcome = publication.NotApplied
		}
		return fault(point+".rename", outcome, false, err)
	}
	if err := fs.SyncDir(filepath.Dir(target), point+".parent.sync"); err != nil {
		return fault(point+".parent.sync", publication.Unknown, false, err)
	}
	return nil
}

func cleanStage(fs *IO, stage, point string) error {
	return fs.Do(point+".cleanup", func() error { return os.RemoveAll(stage) })
}

func Backup(ctx context.Context, capture *Capture, cfg Config, fs *IO, destination string, yield func() error) error {
	return BackupWithIndex(ctx, capture, cfg, fs, destination, yield, nil)
}

func BackupWithIndex(ctx context.Context, capture *Capture, cfg Config, fs *IO, destination string, yield func() error, newIndex func() (index.Backend, error)) (returned error) {
	target, err := destinationPath(filepath.Dir(capture.Files[0].File.Path), destination)
	if err != nil {
		return err
	}
	stage, err := os.MkdirTemp(filepath.Dir(target), ".tarp-backup-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			returned = errors.Join(returned, cleanStage(fs, stage, "backup"))
		}
	}()
	meta := format.Backup{UUID: capture.Current.UUID, LastUserSequence: capture.Current.LastUserSequence, CheckpointCutoffSequence: capture.Catalog.CheckpointCutoffSequence,
		SourceValueCacheBudgetBytes: cfg.ValueCacheBudgetBytes, SourceMaxRecordBytes: cfg.MaxRecordBytes}
	addBytes := func(name string, b []byte) error {
		if err := fs.NewFile(filepath.Join(stage, name), b, "backup.metadata"); err != nil {
			return err
		}
		meta.Files = append(meta.Files, format.BackupFile{Name: name, Bytes: int64(len(b)), Digest: sha256.Sum256(b)})
		return nil
	}
	if err := addBytes("CURRENT", format.EncodeFrontier(capture.Current)); err != nil {
		return err
	}
	if err := addBytes(ManifestName(capture.Current.UUID, capture.Current.ManifestID), capture.Manifest); err != nil {
		return err
	}
	buffer := make([]byte, 32<<10)
	stream := &backupStream{reader: bufio.NewReaderSize(nil, len(buffer)), source: backupReaderAt{fs: fs, yield: yield}}
	// Validate key transitions while streaming the captured records into the staged copy.
	var visit recordVisitor
	var backend index.Backend
	var builder replayBuilder
	var window *keyWindow
	if newIndex != nil {
		backend, err = newIndex()
		if err != nil {
			return &IndexWorkError{Err: err}
		}
		builder = newReplayBuilder(ctx, backend, capture.Catalog.PrunedThroughSequence)
		defer func() {
			if backend != nil {
				builder.abort()
				returned = errors.Join(returned, backend.Close())
			}
		}()
		visit = builder.apply
	} else {
		window = newKeyWindow("", verifyWindowBytes)
		visit = func(f *File, _ int64, r format.RecordHeader, key string) error {
			if err := window.apply(f.Kind, capture.Catalog.PrunedThroughSequence, r, key); err != nil {
				return &SourceError{Err: err}
			}
			return nil
		}
	}
	for _, captured := range capture.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := copySegment(ctx, capture, captured, visit, stage, fs, stream, buffer)
		if err != nil {
			return err
		}
		meta.Files = append(meta.Files, entry)
	}
	if backend != nil {
		if err := builder.finishValidation(); err != nil {
			return err
		}
		builder.abort()
		err = backend.Close()
		backend = nil
		if err != nil {
			return &IndexWorkError{Err: err}
		}
	} else if window.more {
		if _, err := validateReadOnly(ctx, stage, verifyWindowBytes); err != nil {
			return err
		}
	}
	if err := fs.NewFile(filepath.Join(stage, "BACKUP"), format.EncodeBackup(meta), "backup.manifest"); err != nil {
		return err
	}
	err = publishDirectory(ctx, fs, stage, target, "backup")
	published = err == nil
	return err
}

// backupReaderAt observes physical payload reads after the segment header.
type backupReaderAt struct {
	io.ReaderAt
	fs         *IO
	yield      func() error
	batchBytes int
	yieldErr   error
}

func (r *backupReaderAt) ReadAt(b []byte, off int64) (int, error) {
	if r.batchBytes >= 128<<10 {
		r.batchBytes = 0
		if r.yield != nil {
			if err := r.yield(); err != nil {
				r.yieldErr = err
				return 0, err
			}
		}
	}
	n, err := r.ReaderAt.ReadAt(b, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	if n > 0 {
		r.batchBytes += n
		if hookErr := r.fs.Check("backup.read:after"); hookErr != nil {
			return 0, hookErr
		}
	}
	return n, err
}

type backupStream struct {
	reader *bufio.Reader
	source backupReaderAt
}

func copySegment(ctx context.Context, capture *Capture, captured CapturedFile, visit recordVisitor, stage string, fs *IO, stream *backupStream, buffer []byte) (entry format.BackupFile, returned error) {
	src, header, _, err := openSelected(captured.File.Path, capture.Current.UUID, captured.Entry, captured.File.ID == capture.Current.ActiveID)
	if err != nil {
		return entry, selectedReadFailure(captured.Entry.ID, &SourceError{Err: err})
	}
	defer func() { returned = errors.Join(returned, src.Close()) }()
	name := DataName(capture.Current.UUID, captured.File.ID)
	dst, err := os.OpenFile(filepath.Join(stage, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return entry, err
	}
	defer func() { returned = errors.Join(returned, dst.Close()) }()
	h := sha256.New()
	writer := &outputWriter{fs: fs, fd: dst, hash: h, point: "backup.copy"}
	if _, err := writer.Write(header); err != nil {
		return entry, err
	}
	// Bound readahead to the captured prefix, including when a writer grows
	// the active file. Copy and hash each contiguous block once; publication
	// still requires successful validation of every record in those blocks.
	const start = int64(format.SegmentHeaderBytes)
	stream.source.ReaderAt = src
	source := io.NewSectionReader(&stream.source, start, captured.Entry.Bytes-start)
	stream.reader.Reset(io.TeeReader(source, writer))
	span := format.NewRecordSequenceValidator(captured.Entry, capture.Catalog.CheckpointCutoffSequence)
	for offset := start; offset < captured.Entry.Bytes; {
		if err := ctx.Err(); err != nil {
			return entry, err
		}
		r, key, err := format.ReadRecordKey(ctx, stream.reader, offset, captured.Entry.Bytes-offset, buffer)
		if err != nil {
			var destination *destinationError
			if stream.source.yieldErr == nil && !errors.As(err, &destination) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				err = selectedReadFailure(captured.Entry.ID, &SourceError{Err: fmt.Errorf("%s: record at offset %d: %w", name, offset, err)})
			}
			return entry, err
		}
		if err := span.Accept(r); err != nil {
			return entry, selectedReadFailure(captured.Entry.ID, &SourceError{Err: fmt.Errorf("%s: record at offset %d: %w", name, offset, err)})
		}
		if err := visit(captured.File, offset, r, key); err != nil {
			return entry, fmt.Errorf("%s: record at offset %d: %w", name, offset, err)
		}
		offset += r.EncodedRecordBytes()
	}
	if err := span.Finish(); err != nil {
		return entry, selectedReadFailure(captured.Entry.ID, &SourceError{Err: fmt.Errorf("%s: %w", name, err)})
	}
	if captured.File.ID != capture.Current.ActiveID {
		if err := checkSealedDigest(h, captured.Entry); err != nil {
			return entry, selectedReadFailure(captured.Entry.ID, err)
		}
	}
	if err := fs.Sync(dst, "backup.file.sync"); err != nil {
		return entry, err
	}
	entry = format.BackupFile{Name: name, Bytes: captured.Entry.Bytes}
	copy(entry.Digest[:], h.Sum(nil))
	return entry, nil
}

func backupSourcePath(ctx context.Context, source string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(source)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(absolute)
}

// Restore verifies the source and staged copy using independent replay state.
func Restore(ctx context.Context, source, destination string, cfg Config, fs *IO) (returned error) {
	source, err := backupSourcePath(ctx, source)
	if err != nil {
		return err
	}
	target, err := destinationPath(source, destination)
	if err != nil {
		return err
	}
	verify := restoreVerifier(filepath.Dir(target), verifyWindowBytes)
	buffer := make([]byte, 32<<10)
	backup, err := verifyBackup(ctx, source, buffer, verify)
	if err != nil {
		return err
	}
	if err := cfg.CheckLimits(backup.info.LiveRecordSizes); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(filepath.Dir(target), ".tarp-restore-")
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			returned = errors.Join(returned, cleanStage(fs, stage, "restore"))
		}
	}()
	for _, f := range backup.files {
		if err := copyBackupFile(ctx, source, stage, f, fs, buffer); err != nil {
			return fmt.Errorf("%s: %w", f.Name, err)
		}
	}
	image, err := validateBackupImage(ctx, stage, verify)
	if err != nil {
		return err
	}
	if err := cfg.CheckLimits(image.LiveRecordSizes); err != nil {
		return err
	}
	err = publishDirectory(ctx, fs, stage, target, "restore")
	published = err == nil
	return err
}

// BackupInfo describes a fully verified backup. Size minima come from live records.
type BackupInfo struct {
	DatabaseID                 [16]byte
	LastUserSequence, LiveKeys uint64
	LiveRecordSizes
}

type verifiedBackup struct {
	files []format.BackupFile
	info  BackupInfo
}

// VerifyBackup checks an immutable backup without creating a destination or acquiring a lock.
func VerifyBackup(ctx context.Context, dir string) (BackupInfo, error) {
	source, err := backupSourcePath(ctx, dir)
	if err != nil {
		return BackupInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return BackupInfo{}, err
	}
	backup, err := verifyBackup(ctx, source, make([]byte, 32<<10), nil)
	return backup.info, err
}

// VerifyBackupWithScratch places its temporary sort runs in an existing
// caller-selected directory outside the immutable backup.
func VerifyBackupWithScratch(ctx context.Context, dir, scratch string) (BackupInfo, error) {
	source, err := backupSourcePath(ctx, dir)
	if err != nil {
		return BackupInfo{}, err
	}
	if scratch == "" {
		return BackupInfo{}, fmt.Errorf("storage: empty verification scratch directory: %w", os.ErrInvalid)
	}
	scratch, err = backupSourcePath(ctx, scratch)
	if err != nil {
		return BackupInfo{}, err
	}
	inside, err := directoryWithin(source, scratch)
	if err != nil {
		return BackupInfo{}, err
	}
	if inside {
		return BackupInfo{}, fmt.Errorf("storage: verification scratch directory must be outside source: %w", os.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return BackupInfo{}, err
	}
	backup, err := verifyBackup(ctx, source, make([]byte, 32<<10), func(ctx context.Context, dir string) (*Image, error) {
		return verifySorted(ctx, dir, scratch, verifyWindowBytes)
	})
	return backup.info, err
}

type imageVerifier func(context.Context, string) (*Image, error)

// Only the choice of verifier carries over from the source to the stage.
// Each scratch replay starts from the staged or source files with a fresh workspace.
func restoreVerifier(scratch string, windowBytes int64) imageVerifier {
	var spilled bool
	withScratch := func(ctx context.Context, dir string) (*Image, error) {
		spilled = true
		return verifySorted(ctx, dir, scratch, windowBytes)
	}
	return func(ctx context.Context, dir string) (*Image, error) {
		if spilled {
			return withScratch(ctx, dir)
		}
		return validateWindows(ctx, dir, windowBytes, withScratch)
	}
}

func verifySorted(ctx context.Context, dir, scratch string, limit int64) (verified *Image, result error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Reserve the first sort run so even a small backup checks the supplied
	// directory. Larger backups use this file for their first spilled chunk.
	run, err := newReplayRun(scratch)
	if err != nil {
		return nil, &IndexWorkError{Err: err}
	}
	history := sortedReplay{ctx: ctx, dir: scratch, limit: limit, pending: run}
	defer func() {
		if err := history.close(); err != nil {
			verified = nil
			result = errors.Join(result, &IndexWorkError{Err: err})
		}
	}()
	image, err := readImage(ctx, dir)
	if err != nil {
		return nil, err
	}
	if err := image.scan(ctx, history.apply); err != nil {
		return nil, err
	}
	err = history.reduce(image.Catalog.PrunedThroughSequence, func(key string, addr index.Address) error {
		return image.measure(key, addr.Bytes, addr.ValueBytes)
	})
	if err != nil {
		return nil, replayWorkError(err)
	}
	return image, ctx.Err()
}

func verifyBackup(ctx context.Context, source string, buffer []byte, verify imageVerifier) (verifiedBackup, error) {
	b, err := ReadRegular(filepath.Join(source, "BACKUP"))
	if err != nil {
		return verifiedBackup{}, requiredFileError("BACKUP", err)
	}
	meta, err := format.DecodeBackup(b)
	if err != nil {
		return verifiedBackup{}, fmt.Errorf("BACKUP: %w", err)
	}
	files := make(map[string]struct{}, len(meta.Files))
	for _, f := range meta.Files {
		_, data := parseID(f.Name, "DATA", meta.UUID)
		_, manifest := parseID(f.Name, "MANIFEST", meta.UUID)
		if f.Name != "CURRENT" && !data && !manifest {
			return verifiedBackup{}, fmt.Errorf("BACKUP: %w: unsafe entry %q", ErrCorrupt, f.Name)
		}
		files[f.Name] = struct{}{}
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return verifiedBackup{}, err
	}
	for _, entry := range entries {
		if entry.Name() == "BACKUP" {
			continue
		}
		if _, listed := files[entry.Name()]; !listed {
			return verifiedBackup{}, fmt.Errorf("BACKUP: %w: unlisted file %q", ErrCorrupt, entry.Name())
		}
	}
	for _, f := range meta.Files {
		if err := verifyBackupFile(ctx, source, f, buffer); err != nil {
			return verifiedBackup{}, requiredFileError(f.Name, err)
		}
	}
	if len(entries) != len(files)+1 {
		return verifiedBackup{}, fmt.Errorf("%w: missing BACKUP", ErrCorrupt)
	}
	image, err := validateBackupImage(ctx, source, verify)
	if err != nil {
		return verifiedBackup{}, err
	}
	if image.Current.UUID != meta.UUID || image.Current.LastUserSequence != meta.LastUserSequence || image.Catalog.CheckpointCutoffSequence != meta.CheckpointCutoffSequence {
		return verifiedBackup{}, fmt.Errorf("BACKUP: %w: identity, sequence or cutoff differs from the selected image", ErrCorrupt)
	}
	if image.Files[image.Current.ActiveID].Size.Load() != image.Current.ActiveCommittedEndOffset {
		return verifiedBackup{}, fmt.Errorf("%s: %w: length differs from committed end", DataName(meta.UUID, image.Current.ActiveID), ErrCorrupt)
	}
	if len(files) != len(image.Files)+2 {
		return verifiedBackup{}, fmt.Errorf("BACKUP: %w: number of files differs from selected image", ErrCorrupt)
	}
	if _, ok := files["CURRENT"]; !ok {
		return verifiedBackup{}, fmt.Errorf("BACKUP: %w: missing CURRENT entry", ErrCorrupt)
	}
	name := ManifestName(image.Current.UUID, image.Current.ManifestID)
	if _, ok := files[name]; !ok {
		return verifiedBackup{}, fmt.Errorf("BACKUP: %w: missing %s entry", ErrCorrupt, name)
	}
	for id := range image.Files {
		name := DataName(meta.UUID, id)
		if _, ok := files[name]; !ok {
			return verifiedBackup{}, fmt.Errorf("BACKUP: %w: missing %s entry", ErrCorrupt, name)
		}
	}
	return verifiedBackup{
		files: meta.Files,
		info: BackupInfo{
			DatabaseID: image.Current.UUID, LastUserSequence: image.Current.LastUserSequence, LiveKeys: image.LiveKeys,
			LiveRecordSizes: image.LiveRecordSizes,
		},
	}, nil
}

// Once BACKUP is recognized, every selected object must have the canonical
// signature. An incompatible inner CURRENT is corruption of this backup.
func validateBackupImage(ctx context.Context, dir string, verify imageVerifier) (*Image, error) {
	var img *Image
	var err error
	if verify != nil {
		img, err = verify(ctx, dir)
	} else {
		img, err = validateReadOnly(ctx, dir, verifyWindowBytes)
	}
	if errors.Is(err, format.ErrUnsupported) {
		return nil, fmt.Errorf("%w: backup selected an incompatible CURRENT", ErrCorrupt)
	}
	return img, err
}

func verifyBackupFile(ctx context.Context, dir string, entry format.BackupFile, buffer []byte) (returned error) {
	fd, err := OpenRegular(filepath.Join(dir, entry.Name))
	if err != nil {
		return err
	}
	defer func() { returned = errors.Join(returned, fd.Close()) }()
	st, err := fd.Stat()
	if err != nil {
		return err
	}
	if st.Size() != entry.Bytes {
		return fmt.Errorf("%w: backup file length", ErrCorrupt)
	}
	h := sha256.New()
	for offset := int64(0); offset < entry.Bytes; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(int64(len(buffer)), entry.Bytes-offset)
		b := buffer[:int(n)]
		if _, err := fd.ReadAt(b, offset); err != nil {
			return err
		}
		if _, err := h.Write(b); err != nil {
			return err
		}
		offset += n
	}
	if actual := h.Sum(nil); string(actual) != string(entry.Digest[:]) {
		return fmt.Errorf("%w: backup digest", ErrCorrupt)
	}
	return nil
}

func copyBackupFile(ctx context.Context, source, stage string, entry format.BackupFile, fs *IO, buffer []byte) (returned error) {
	src, err := OpenRegular(filepath.Join(source, entry.Name))
	if err != nil {
		return err
	}
	defer func() { returned = errors.Join(returned, src.Close()) }()
	dst, err := os.OpenFile(filepath.Join(stage, entry.Name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer func() { returned = errors.Join(returned, dst.Close()) }()
	h := sha256.New()
	writer := &outputWriter{fs: fs, fd: dst, hash: h, point: "restore.copy"}
	for offset := int64(0); offset < entry.Bytes; {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := min(int64(len(buffer)), entry.Bytes-offset)
		b := buffer[:int(n)]
		if _, err := src.ReadAt(b, offset); err != nil {
			return err
		}
		if _, err := writer.Write(b); err != nil {
			return err
		}
		offset += n
	}
	if string(h.Sum(nil)) != string(entry.Digest[:]) {
		return fmt.Errorf("%w: backup changed while restoring", ErrCorrupt)
	}
	return fs.Sync(dst, "restore.file.sync")
}
