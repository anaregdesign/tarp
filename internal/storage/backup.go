package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/anaregdesign/tarp/internal/format"
	"github.com/anaregdesign/tarp/internal/platform"
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
	relative, err := filepath.Rel(source, absolute)
	if err != nil {
		return "", err
	}
	if relative == "." || relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("storage: destination must be outside source")
	}
	return absolute, nil
}

func publishDirectory(ctx context.Context, fs *IO, stage, target, point string) error {
	if err := ctx.Err(); err != nil {
		return fault(point+".context", "not-applied", false, err)
	}
	if err := fs.SyncDir(stage, point+".stage.sync"); err != nil {
		return fault(point+".stage.sync", "not-applied", false, err)
	}
	if err := ctx.Err(); err != nil {
		return fault(point+".context", "not-applied", false, err)
	}
	if err := fs.Do(point+".rename", func() error { return platform.RenameNoReplace(stage, target) }); err != nil {
		return fault(point+".rename", "unknown", false, err)
	}
	if err := fs.SyncDir(filepath.Dir(target), point+".parent.sync"); err != nil {
		return fault(point+".parent.sync", "unknown", false, err)
	}
	return nil
}

func cleanStage(fs *IO, stage, point string) error {
	return fs.Do(point+".cleanup", func() error { return os.RemoveAll(stage) })
}

func Backup(ctx context.Context, capture *Capture, cfg Config, fs *IO, destination string) (returned error) {
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
	meta := format.Backup{UUID: capture.Current.UUID, Sequence: capture.Current.Sequence, Cutoff: capture.Catalog.Cutoff,
		SourceCacheBytes: cfg.CacheBytes, SourceRecordBytes: cfg.RecordBytes, SourceFileBytes: cfg.FileBytes}
	addBytes := func(name string, b []byte) error {
		if err := fs.NewFile(filepath.Join(stage, name), b, "backup.metadata"); err != nil {
			return err
		}
		meta.Files = append(meta.Files, format.BackupFile{Name: name, Bytes: int64(len(b)), Hash: sha256.Sum256(b)})
		return nil
	}
	if err := addBytes("CURRENT", format.EncodeFrontier(capture.Current)); err != nil {
		return err
	}
	if err := addBytes(ManifestName(capture.Current.UUID, capture.Current.ManifestID), capture.Manifest); err != nil {
		return err
	}
	buffer := make([]byte, 32<<10)
	last := capture.Catalog.Cutoff
	for _, captured := range capture.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := copySegment(ctx, captured, capture.Current.UUID, captured.File.ID == capture.Current.ActiveID, capture.Catalog.Cutoff, &last, stage, fs, buffer)
		if err != nil {
			return err
		}
		meta.Files = append(meta.Files, entry)
	}
	if last != capture.Current.Sequence {
		return &SourceError{Err: fmt.Errorf("%w: backup sequence", ErrCorrupt)}
	}
	if err := fs.NewFile(filepath.Join(stage, "BACKUP"), format.EncodeBackup(meta), "backup.manifest"); err != nil {
		return err
	}
	err = publishDirectory(ctx, fs, stage, target, "backup")
	published = err == nil
	return err
}

func copySegment(ctx context.Context, captured CapturedFile, uuid [16]byte, mayGrow bool, cutoff uint64, last *uint64, stage string, fs *IO, buffer []byte) (entry format.BackupFile, returned error) {
	src, err := OpenRegular(captured.File.Path)
	if err != nil {
		return entry, &SourceError{Err: err}
	}
	defer func() { returned = errors.Join(returned, src.Close()) }()
	st, err := src.Stat()
	if err != nil {
		return entry, &SourceError{Err: err}
	}
	if st.Size() < captured.Bytes || !mayGrow && st.Size() != captured.Bytes {
		return entry, &SourceError{Err: fmt.Errorf("%w: backup source file length", ErrCorrupt)}
	}
	name := DataName(uuid, captured.File.ID)
	dst, err := os.OpenFile(filepath.Join(stage, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return entry, err
	}
	defer func() { returned = errors.Join(returned, dst.Close()) }()
	header := make([]byte, format.SegmentHeaderBytes)
	if _, err := src.ReadAt(header, 0); err != nil {
		return entry, &SourceError{Err: err}
	}
	decoded, err := format.DecodeSegment(header)
	if err != nil {
		return entry, &SourceError{Err: err}
	}
	if decoded.UUID != uuid || decoded.ID != captured.File.ID || decoded.Kind != captured.File.Kind {
		return entry, &SourceError{Err: fmt.Errorf("%w: backup segment identity", ErrCorrupt)}
	}
	h := sha256.New()
	writer := &outputWriter{fs: fs, fd: dst, hash: h, point: "backup.copy"}
	if _, err := writer.Write(header); err != nil {
		return entry, err
	}
	for offset := int64(format.SegmentHeaderBytes); offset < captured.Bytes; {
		if err := ctx.Err(); err != nil {
			return entry, err
		}
		r, err := format.CopyRecord(ctx, sourceReader{src}, writer, offset, captured.Bytes-offset, nil, buffer)
		if err != nil {
			var destination *destinationError
			if !errors.As(err, &destination) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				err = &SourceError{Err: err}
			}
			return entry, err
		}
		if captured.File.Kind == format.Snapshot {
			if r.Kind != format.Put || r.Sequence > cutoff {
				return entry, &SourceError{Err: fmt.Errorf("%w: backup snapshot", ErrCorrupt)}
			}
		} else {
			if *last == math.MaxUint64 || r.Sequence != *last+1 {
				return entry, &SourceError{Err: fmt.Errorf("%w: backup log sequence", ErrCorrupt)}
			}
			*last = r.Sequence
		}
		offset += r.Bytes()
	}
	if err := fs.Sync(dst, "backup.file.sync"); err != nil {
		return entry, err
	}
	entry = format.BackupFile{Name: name, Bytes: captured.Bytes}
	copy(entry.Hash[:], h.Sum(nil))
	return entry, nil
}

func Restore(ctx context.Context, source, destination string, cfg Config, fs *IO) (returned error) {
	source, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	target, err := destinationPath(source, destination)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := ReadRegular(filepath.Join(source, "BACKUP"))
	if err != nil {
		return fmt.Errorf("%w: backup manifest: %w", ErrCorrupt, err)
	}
	meta, err := format.DecodeBackup(b)
	if err != nil {
		return err
	}
	files := make(map[string]format.BackupFile, len(meta.Files))
	for _, f := range meta.Files {
		_, data := parseID(f.Name, "DATA", meta.UUID)
		_, manifest := parseID(f.Name, "MANIFEST", meta.UUID)
		if f.Name != "CURRENT" && !data && !manifest {
			return fmt.Errorf("%w: unsafe backup entry", ErrCorrupt)
		}
		files[f.Name] = f
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	if len(entries) != len(files)+1 {
		return fmt.Errorf("%w: unexpected/missing backup objects", ErrCorrupt)
	}
	buffer := make([]byte, 32<<10)
	for _, entry := range entries {
		if entry.Name() == "BACKUP" {
			continue
		}
		f, listed := files[entry.Name()]
		if !listed {
			return fmt.Errorf("%w: unlisted backup object", ErrCorrupt)
		}
		if err := verifyBackupFile(ctx, source, f, buffer); err != nil {
			return err
		}
	}
	image, err := Validate(source, cfg)
	if err != nil {
		return err
	}
	if image.Current.UUID != meta.UUID || image.Current.Sequence != meta.Sequence || image.Catalog.Cutoff != meta.Cutoff ||
		image.Files[image.Current.ActiveID].Size.Load() != image.Current.ActiveEnd ||
		len(files) != len(image.Files)+2 {
		return fmt.Errorf("%w: backup image identity or committed length", ErrCorrupt)
	}
	if _, ok := files["CURRENT"]; !ok {
		return fmt.Errorf("%w: missing backup frontier", ErrCorrupt)
	}
	if _, ok := files[ManifestName(image.Current.UUID, image.Current.ManifestID)]; !ok {
		return fmt.Errorf("%w: missing backup catalog", ErrCorrupt)
	}
	for id := range image.Files {
		if _, ok := files[DataName(meta.UUID, id)]; !ok {
			return fmt.Errorf("%w: missing selected backup segment", ErrCorrupt)
		}
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
	for _, f := range meta.Files {
		if err := copyBackupFile(ctx, source, stage, f, fs, buffer); err != nil {
			return err
		}
	}
	if _, err := Validate(stage, cfg); err != nil {
		return err
	}
	err = publishDirectory(ctx, fs, stage, target, "restore")
	published = err == nil
	return err
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
	if actual := h.Sum(nil); string(actual) != string(entry.Hash[:]) {
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
	if string(h.Sum(nil)) != string(entry.Hash[:]) {
		return fmt.Errorf("%w: backup changed while restoring", ErrCorrupt)
	}
	return fs.Sync(dst, "restore.file.sync")
}
