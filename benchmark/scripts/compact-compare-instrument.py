"""Add stage timing only to archived v0.8.0 and v0.9.0 diagnostic builds."""
from pathlib import Path
import shutil
import sys

root = Path(sys.argv[1])
arm = sys.argv[2]
if arm not in ("base", "next"):
    raise ValueError(arm)
clock_import = '"github.com/anaregdesign/tarp/internal/compareclock"'


def edit(rel, changes):
    path = root / rel
    source = path.read_text()
    for old, new, count in [('import (', 'import (\n\t' + clock_import, 1), *changes]:
        if source.count(old) != count:
            raise ValueError(f"{rel}: expected {count} matches for {old!r}")
        source = source.replace(old, new)
    path.write_text(source)


def on_entry(prefix, statement):
    """Match the exact single-line Go function declaration in this release."""
    return (prefix + ' {', prefix + ' {\n\t' + statement, 1)


def timed(needle, timer):
    return on_entry(needle, f'defer compareclock.{timer}.Add(compareclock.Now())')


def writer_wait(timer):
    # A nested scope allows multiple acquisitions in the same function.
    return ('{\n start := compareclock.Now()\n db.writer.Lock()\n'
            f' compareclock.{timer}.Add(start)\n}}')


clock_dir = root / 'internal/compareclock'
clock_dir.mkdir()
shutil.copyfile(Path(__file__).resolve().parents[1] / 'testdata/compact_compare_clock.go',
                clock_dir / 'clock.go')
(root / 'benchmark/compact_compare_clock_test.go').write_text(
    'package benchmark\nimport ' + clock_import +
    '\nfunc init() { compactDiagnostic = compareclock.Report }\n')

moves = 'moves []storage.Relocation' if arm == 'base' else 'moves *storage.Relocations'
edit('internal/engine/compact.go', [
    on_entry('func (db *DB) Compact(ctx context.Context) (result CompactResult, returned error)',
             'compareclock.BeginCompact()\n\tdefer compareclock.EndCompact()'),
    timed(f'func (db *DB) relocate({moves}, after string)', 'Relocate'),
    ('db.writer.Lock()', writer_wait('OtherWriterWait'), 4),
])
edit('internal/engine/db.go', [
    ('db.writer.Lock()\n\tif hooks', writer_wait('ForegroundWait') +
     '\n\tcompareclock.ForegroundOwnsWriter.Store(true)\n\tif hooks', 1),
    ('db.writer.Unlock()\n\tif db.cleaner.writers',
     'compareclock.ForegroundOwnsWriter.Store(false)\n\tdb.writer.Unlock()\n\tif db.cleaner.writers', 1),
])
# These DB-wide publication and DATA syncs run under the foreground writer
# gate. Private checkpoint output uses different point names outside the gate.
old_sync = 'func (i *IO) Sync(f *os.File, point string) error { return i.Do(point, f.Sync) }'
edit('internal/storage/io.go', [(old_sync, '''func (i *IO) Sync(f *os.File, point string) error {
    if compareclock.ForegroundOwnsWriter.Load() {
        switch point {
        case "data.sync", "segment.sync", "segment.dirsync", "catalog.sync", "catalog.dirsync", "current.sync", "current.dirsync":
            defer compareclock.ForegroundSync.Add(compareclock.Now())
        }
    }
    return i.Do(point, f.Sync)
}''', 1)])

if arm == 'base':
    replay = 'func (c *Capture) Replay(ctx context.Context, fs *IO) ([]Relocation, error)'
    checkpoint_args = 'moves []Relocation'
    copy_record = 'func (r *snapshotReader) copyRecord(ctx context.Context, fs *IO, old Location, key string, dst *os.File, offset int64, buffer []byte) error'
    callback_changes = []
else:
    replay = 'func (c *Capture) replayLive(ctx context.Context, fs *IO, history index.Backend, live index.View, visit func(*os.File, string, Location) error) error'
    checkpoint_args = 'history index.Backend, live index.View, moves *Relocations'
    copy_record = 'func copyCheckpointRecord(ctx context.Context, fs *IO, src *os.File, old Location, key string, dst *os.File, offset int64, buffer []byte) error'
    callback_changes = [timed('func(src *os.File, key string, old Location) error', 'Emit')]
    edit('internal/engine/index.go', [
        ('db.writer.Lock()', writer_wait('RelocateWait'), 1),
    ])
edit('internal/storage/snapshot.go', [timed(replay, 'Replay')])
edit('internal/storage/checkpoint.go', [
    timed('func (c *Checkpoint) Copy(ctx context.Context, ' + checkpoint_args +
          ', reserve func() (*File, error)) (result error)', 'Copy'),
    timed(copy_record, 'RecordCopy'),
    *callback_changes,
])
