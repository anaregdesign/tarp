"""Add stage timing only to diagnostic builds of the compared archives."""
from pathlib import Path
import shutil
import sys

root = Path(sys.argv[1])
layout = sys.argv[2]
if layout not in ("v08", "v09"):
    raise ValueError(layout)
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


def probe_relocation_index_gate(source):
    # The foreground staging path can also take this gate. Only measure the
    # relocation functions, including the fallback after a staged-key conflict.
    before = 'db.indexGate.Lock()\n\tdefer db.indexGate.Unlock()'
    after = ('gateStart := compareclock.Now()\n\tdb.indexGate.Lock()\n'
             '\tcompareclock.RelocateIndexWait.Add(gateStart)\n\tdefer db.indexGate.Unlock()\n'
             '\tdefer compareclock.RelocateIndexHold.Add(compareclock.Now())')
    functions = source.split('\nfunc ')
    count = 0
    for i, fn in enumerate(functions):
        if fn.startswith(('(db *DB) relocateBatch(', '(db *DB) relocateUnstaged(')):
            if fn.count(before) != 1:
                raise ValueError('missing index gate in ' + fn.splitlines()[0])
            functions[i] = fn.replace(before, after)
            count += 1
    if count == 0:
        raise ValueError('missing gated relocation')
    return '\nfunc '.join(functions)


clock_dir = root / 'internal/compareclock'
clock_dir.mkdir()
shutil.copyfile(Path(__file__).resolve().parents[1] / 'testdata/compact_compare_clock.go',
                clock_dir / 'clock.go')
(root / 'benchmark/compact_compare_clock_test.go').write_text(
    'package benchmark\nimport ' + clock_import +
    '\nfunc init() {\n compactDiagnostic = compareclock.Report\n compactGateSamples = compareclock.GateSamples\n}\n')

compact = (root / 'internal/engine/compact.go').read_text()
# Relocation has different parameters in each revision. Time the full method.
relocate, = (line.removesuffix(' {') for line in compact.splitlines()
             if line.startswith('func (db *DB) relocate(') and line.endswith(' {'))
edit('internal/engine/compact.go', [
    on_entry('func (db *DB) Compact(ctx context.Context) (result CompactResult, returned error)',
             'compareclock.BeginCompact()\n\tdefer compareclock.EndCompact()'),
    timed(relocate, 'Relocate'),
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

if layout == 'v08':
    replay = 'func (c *Capture) Replay(ctx context.Context, fs *IO) ([]Relocation, error)'
    copy_record = 'func (r *snapshotReader) copyRecord(ctx context.Context, fs *IO, old Location, key string, dst *os.File, offset int64, buffer []byte) error'
    callback_changes = []
else:
    replay = 'func (c *Capture) replayLive(ctx context.Context, fs *IO, history index.Backend, live index.View, visit func(*os.File, string, Location) error) error'
    copy_record = 'func copyCheckpointRecord(ctx context.Context, fs *IO, src *os.File, old Location, key string, dst *os.File, offset int64, buffer []byte) error'
    callback_changes = [timed('func(src *os.File, key string, old Location) error', 'Emit')]
    edit('internal/engine/index.go', [
        ('db.writer.Lock()', writer_wait('RelocateWait'), 1),
        ('defer db.writer.Unlock()', 'defer db.writer.Unlock()\n\tdefer compareclock.RelocateHold.Add(compareclock.Now())', 1),
    ])
    index_file = root / 'internal/engine/index.go'
    index_file.write_text(probe_relocation_index_gate(index_file.read_text()))
edit('internal/storage/snapshot.go', [timed(replay, 'Replay')])
checkpoint = (root / 'internal/storage/checkpoint.go').read_text()
copy, = (line.removesuffix(' {') for line in checkpoint.splitlines()
         if line.startswith('func (c *Checkpoint) Copy(') and line.endswith(' {'))
edit('internal/storage/checkpoint.go', [
    timed(copy, 'Copy'),
    timed(copy_record, 'RecordCopy'),
    *callback_changes,
])
