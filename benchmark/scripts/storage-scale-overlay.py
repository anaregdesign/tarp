"""Prepare a Go overlay for the archived source mounted at /src in the runner."""
import json
import sys
from pathlib import Path

source, output = map(Path, sys.argv[1:])
replace = {}
probe = '"github.com/anaregdesign/tarp/benchmark/scaleprobe"'


def instrument(name, changes, add_import=True):
    text = (source / name).read_text()
    if add_import:
        changes = [('import (', 'import (\n\t' + probe, 1), *changes]
    for old, new, count in changes:
        if text.count(old) != count:
            raise ValueError(f'{name}: expected {count} occurrences of {old!r}')
        text = text.replace(old, new)
    filename = name.replace('/', '-')
    (output / filename).write_text(text)
    replace['/src/' + name] = '/probe/' + filename


instrument('benchmark/scaleprobe/probe.go', [
    ('const Enabled = false', 'const Enabled = true', 1),
], False)
instrument('internal/storage/store.go', [('os.ReadDir(', 'scaleprobe.ReadDir(', 3)])
instrument('internal/storage/clean_retire.go', [
    ('removed, err := remove(f.Path)',
     'previous := unlinked\n\t\tremoved, err := remove(f.Path)\n\t\tscaleprobe.UnlinkedDataBytes.Add(uint64(unlinked-previous))', 1),
])
instrument('internal/storage/recovery.go', [
    ('scan(ctx context.Context, visit recordVisitor) error {',
     'scan(ctx context.Context, visit recordVisitor) error {\n\tscaleprobe.ReplayPasses.Add(1)', 1),
    ('segment := image.Segments[entry.ID]\n\t\tfd, _, physical, err := openSelectedForRead(segment.Path, image.Current.UUID, entry, entry.ID == image.Current.ActiveID,',
     'scaleprobe.ReplayBytes.Add(uint64(entry.Bytes))\n\t\tsegment := image.Segments[entry.ID]\n\t\tfd, _, physical, err := openSelectedForRead(segment.Path, image.Current.UUID, entry, entry.ID == image.Current.ActiveID,', 1),
])
instrument('internal/engine/index.go', [
    ('if _, busy := db.memoryRemap.staged[move.Key]; busy {',
     'if _, busy := db.memoryRemap.staged[move.Key]; busy {\n\t\t\tscaleprobe.RelocationConflicts.Add(1)', 1),
])
instrument('internal/engine/cleaner.go', [
    ('entries, manifestBytes := db.store.Catalog.Entries, len(db.store.Manifest)',
     'lastUserSequence := db.store.Current.LastUserSequence\n\tentries, manifestBytes := db.store.Catalog.Entries, len(db.store.Manifest)', 1),
    ('return pending && removed > 0, pending && removed == 0',
     'if !pending { scaleprobe.CleanerIdle(lastUserSequence) }\n\t\treturn pending && removed > 0, pending && removed == 0', 1),
])
(output / 'overlay.json').write_text(json.dumps({'Replace': replace}, indent=2))
