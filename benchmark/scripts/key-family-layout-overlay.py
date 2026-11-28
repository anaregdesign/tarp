"""Build the test-only Go overlay for the source mounted at /src."""
import json
import sys
from pathlib import Path

source, output = map(Path, sys.argv[1:])
output.mkdir()
replace = {}


def instrument(name, changes):
    text = (source / name).read_text()
    for old, new in changes:
        if text.count(old) != 1:
            raise ValueError(f'{name}: expected one occurrence of {old!r}')
        text = text.replace(old, new)
    filename = name.replace('/', '-')
    (output / filename).write_text(text)
    replace['/src/' + name] = '/probe/' + filename


# The benchmark keys have a ten-byte domain/generation/family prefix. Change
# the ordered output rollover only. Active append and CURRENT stay unchanged.
instrument('internal/storage/checkpoint.go', [
    ('var segment *DataSegment\n\tvar bounds snapshotBoundsBuilder',
     'splitFamilies := requireOrder && os.Getenv("TARP_KEY_FAMILY_BOUNDARIES") == "1"\n'
     '\tvar segment *DataSegment\n\tvar bounds snapshotBoundsBuilder'),
    ('previousOutputEnd := offset\n\t\tif fd == nil || offset > c.store.Config.TargetSegmentBytes-old.RecordBytes {',
     'previousOutputEnd := offset\n'
     '\t\tboundary := splitFamilies && len(previousKey) >= 10 && len(key) >= 10 && previousKey[:10] != key[:10]\n'
     '\t\tif fd == nil || offset > c.store.Config.TargetSegmentBytes-old.RecordBytes || boundary {'),
])

# The fixture has no active writers, parity worker or pins at this point. An
# idle result from the normal worker gives every arm the same stop boundary.
instrument('internal/engine/cleaner.go', [
    ('db.notifyCleaner()\n\t\tdefault:\n\t\t\tdelay = 0\n\t\t}',
     'db.notifyCleaner()\n\t\tdefault:\n\t\t\tdelay = 0\n'
     '\t\t\t_ = db.store.IO.Check("measure.family.cleaner.idle")\n\t\t}'),
])

(output / 'overlay.json').write_text(json.dumps({'Replace': replace}, indent=2) + '\n')
