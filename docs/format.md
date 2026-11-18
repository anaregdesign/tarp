# Version-one file and backup formats

All integers are unsigned little-endian unless stated otherwise. Version is 1.
CRC32C means the Castagnoli polynomial; SHA-256 covers complete file bytes.
Reserved fields must be zero. CRCs are accidental-corruption detection, not
authenticity. Unknown versions fail with `ErrUnsupportedFormat`; invalid lengths,
reserved fields, checksums, identities, or structural sequences fail explicitly.
Do not silently migrate or guess a format.

## Names and selection

```text
LOCK
CURRENT
MANIFEST-<32 lowercase UUID hex>-<16 lowercase nonzero ID hex>
DATA-<32 lowercase UUID hex>-<16 lowercase nonzero ID hex>
.tarp-current-<32 lowercase UUID hex>-<random>.tmp
```

IDs share one allocation space and do not wrap. Recognized names are inspected
before assigning IDs on reopen. UUID-bound names allow safe recognition of an
unselected partially written header; unknown objects are not garbage. `CURRENT`
alone selects the authoritative catalog. Metadata is not a data file subject
to `MaxDataFileBytes`. Backup excludes LOCK, unselected objects, and temporary
files and adds one `BACKUP` metadata file.

## Data segment header: 64 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `TARPSEG1` |
| 8 | 2 | Version |
| 10 | 2 | Header length, 64 |
| 12 | 4 | Kind: log=1, snapshot=2 |
| 16 | 16 | Database UUID |
| 32 | 8 | Nonzero file ID |
| 40 | 20 | Reserved |
| 60 | 4 | CRC32C of bytes [0,60) |

## Whole record: 48-byte header, then key and value

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `TARPREC1` |
| 8 | 2 | Version |
| 10 | 1 | Kind: Put=1, Delete=2 |
| 11 | 1 | Flags, zero |
| 12 | 4 | CRC32C of complete header with this field zero |
| 16 | 8 | Nonzero user sequence |
| 24 | 8 | Nonzero key length K |
| 32 | 8 | Value length V |
| 40 | 4 | CRC32C of key followed by value |
| 44 | 4 | Reserved |
| 48 | K+V | Key followed by value |

Delete requires V=0. Empty Put values are valid. No padding, trailer, or
cross-file fragmentation exists. Checked lengths must fit the committed
remaining range, signed file offsets, and native allocation limits before use.
Replay streams values, verifies complete byte counts and CRCs, then retains keys.
Obsolete records are validated even above a new admission limit.

## Commit frontier: exactly 128 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `TARPCUR1` |
| 8 | 2 | Version |
| 10 | 2 | Length, 128 |
| 12 | 4 | Reserved |
| 16 | 16 | Database UUID |
| 32 | 8 | Nonzero publication generation |
| 40 | 8 | Selected catalog ID |
| 48 | 32 | SHA-256 of selected complete catalog |
| 80 | 8 | Active log segment ID |
| 88 | 8 | Active committed end, at least 64 |
| 96 | 8 | Last user sequence; initial value zero |
| 104 | 20 | Reserved |
| 124 | 4 | CRC32C of bytes [0,124) |

Existing selected sealed/snapshot lengths must match exactly. Active physical
length must be at least its committed end; only a suffix strictly beyond that
end is discardable after validation and compatible-options checks.

## Shared catalog/backup envelope: 32 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | `TARPCAT1` or `TARPBAK1` |
| 8 | 2 | Version |
| 10 | 2 | Reserved |
| 12 | 8 | Payload length, exactly file size minus 32 |
| 20 | 4 | Payload CRC32C |
| 24 | 4 | Complete header CRC32C with this field zero |
| 28 | 4 | Reserved |

## Catalog payload

UUID[16], cutoff sequence u64, active ID u64, entry count u64, followed by
24-byte entries: ID u64, exact length u64, kind u64.
The count must exactly match the remaining payload. IDs are nonzero and unique,
including the active ID. Snapshot entries precede log entries; no snapshot is
allowed with cutoff zero. The active header-only/log prefix is selected separately
by CURRENT, not duplicated in the entry list.

Snapshot records are unique live Put keys with original nonzero versions at or
below the cutoff. Subsequent logs contain contiguous sequences beginning at
cutoff+1. A Delete must remove a live key. An empty checkpoint still preserves
its cutoff and next sequence; do not infer them from surviving keys.

## BACKUP payload

UUID[16], sequence u64, cutoff u64, advisory source cache bytes u64, advisory source
record bytes u64, advisory source file bytes u64, file count u64. Each variable
entry is name length u16, ASCII name bytes, exact file length u64, SHA-256[32].
Names are 1..96 bytes and must be generated CURRENT/DATA/MANIFEST names for the
same UUID. Reject duplicates, unsafe/missing/extra/nonregular objects, trailing
bytes, unsupported metadata, and inconsistent cutoff/frontier identities.

The listed files are exactly captured CURRENT, selected catalog, selected
snapshot/sealed files, and the active committed prefix. BACKUP does not list
itself or LOCK. Source options are not minimum limits: restore independently
replays the complete image and derives live minima, checks exact prefixes, and
validates staged bytes rather than relying on hashes alone.

Independent fixed hex vectors for all five codecs, per-byte bit flips,
truncations, and decoder fuzzing live in `internal/format/format_test.go`.
