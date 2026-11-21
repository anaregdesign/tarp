# Storage and backup format

All integers are unsigned little-endian unless stated otherwise. Each object has
one fixed eight-byte ASCII signature. There is no format-version field. All
reserved bytes are zero. CRC32C uses the Castagnoli polynomial. SHA-256 covers
complete file bytes. These checks detect accidental corruption.

An unknown outer signature in `CURRENT` or `BACKUP` returns
`ErrUnsupportedFormat`. Existing databases stay untouched and restore leaves
the destination unpublished. A recognized envelope with inconsistent contents,
a bad checksum, or a selected object with the wrong signature is corruption.

## Names and selection

```text
LOCK
CURRENT
MANIFEST-<32 lowercase UUID hex>-<16 lowercase nonzero ID hex>
DATA-<32 lowercase UUID hex>-<16 lowercase nonzero ID hex>
INDEX-<32 lowercase UUID hex>-<16 lowercase nonzero ID hex>
.tarp-current-<32 lowercase UUID hex>-<random>.tmp
.tarp-work/
```

IDs share one allocation space and do not wrap. Recognized names are inspected
before assigning IDs on reopen. UUID-bound names allow safe recognition of an
unselected partially written header; unknown objects are not garbage. `CURRENT`
alone selects the authoritative catalog. `.tarp-work/` is a reserved directory
for process-private scratch. Backup excludes it, INDEX, LOCK, unselected objects and
temporary files, and adds one `BACKUP` metadata file.

## Data segment header: 64 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `TARPSEGM` |
| 8 | 2 | Reserved |
| 10 | 2 | Header length, 64 |
| 12 | 4 | Kind: raw log=1, checkpoint snapshot=2, cleaned log=3 |
| 16 | 16 | Database UUID |
| 32 | 8 | Nonzero file ID |
| 40 | 20 | Reserved |
| 60 | 4 | CRC32C of bytes [0,60) |

## Whole record: 48-byte header, then key and value

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `TARPRECD` |
| 8 | 2 | Reserved |
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
| 0 | 8 | ASCII `TARPHEAD` |
| 8 | 2 | Reserved |
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

Batch writes append consecutive ordinary records in one segment. One frontier
publication selects the end offset and sequence of the final record.

Existing selected sealed/snapshot lengths must match exactly. Active physical
length must be at least its committed end; only a suffix strictly beyond that
end is discardable after validation and compatible-options checks.

## Shared catalog/backup envelope: 32 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | `TARPMNFT` or `TARPBACK` |
| 8 | 2 | Reserved |
| 10 | 2 | Reserved |
| 12 | 8 | Payload length, exactly file size minus 32 |
| 20 | 4 | Payload CRC32C |
| 24 | 4 | Complete header CRC32C with this field zero |
| 28 | 4 | Reserved |

## Catalog payload

The payload begins with a 144-byte prefix. The first 48 bytes contain the
database UUID[16], full-checkpoint cutoff C u64, pruned-through sequence H u64,
active file ID u64, and entry count u64. Bytes [48,144) hold the index
checkpoint descriptor.

| Payload offset | Bytes | Field |
| ---: | ---: | --- |
| 48 | 8 | INDEX object ID |
| 56 | 8 | Exact INDEX length, within signed file-offset range |
| 64 | 32 | SHA-256 of the complete INDEX |
| 96 | 8 | Covered user sequence |
| 104 | 8 | Number of covered catalog entries |
| 112 | 32 | Covered layout fingerprint |

All 96 descriptor bytes are zero when no checkpoint is selected. A present
descriptor has a nonzero ID distinct from the active DATA, every sealed DATA
and the selected MANIFEST. Recovery reserves this ID even if INDEX is missing.

Exactly the declared number of 72-byte catalog entries follow. The first 40
bytes hold five consecutive u64 fields giving the file ID, exact length, kind,
first sequence and last sequence. The final 32 bytes hold SHA-256 of the
complete sealed DATA, including the segment header. Physical entries require a
nonzero digest. Empty cleaned intervals have a zero digest.

The layout fingerprint is SHA-256 of these concatenated bytes, in order:

1. Eight ASCII bytes `TARPLAYT` and UUID[16].
2. Four little-endian u64 values giving C, H, covered sequence, and covered entry count.
3. The exact wire bytes of each covered entry, in catalog order, including empty intervals.

A prefix must contain every snapshot file. Its final sequence is the last
covered log interval's endpoint, or C when there are no covered log intervals.
The checkpoint may only cover a sequence at or above H. Neither the active ID
nor later sealed entries contribute to this digest, so an append or rotation
can preserve a previously published checkpoint. Cleaning or compaction clears
the descriptor in the same catalog publication that replaces physical files.

| Entry kind | Selected file and coverage |
| --- | --- |
| Snapshot | Nonzero unique ID. First and last are zero. Appears before all log intervals. Only allowed for C > 0. |
| Raw log | Nonzero unique ID. Contains every record from first through last, in order. |
| Cleaned log | Covers the original [first,last] of one log interval or the combined coverage of adjacent repacked intervals. A physical file contains at least one record, in strictly increasing sequence order within that interval. |
| Empty cleaned interval | Kind=cleaned log, ID=0 and length=0. A catalog entry alone represents the covered interval. |

All nonzero DATA IDs are distinct and exclude the active ID and selected catalog ID. A physical sealed
file contains at least a header and a record. Log intervals immediately follow
each other beginning at C+1. A cleaned interval ends at or below H. For active
log replay, `CURRENT` defines S, the last committed user sequence. If S equals
the last covered sequence (or C when there are no sealed logs), the active
committed end must be header-only. Otherwise the active prefix must contain
every remaining sequence through S exactly once. Compute endpoints without
wrapping at the maximum u64. Missing selected physical files are corruption.

The initial C, H and S are zero. C ≤ H ≤ S at every publication. Snapshot Puts
have unique keys across the entire checkpoint, and retain their original nonzero
sequences at or below C. Their physical order is arbitrary. At an incremental
replacement that prunes a Put, set H to the latest committed S under the writer
gate. Keep all post-checkpoint Deletes, including those whose predecessor was
discarded. A replayed Delete of an absent key is permitted only at or below H.
Consequently an omitted Put followed by a Delete in a later raw log still leaves
the key absent. Public Delete requires a live key. A full checkpoint replaces
the complete prefix, can discard all tombstones in that prefix, and sets C and H
to its captured user sequence. Later committed records survive publication.
Reopen, streamed backup and manual checkpoint replay apply the same rules.

## BACKUP payload

UUID[16], sequence u64, cutoff u64, advisory source cache bytes u64, advisory source
record bytes u64, an unused u64, and file count u64. Writers store zero in the
unused field at payload offset 48. Readers also accept values up to 2⁶³−1
that older writers stored there as an advisory per-file limit. Each variable
entry is name length u16, ASCII name bytes, exact file length u64, SHA-256[32].
Names are 1..96 bytes and must be generated CURRENT/DATA/MANIFEST names for the
same UUID. Reject duplicates, unsafe/missing/extra/nonregular objects, trailing
bytes, unsupported metadata, and inconsistent cutoff/frontier identities.

The listed files are exactly captured CURRENT, selected catalog, selected
snapshot/sealed files, and the active committed prefix. Empty logical intervals
reside entirely in the captured catalog. Its digest in CURRENT binds H and the
interval metadata to the backup. BACKUP excludes itself and LOCK. Restore
independently replays the complete image, derives live minima, checks exact
prefixes, and validates staged bytes.

Independent fixed hex vectors for all five codecs and cleaned-interval variants, per-byte bit flips,
truncations, and decoder fuzzing live in `internal/format/format_test.go`.

## Private B+tree files

Opening with `IndexBTree` uses a process-private working tree. Recovery can
import the selected packed index and replay its suffix, or build the tree from DATA. The in-process root descriptor identifies a generation and its
root page. A zero root represents an empty tree. Private files use 8 KiB
pages with one-based IDs. The page header records its CRC-32C, kind, page ID,
slot count, byte boundaries and leftmost child or next overflow page.
A leaf stores sorted suffixes under a common binary prefix, together with
DATA file ID, offset, record length, value length and sequence. Branch
separators route equal keys to the right. Long keys keep a prefix inline
and their remaining bytes in checked overflow pages.

Readers validate offsets, lengths, links, checksums and key ordering before
using pages loaded from disk. A snapshot retains its original root while a
writer copies changed paths. The OS releases the unlinked scratch files
when their descriptors close.


## INDEX file

The catalog selects the entire INDEX length and SHA-256. The outer header is
48 bytes. Bytes [0,8) are `TARPIDX1`, [8,40) hold the selected layout
fingerprint, and [40,48) hold the covered catalog entry count. One u64
tombstone byte count per covered entry follows in catalog order. An empty
interval or snapshot has zero tombstone bytes.

The packed tree follows immediately. Its 48-byte header begins with
`TARPBTP1`, followed by the u32 page size (8192), the CRC32C of [16,48),
and four u64 fields for root page ID, page count, live key count and maximum
key length. Empty trees have four zero fields. Exactly the declared number of
8 KiB pages follow, in one-based page ID order. The importer checks page and
overflow links, search routing, live addresses and totals before admitting
readers. Its private allocator starts above the imported pages. The immutable
published file can be removed after the catalog selects a replacement.
