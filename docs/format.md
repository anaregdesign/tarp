# Storage and backup format

See the [storage terminology](terminology.md) for the relationships between
segments, records, pages, DATA blocks and catalog entries.

All integers are unsigned little-endian unless stated otherwise. Each object has
one fixed eight-byte ASCII signature. There is no format-version field. All
reserved bytes are zero. CRC32C uses the Castagnoli polynomial. File digests use
SHA-256 over complete file bytes. The catalog also uses SHA-256 for the covered
layout fingerprint defined below. These checks detect accidental corruption.

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
PARITY-<32 lowercase UUID hex>-<16 lowercase DATA ID hex>
INDEX-<32 lowercase UUID hex>-<16 lowercase nonzero ID hex>
.tarp-current-<32 lowercase UUID hex>-<random>.tmp
.tarp-work/
```

DATA segment, MANIFEST and INDEX file IDs share one allocation space and do not wrap. Recognized names are inspected
before assigning IDs on reopen. UUID-bound names allow safe recognition of an
unselected partially written header; unknown objects are not garbage. `CURRENT`
alone selects the authoritative catalog. `.tarp-work/` is a reserved directory
for process-private scratch. Backup excludes it, INDEX, PARITY, LOCK, unselected objects and
temporary files, and adds one `BACKUP` metadata file.

## DATA segment header: 64 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `TARPSEGM` |
| 8 | 2 | Reserved |
| 10 | 2 | Header length, 64 |
| 12 | 4 | Kind: raw log segment=1, snapshot segment=2, cleaned log segment=3 |
| 16 | 16 | Database UUID |
| 32 | 8 | Nonzero file ID |
| 40 | 20 | Reserved |
| 60 | 4 | CRC32C of bytes [0,60) |

## DATA record: 48-byte header, then key and value

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

## Sealed DATA parity

One PARITY sidecar protects one sealed DATA segment, including its 64-byte
segment header. A newly sealed segment can await sidecar creation. The filename reuses its DATA ID. The trusted source
of the expected identity, length and SHA-256 is the selected MANIFEST.
Compaction requires stripes bound to this identity. Tarp rebuilds missing or
invalid sidecars from healthy sealed DATA.

| Header offset | Bytes | Field |
| --- | --- | --- |
| 0 | 8 | `TARPPAR2` |
| 8 | 16 | Database UUID |
| 24 | 8 | DATA ID |
| 32 | 8 | Exact DATA length |
| 40 | 32 | SHA-256 of the complete DATA |
| 72 | 4 | DATA kind |
| 76 | 4 | DATA block size, 4096 |
| 80 | 4 | DATA blocks per stripe, 16 |
| 84 | 4 | CRC32C of bytes [0,84) |

After the PARITY header, each stripe has 112 bytes of metadata followed by an
XOR payload. It covers up to 64 KiB of consecutive DATA bytes.

| Stripe offset | Bytes | Field |
| --- | --- | --- |
| 0 | 8 | Zero-based stripe ordinal |
| 8 | 64 | Sixteen u32 CRC32Cs of actual DATA bytes, zero for nonexistent blocks |
| 72 | 32 | Sixteen u16 offsets to the first record header in each block, relative to its start. 0xffff means no record starts there |
| 104 | 4 | CRC32C of the XOR payload |
| 108 | 4 | CRC32C of header bytes [0,84) followed by stripe bytes [0,108) |
| 112 | Up to 4096 | XOR payload computed from the DATA blocks in this stripe |

A shorter final block contributes zero beyond its end to the XOR. The XOR payload length is the smaller
of 4096 and the number of DATA bytes in the stripe.
The header of a record may cross a block boundary. Its anchor belongs to the block
containing its first byte. No padding or trailing bytes follow the final stripe.
The cost is 4208 bytes per full 65536-byte stripe (about 6.42%) plus the 88-byte
file header. For DATA smaller than one block, the sidecar contains a full copy of
the DATA bytes plus 200 bytes of metadata.

## Commit frontier: exactly 128 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | ASCII `TARPHEAD` |
| 8 | 2 | Reserved |
| 10 | 2 | Length, 128 |
| 12 | 4 | Reserved |
| 16 | 16 | Database UUID |
| 32 | 8 | Nonzero CURRENT publication generation |
| 40 | 8 | Selected catalog ID |
| 48 | 32 | SHA-256 of the selected MANIFEST object |
| 80 | 8 | Active log segment ID |
| 88 | 8 | Active committed end offset (exclusive, in bytes), at least 64 |
| 96 | 8 | Last user sequence; initial value zero |
| 104 | 20 | Reserved |
| 124 | 4 | CRC32C of bytes [0,124) |

Batch writes append consecutive ordinary records in one segment. One frontier
publication selects the byte offset immediately after the final record and its inclusive user sequence.

Existing selected sealed segment lengths must match exactly. Active physical
length must be at least its committed end; only a suffix strictly beyond that
end is discardable after validation and compatible-options checks.

## Shared catalog/backup envelope: 32 bytes

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | `TARPMNFT`, `TARPMNED`, `TARPMNSA`, or `TARPBKUP` |
| 8 | 2 | Reserved |
| 10 | 2 | Reserved |
| 12 | 8 | Payload length, exactly file size minus 32 |
| 20 | 4 | Payload CRC32C |
| 24 | 4 | Complete header CRC32C with this field zero |
| 28 | 4 | Reserved |

## Catalog base and edits

A `TARPMNFT` MANIFEST is a complete catalog base. A `TARPMNED` object contains
the previous MANIFEST ID (u64), its SHA-256 (32 bytes), a zero-based splice
position (u64), and a removed entry count (u64). The remaining bytes encode a
catalog payload with the replacement header and zero or one inserted entry.
A rotation appends the sealed active segment. Cleaning replaces or removes
one consecutive range. An INDEX-only publication changes the header.

A `TARPMNSA` object uses the same payload framing for a staged append.
Its splice position is the end of the preceding catalog and its removal
count is zero. It can append up to 1,024 sealed raw log segments. The
nonempty former active segment, when present, is first. A header-only
former active segment is omitted. Inserted DATA IDs increase up to the
new active ID, which identifies the last file of the staged commit.
The cutoff, pruning boundary and INDEX descriptor are preserved.

`CURRENT` authenticates the newest object. Recovery follows the authenticated
predecessors back to a base, then applies edits from oldest to newest before
validating the selected layout. Predecessor IDs must decrease and at most 256
edits can follow a base. Full DATA checkpoints can directly publish a new base.
Native backups serialize their captured layout as a standalone base with its
digest in the captured `CURRENT`.

## Catalog payload

The payload begins with a 144-byte prefix. The first 48 bytes contain the
database UUID[16], full DATA checkpoint cutoff sequence C u64, pruned-through sequence H u64,
active file ID u64, and entry count u64. Bytes [48,144) hold the index
checkpoint descriptor.

| Payload offset | Bytes | Field |
| ---: | ---: | --- |
| 48 | 8 | INDEX object ID |
| 56 | 8 | Exact INDEX length, within signed file-offset range |
| 64 | 32 | SHA-256 authenticating INDEX metadata and its B+tree header |
| 96 | 8 | Covered user sequence |
| 104 | 8 | Number of covered catalog entries |
| 112 | 32 | Covered layout fingerprint |

All 96 descriptor bytes are zero when no index checkpoint is selected. A present
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

A covered catalog prefix must contain every snapshot segment. Its covered sequence is the last
user sequence of the final covered log interval, or C when there are no covered log intervals.
The index checkpoint covers a sequence at or above the H recorded in its own
covered layout. Incremental cleaning can advance the selected catalog's H
without changing that saved layout. Neither the active ID
nor later sealed entries contribute to this fingerprint, so an append or rotation
can preserve a previously published checkpoint. Incremental cleaning retains
the descriptor and its original covered layout. A full DATA checkpoint clears
the descriptor when publishing its new layout.

| Entry kind | Selected file and coverage |
| --- | --- |
| Snapshot segment | Nonzero unique ID. First and last are zero. Appears before all log intervals. Only allowed for C > 0. |
| Raw log segment | Nonzero unique ID. Contains every record from first through last, in order. |
| Cleaned log segment | Covers the original `[first, last]` of one log interval or the combined coverage of consecutive log sequence intervals. A physical file contains at least one record, in strictly increasing sequence order within that interval. |
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
the key absent. Public Delete requires a live key. A full DATA checkpoint replaces
the complete prefix, can discard all tombstones in that prefix, and sets C and H
to its captured user sequence. Later committed records survive publication.
Reopen, streamed backup and full DATA checkpoint replay apply the same rules.

## BACKUP payload

The 56-byte prefix contains UUID[16], last user sequence u64, full DATA
checkpoint cutoff sequence u64, advisory source
value-cache budget u64, advisory source maximum record bytes u64, and file count u64. Each variable
entry is name length u16, ASCII name bytes, exact file length u64, SHA-256[32].
Names are 1..96 bytes and must be generated CURRENT/DATA/MANIFEST names for the
same UUID. Reject duplicates, unsafe/missing/extra/nonregular objects, trailing
bytes, unsupported metadata, and inconsistent cutoff/frontier identities.

The listed files are exactly captured CURRENT, selected catalog, selected
sealed DATA segments, and the active committed DATA prefix. Empty logical intervals
reside entirely in the captured catalog. Its digest in CURRENT binds H and the
interval metadata to the backup. BACKUP excludes itself and LOCK. Restore
independently replays the complete image, derives live minima, checks exact
prefixes, and validates staged bytes. The restored database is published with
a fresh, empty LOCK file.

Independent fixed hex vectors for all five codecs and cleaned-interval variants, per-byte bit flips,
truncations, and decoder fuzzing live in `internal/format/format_test.go`.

## Private B+tree files

Opening with `IndexBTree` uses a process-private working tree. Recovery can
reuse the selected packed INDEX as an immutable base with private copy-on-write
pages, import the packed tree, or build from DATA. The in-process root descriptor identifies a
B+tree root generation and its root page. A zero root represents an empty tree. Private files use 8 KiB
pages with one-based IDs. The page header records its CRC-32C, kind, page ID,
slot count, byte boundaries and leftmost child or next overflow page.
A leaf page stores sorted key suffixes under a shared key prefix, together with
DATA file ID, offset, record length, value length and sequence. Branch
separators route equal keys to the right. Long keys keep a prefix inline
and their remaining bytes in checked overflow pages.

Readers validate offsets, lengths, links, checksums and key ordering before
using pages loaded from disk. A B+tree index snapshot retains its original root while a
writer copies changed paths. The OS releases the unlinked scratch files
when their descriptors close.


## INDEX file

Both standalone and layered INDEX files begin with a 128-byte summary.

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | `TARPIDX2` for a standalone INDEX, or `TARPIDXL` for an INDEX overlay |
| 8 | 32 | Covered layout fingerprint |
| 40 | 8 | Covered catalog entry count |
| 48 | 8 | Length of the encoded covered catalog base |
| 56 | 8 | Length of the physical B+tree header and page slots, excluding hashes |
| 64 | 8 | Live key count |
| 72 | 8 | Upper bound on the largest live encoded record |
| 80 | 8 | Upper bound on the largest live value |
| 88 | 32 | Root SHA-256 for the page hashes |
| 120 | 8 | Reserved |

An overlay places its parent's 96-byte INDEX descriptor immediately after
the summary. The covered catalog base follows, with its own INDEX descriptor
cleared. Then each covered entry has three u64 counts in catalog order for
tombstone bytes, live keys and live record bytes. The last metadata field is
the 48-byte B+tree header. The descriptor digest hashes the optional parent
descriptor, encoded catalog, count tuples, 128-byte summary, and B+tree header
in that order.

The standalone packed tree's 48-byte header begins with
`TARPBTP1`, followed by the u32 page size (8192), the CRC32C of [16,48),
and four u64 fields for root page ID, page count, live key count and maximum
key length. Empty trees have four zero fields. Exactly the declared number of
8 KiB pages follow, in one-based page ID order.

An INDEX overlay has this 48-byte B+tree header. The numeric fields are u64.

| Offset | Bytes | Field |
| ---: | ---: | --- |
| 0 | 8 | `TARPBTL1` |
| 8 | 8 | Highest inherited page ID |
| 16 | 8 | Root page ID |
| 24 | 8 | Highest logical page ID |
| 32 | 8 | Live key count |
| 40 | 8 | Maximum key length |

The parent's highest logical page ID equals the inherited boundary. The overlay
stores one 8 KiB slot per page ID strictly above that boundary through the new
highest logical page ID, in ascending order. Slots unreachable from its tree
are zero-filled. An empty logical tree has a zero root, count and key length
and retains its page ID boundary. A chain has one standalone base and at most
eight overlays, each with at most 32,768 slots (256 MiB of page slots). Parent
object IDs decrease and parent covered sequences never exceed the child's.
Each covered catalog shares the same full DATA checkpoint cutoff. Ancestors
supply inherited pages, while the tip supplies the layout and counts used to
replay changed DATA.

The footer stores SHA-256 hashes in 4 KiB blocks with up to 128 hashes per
block and zero padding in unused hash positions. The first level hashes the
physical page slots. Each subsequent level hashes the previous level's blocks,
ending with a single block whose hash is recorded in the summary. A file with
zero physical page slots has no hash blocks and a zero root hash.

Eager standalone INDEX import validates packed pages and tree topology before
admitting readers. `Indexed` with B+tree authenticates individual pages as it
loads them. `Full` and IndexMemory replay authoritative DATA for layered
checkpoints. A valid selected INDEX chain retains its ancestor files through
publication and cleanup. New private pages use IDs above the highest immutable
page ID. An open immutable file stays readable through its descriptor after a
newer checkpoint retires its name.
