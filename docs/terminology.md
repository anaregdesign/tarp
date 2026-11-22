# Storage terminology

A lookup in the B+tree visits branch pages and a leaf page to find an encoded
key's DATA record address. The address gives the DATA segment ID and the byte
offset of the record header. A read uses the record length to fetch the value.
Parity interprets that same segment as a sequence of DATA blocks starting at
file offset zero. A single record can cross several block and stripe boundaries.

## Stored units

| Term | Meaning in Tarp |
| --- | --- |
| DATA segment | One `DATA-…` file with a 64-byte segment header. Raw log, cleaned log and snapshot are segment kinds. Active and sealed describe the segment's lifecycle. `TargetSegmentBytes` applies to each output segment. |
| DATA record | One Put or Delete stored wholly in one segment. The record header occupies 48 bytes and the record payload contains the encoded key followed by the encoded value. A batch can publish multiple records together. |
| Catalog entry | A descriptor in the selected MANIFEST. An empty cleaned interval covers user sequences with zero physical segments. The active segment's descriptor is derived separately from the catalog and CURRENT. |
| Index entry | A mapping from one live encoded key to its DATA record address, in either index backend. The address also carries the record length, value length and user sequence for checked reads. |
| B+tree page | An 8 KiB unit with a one-based page ID in a private B+tree or the packed tree inside an INDEX file. A branch page routes by separator and child page ID. A leaf page stores keys and DATA record addresses. An overflow page holds the remaining bytes of a long key. |
| Node, cell, slot | A node represents a leaf or branch in the tree. A cell holds one leaf mapping or one branch separator and child. A slot gives the cell's byte offset within its page. |
| DATA block | Up to 4 KiB of a segment covered by one parity block checksum. The last block can be short. Its record anchor gives the offset, relative to the block, of the first record header starting there, if any. |
| Parity stripe | Up to 16 consecutive DATA blocks in one segment. PARITY stores 112 bytes of stripe metadata and an XOR payload of up to 4 KiB. A valid stripe can reconstruct one unavailable DATA block. |
| I/O chunk, buffer, sort run | A chunk is the portion processed in one step. A buffer holds bytes in memory, such as the replay read-ahead buffer or a key-copy buffer. A replay sort run is an ordered stream of temporary records and can span multiple in-memory chunks. |

## Addresses, ranges and ordering

- A **DATA byte range** uses file-relative offsets `[start, end)`. The active
  committed end offset is the first byte beyond the active committed prefix.
  The physical file length can exceed that offset after an interrupted append.
  **Contiguous DATA records** are in one segment with no gap between records.
- A **log sequence interval** uses inclusive user sequences `[first, last]`.
  The **sealed-through sequence** is the last sequence covered by the full DATA
  checkpoint and sealed log intervals. The **checkpoint cutoff sequence** is
  the last sequence included in the full DATA checkpoint. The **pruned-through
  sequence** bounds replay of Deletes whose preceding Put cleaning could have
  removed. The **last user sequence** includes the active committed prefix.
- A **key range** uses encoded-key order and a half-open upper bound when one
  exists. An **encoded key prefix** selects keys for a prefix scan. A **shared
  key prefix** compresses keys within a B+tree page. Public scan order is
  unspecified. Keys matching the same prefix can occupy distant DATA offsets.
- A **covered catalog prefix** comprises the consecutive catalog entries
  described by an index checkpoint. Recovery imports the index and replays the
  subsequent **DATA log suffix**. A cleaner selects consecutive segments in
  catalog order, which says nothing about their placement on the device.
- A **user sequence** identifies a record's logical version. Relocation can
  change its address while preserving that sequence. A **CURRENT publication
  generation** identifies a commit frontier publication, including maintenance.
  A **B+tree root generation** identifies a process-private index state that
  readers can hold while a writer publishes another root.

## Byte accounting and measurements

For a Put with a 1-byte encoded key and a 3-byte encoded value, the **record
payload** is 4 bytes, the **encoded record** is 52 bytes and its **value-cache
charge** is 132 bytes (128 bytes of logical metadata plus key and value).
A write batch charges the sum of its encoded record lengths against the
effective segment target minus the segment header. A BatchGet of that key
twice charges 6 **result value bytes**. A cache miss counts a live key absent
from the value LRU. A missing-key lookup counts an absent key.

**Selected DATA bytes** include segment headers, live Put records, obsolete Put
records and retained Delete records (tombstones). **Live record bytes** count
only the current Put for each live key. **Unlinked bytes** sum logical lengths
of removed files. Filesystem allocation and available space have their own
measurements. See [database statistics](stats.md).

Name locality measurements after their observation point. For example,
**distinct 4 KiB DATA file ranges per scan** counts distinct pairs of segment
and aligned offset range touched by records. **B+tree page reads** count pager
fetches that may hit the OS page cache. **Process read bytes** count I/O charged
by the operating system. Use **filesystem block**, **OS page cache** and
**NBD overlay page** when describing those layers.

## Publication, maintenance and recovery

| Term | What it selects, retains or produces |
| --- | --- |
| Commit frontier and catalog | CURRENT records the durable commit frontier and selects a MANIFEST by ID and file digest. The catalog in that MANIFEST describes the DATA layout and optional index checkpoint. Foreground writes publish their in-memory index changes after durable frontier publication. |
| File digest and covered layout fingerprint | A file digest covers the complete file bytes. The covered layout fingerprint binds an index checkpoint to specific catalog entries and sequence boundaries. Appends after that prefix can preserve the fingerprint. A process-local layout revision tracks replacements for a worker. |
| Full DATA checkpoint | `Compact` replaces all history through its captured cutoff with snapshot segments containing the retained live records. An index snapshot is a reader's captured view. |
| Index checkpoint | `CheckpointIndex` publishes a derived immutable INDEX for a covered catalog prefix. `IndexMode` chooses the runtime backend. `OpenValidation` chooses how to verify DATA at Open. Both backends can import the packed index. |
| Cleaning and repacking | Single-segment cleaning and grouped log cleaning reclaim obsolete records. Small-segment repacking combines small segments, even when every record is live. A retired segment has left the selected layout. Unlink removes its file name once its references permit cleanup. |
| Segment pin, live index reference and read-handle lease | A reader, backup or worker can pin a segment. Live index entries also retain their referenced segment until remapped. Either delays unlink. A read-handle lease keeps an opened descriptor available to an in-flight read, including during replacement of the named file. |
| Parity protection and DATA repair | Sealing ends appends to a segment. Protection starts once its PARITY sidecar is durable. `ProtectParity` verifies or builds sidecars. `RepairDataFromParity` verifies the selected DATA and repairs recoverable sealed segments from PARITY. |
| File backup, live key-value export and historical record salvage | `Backup` captures committed file bytes at one point in time. Scanning live entries exports the current key-value state of the captured view. `SalvageData` exports verified historical records from one sealed segment, including superseded Puts and Deletes. Its reported gaps are physical byte ranges. |
| Unavailable DATA and dropped key | An unavailable record cannot be served by that read. Repair may require additional space. A dropped key leaves the live state only when a recovery checkpoint publishes proven loss. |

See the [wire format](format.md) for byte layouts and the
[durability contract](durability.md) for publication and recovery guarantees.
