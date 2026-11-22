# Durability, concurrency, and operational contract

## Support boundary

The current unversioned format and its automatic cleaner have a functional
recovery test suite. Their whole-VM power-loss behavior and performance require
fresh evidence. The historical qualification below belongs to the previous
storage implementation. Its frozen source fingerprint and run links are in the
[execution matrix](qualification.md#final-selected-target-execution-matrix).

That qualification passed on revision
[`347a1a1`](https://github.com/anaregdesign/tarp/commit/347a1a1f53be13100a8c83da7378b9af26e4657c)
with Go 1.26.11/1.27.4, Ubuntu 24.04.5 amd64, kernel 6.8.0-147-generic,
local guest ext4 `data=ordered` with normal journaling/barriers and no DAX,
QEMU 8.2.2 and an honest-flush volatile NBD block model.
The external controller performed 328 phase cuts and two post-exhaustion cuts,
waited for NBD EOF, discarded any RAM overlay, and verified file-only recovery/
captured backups. All 330 engine cuts recorded zero dirty NBD overlay bytes;
they still abruptly lost the complete guest CPU/RAM, including its page cache.
The separate raw-device controls actually discarded 4,096 overlay-page bytes:
flushed A survived and unflushed B was absent. Observed guest FUA traffic was zero.
The recorded image/kernel hashes, exact features/mounts, storage parameters,
source fingerprint and raw witnesses define this experimental boundary.
NBD here backs a guest block device; it is not a network filesystem.

Qualification applies to that revision and that precisely tested virtual stack. A physical
deployment must satisfy the same documented filesystem/sync and honest-flush
assumptions; no firmware/cache certification is inferred from a VM result.
Additional kernels/filesystems/storage configurations require separate evidence.

Native macOS ordinary tests, hosted Linux amd64 CI, and Linux arm64
Docker/LinuxKit overlayfs tests are development/functional evidence only.
They do not establish that production target's crash guarantees.
No XFS, NFS/SMB, FUSE, overlayfs, tmpfs, DAX,
barrier-disabled filesystem, or generic-all-Linux power-loss guarantee is made.
Device failure/loss and lying flush implementations are excluded. Checksums
detect accidental corruption; they do not repair lost hardware or authenticate
data. Files alone are the engine's recovery source.

The initial Go support policy is the latest officially supported patches of Go 1.26 and
1.27, with a `go 1.26.0` module minimum and no forced `toolchain` directive.
Observed validation patches are 1.26.11 and 1.27.4. Advance the policy with Go
support rather than interpreting those patches as permanently supported.
Automated CI intentionally covers only Linux amd64 with the latest stable Go.

## Package and lock ownership

The root package exposes the public Go API. `internal/engine` coordinates
lifecycle admission, index and cache access, writers, and maintenance.
`internal/storage` owns file protocols, replay and leases. `internal/format`
defines binary codecs, `internal/cache` implements exact global LRU, and
`internal/platform` provides Linux and Darwin primitives. Measurement drivers
and qualification tools live under `benchmark/`.

One nonblocking advisory `flock` and an in-process actual-directory identity
registry reject duplicate and aliased opens. The stable `LOCK` inode is never
unlinked to "break" ownership. External edits/renames/unlinks of the directory,
its lock, or engine files are unsupported. Parent hierarchies must already
exist durably and remain stable.
Generated directories/files use 0700/0600, subject to a stricter umask.

A writer gate serializes durable publications. The short state mutex orders
index/cache visibility and exact global MRU updates. Disk I/O, waiting for a
writer/read handle, and full-value copying do not happen under the state mutex.
BatchGet captures the Value LRU and index under the publication gate. Cold index lookups execute outside the state mutex. It returns results
in input order and preserves cache recency. BatchPut and BatchDelete publish
all index and cache updates under one state lock. Each separate API call has
its own linearization point.

Hits capture immutable cached bytes under state and return a copy outside it.
Misses pin an indexed location, perform positional checked I/O outside state,
then recheck health and version. A changed/deleted/reinserted key causes retry;
an unchanged version may safely have moved physically during compaction.
Failed or missing reads do not create cache entries. A source read failure
on protected sealed DATA can trigger verified online repair and a retry. A
confirmed local loss in sealed DATA fails the affected request and permits
other reads and writes to continue. Cache hits read memory and never
start an on-disk scrub.

For units, identifiers and lifetimes, see the [storage terminology](terminology.md).

## Commit and recovery

The segmented full-value log supplies the WAL/recovery role; there is no second
duplicate-value redo WAL. A backup is not a WAL.

For an ordinary mutation:

1. Reject invalid or oversized input. The caller keeps the input stable through return.
2. Under the writer gate, own the key and stage its index change at the intended DATA address. Append the record at that address.
3. Sync the data file.
4. Completely write and sync a private replacement `CURRENT`.
5. Rename it over `CURRENT` and sync the directory.
6. For a Put, copy the value for the Value LRU. Publish the prepared index and cache update
   under the state lock, then return nil. Writers waiting for the gate retain
   their callers' input slices.

This conservative protocol normally uses three sync barriers per mutation.
There is no asynchronous acknowledgment, disabled-sync setting, or group commit.
When an append would take a nonempty log past its target (4 MiB by default),
tarp writes the new segment header and first record, then syncs them together.
It writes and syncs the new catalog and syncs the directory before replacing
`CURRENT`. This one frontier publication selects both the new active log and
the committed record. Sealing for a full DATA checkpoint instead selects an empty
active log at the existing sequence. Sequences, generations and file IDs never wrap.
No record spans files or overwrites previously committed bytes.

A nonempty BatchPut or BatchDelete encodes consecutive ordinary records with
dense user sequences in a single segment. If the active file has insufficient
room, the writer starts a new segment containing the entire batch. A single
replacement CURRENT publishes its final sequence and offset, after the data
and any new catalog are synced. Memory publication follows durable publication.
An uncertain commit outcome applies to the entire batch. Reopen and backup
select either frontier and can never select an intermediate record boundary.
The prior whole-VM campaign described above predates the batch API.

An unknown outer signature in an existing `CURRENT` returns
`ErrUnsupportedFormat` before any repair, initialization or cleanup. For the
supported layout, recovery locks the directory and validates the selected `CURRENT`, its
SHA-256-bound catalog, each selected segment header and length, and all live
option minima. Full recovery verifies each sealed DATA digest and each committed
active record. A validated INDEX can supply the covered key history. Indexed
recovery skips the covered segments’ encoded records after validating INDEX and replays the
committed DATA log suffix. Both start with a cold value cache. Raw logs retain dense sequences. Cleaned intervals
preserve coverage even when they have zero records. Deletes through the catalog
pruning boundary may refer to a Put removed by cleaning. All selected physical
DATA files must exist. See the [replay rules](format.md#catalog-payload).
Missing required files and invalid checksums match `ErrCorrupt`.
Read failures such as permission or device errors preserve the filesystem
cause for `errors.Is` and `errors.As`.
Only after that validation may recovery truncate/sync the active suffix
strictly beyond the committed frontier or delete recognized unselected objects.

Full replay, `Restore`, and the validation pass of a manual full DATA checkpoint
read each selected segment sequentially in read-ahead chunks of up to 32 KiB.
In the active segment the committed end offset bounds read-ahead, even when
the physical file has a longer tail. The
decoder verifies each record's checksum before applying it to the rebuilt index.

An incomplete or checksum-invalid final **committed** record is corruption, not
a torn tail to skip. Never select an older catalog, promote temporary metadata,
or infer commitment from a plausible final CRC. Missing/truncated/extended
selected sealed files, unrecognized directory entries and nonregular engine files
fail explicitly.
At the database root, cleanup identifies obsolete DATA, catalogs and temporary
CURRENT files by their UUID-bound names.

A successful new `Open` includes durable directory/lock/catalog initialization.
The owner syncs the immediate parent even if another caller created the empty
directory, so existing-but-not-yet-durable directory creation is not overlooked.
A crash before first successful initialization can leave a nonempty directory
without `CURRENT`; this is an explicit incomplete/corrupt state, not permission
to erase/reinitialize it. Inspect the unused directory before manually removing
anything. A small-options reopen failure leaves existing data unchanged.

## Failure outcomes

Use `errors.Is` for category/underlying causes and `errors.As` for `*OpError`,
`*SizeError`, and `*CompatibilityError`. I/O paths preserve phase and cause;
`ErrNoSpace` includes ENOSPC and EDQUOT.

| Situation | Requested operation outcome | Instance |
| --- | --- | --- |
| Invalid/oversized input or missing delete | `NotApplied`; no file/value/cache-order change | Healthy |
| B+tree setup fails after `Open` publishes the first durable CURRENT | `Committed` for creation of the empty database | Fix the storage error and retry `OpenExisting` |
| Source append/sync or frontier preparation before rename fails | `NotApplied` logically; unreachable bytes may remain | Fail-stop |
| Frontier rename attempted or directory sync uncertain | `Unknown`, matching `ErrCommitUncertain` | Fail-stop |
| All commit syncs finish but another read detects source failure before memory publication | Known `Committed`, unhealthy error | Fail-stop |
| Correctable read/checksum failure in protected sealed DATA | Retry after verified repair | Healthy |
| Uncertain rename or directory sync during online DATA repair | The triggering operation reports `NotApplied`. The DATA replacement matches `ErrCommitUncertain` | Fail-stop |
| Confirmed sealed DATA damage exceeds available parity protection | `ErrDataUnavailable` with the DATA file ID and underlying cause | Remains open for reads and writes |
| Active DATA read failure, invalid live index, or unlocalized source failure | Explicit error including the source cause | Fail-stop |
| Backup or restore finds an existing destination, including an `EEXIST` from the no-replace rename | `NotApplied`, matching `fs.ErrExist` | Choose a new destination to retry |
| Other backup or restore destination rename failures, or a parent directory sync failure after rename | `Unknown`, matching `ErrCommitUncertain` for the destination | Inspect the destination before retry. A backup source can continue serving requests after a destination failure |
| Preliminary log rotation fails during `Compact` | `NotApplied` for the checkpoint, even if the rotation selected a new active log | An I/O failure during publication requires close/reopen |
| Unpublished checkpoint ENOSPC/EDQUOT | Selected sources retained; cleanup errors joined | May remain healthy |
| Cleanup after a durable checkpoint | Committed maintenance error | Valid logical image; retry cleanup |

Once unhealthy, newly admitted cached and uncached operations both fail with
`ErrUnhealthy` and the retained first cause. A read already linearized before
failure is not revoked. Close/reopen to inspect the authoritative files;
an uncertain operation may appear or not, and errors do not universally roll back.
Never blindly retry an unknown write assuming it did not occur.

Close stops new admission, cancels and joins its worker, waits for admitted
calls including writers and manual maintenance, closes handles and releases ownership last. Concurrent/repeated
calls return the same cached joined close/unhealthy result. `ErrClosed` takes
precedence over unhealthy for newly called methods. Close does not repair
uncertainty or supply the first durability barrier.

## Maintenance, capacity, and backup

On fresh Open and reopen, the worker first considers single-segment cleaning of selected sealed DATA segments.
Per-file live Put and required Delete byte counts estimate replacement size.
For a candidate with source bytes S and estimated bytes to write W (including
metadata), the initial eligibility rule is `W <= S/2`. This gives estimated
net savings of at least one third of read-plus-write bytes. The worker ranks
eligible files by highest savings per I/O byte, with stable tie breaks.

When no single segment qualifies, grouped log cleaning processes consecutive
sealed logs in catalog order with one catalog publication. It considers up to eight catalog entries,
including empty intervals. Each physical input must fit within one segment
target and retain at most half its bytes. The combined replacement DATA must
also fit within one target. The same savings threshold applies to the whole
group, charging metadata once and retaining all tombstones.

When neither cleaning policy qualifies, small-segment repacking looks for four to
eight small segments of the same family (snapshots or sealed logs) in catalog order. Each
may occupy up to one quarter of the effective segment target. Their combined
size including headers must fit within one target, and the largest must be
no larger than all the others combined. This avoids repeatedly rewriting a
larger file as a few tiny ones accumulate. An empty log interval between
inputs contributes its sequence coverage without counting as a physical file.
Fewer than four small files, or small files separated by a large one, can remain.

The worker checks on Open, after rotation and after writes invalidate sealed
records. It checks again after each completed pass. It validates and streams
its inputs one at a time, including obsolete records, and yields in bounded
chunks for queued writers and backup. Input selection and copying use the
sealed catalog, leaving the active log available for appends.

After copying, the worker reserves a manifest ID and builds the replacement
catalog under the writer gate. It releases the gate while writing and syncing
the unselected manifest. Under the gate again, it publishes against the latest
committed frontier. If a foreground rotation selected a different manifest,
the worker discards its staging manifest and rebuilds it once under the gate.
A repacked log segment covers its inputs' full sequence interval and retains their
Delete records. Each replacement DATA file and its matching PARITY are verified
and synced before catalog publication. The catalog reaches stable storage before the
temporary CURRENT is synced, renamed and directory-synced.
An uncertain publication fail-stops while the writer gate is still held and
retains both layouts for validated reopen. After durable publication, remapping
checks file, offset and sequence so later changes win. Reclamation waits for all
pins. No incremental publication advances the user sequence.

Manual `Compact(ctx)` builds a full DATA checkpoint. It durably captures the
complete DATA history through the checkpoint cutoff sequence `C`, validates
segment headers and records, and rebuilds the captured state outside root locks.
Without options, it groups live Put records by source file, reads them in offset
order with one source descriptor open at a time, and writes snapshot segments
using the same size target as log segments, preserving record sequences.
Tombstones can be dropped only because all earlier history is replaced.
Publication combines the checkpoint with the **entire** committed DATA log
suffix after `C`. Each relocation checks the exact prior address and publishes
the new address and its file accounting together.
For the in-memory index, a batch can move while a foreground write syncs if their
keys are disjoint. A batch that overlaps a staged key waits for the writer to
commit or abort, then checks the addresses again. The B+tree takes the writer gate
for each relocation batch. Later Put/Delete wins.

`db.Compact(ctx, tarp.WithKeyOrder())` uses the same checkpoint cutoff sequence,
atomic publication, conditional relocation and loss-recovery protocol. It writes
DATA records in encoded-key order across snapshot segments and can carry forward
previously verified immutable snapshot segments and their PARITY under the same
identities. It checks the new DATA log suffix and changed base ranges. Values in
reused files can still have latent damage until checked.
Order certification is local to the open handle. Backup and restore use the
existing DATA and catalog formats.

Old paths remain until they are unselected, have no index references, and have
no reader/backup/compaction pins. The compaction releases its own capture pins
before cleanup; successful unlinks also remove retired registry objects.
After an unlink, a failed directory sync stays pending. The next manual cleanup
or worker retirement pass retries it, even if there are no more files to remove.
Retired read descriptors close after their final read-handle lease is released. Pin release performs
no disk I/O. The worker revisits its pending retirements, and a full DATA checkpoint
and validated reopen can also clean eligible files. `DB.AutoCompactionStatus`
reports the worker's deferral reason and most recent error. Its
`PinnedRetiredDataBytes` records the logical bytes of retired DATA segments
retained by segment pins or live index references at the latest retirement check. Its `UnlinkedBytes` counts the logical file bytes
unlinked by the worker during this DB lifetime.

Provision space for current data, copied live records and tombstones, headers,
catalogs, backup files, pinned generations, concurrent tail growth and allocation
overhead. Free-space inspection is advisory. Insufficient scratch space defers
incremental work. Recoverable staging ENOSPC/EDQUOT leaves selected sources
intact. If discarding the cleaner's unpublished DATA file fails, the worker
retries the unlink and directory sync on a later retirement pass. Maintain
enough free space for concurrent foreground writes.

Online backup captures metadata bytes and exact committed prefixes at S under
the writer gate, pins them, and copies outside it. The active file can grow;
only its captured prefix is copied. Source CRCs are checked on copied bytes and
each output file is SHA-256 hashed. The unversioned `BACKUP` envelope records advisory
source settings. Restore rejects an unsupported outer signature before publishing
a destination. For supported backups it derives live compatibility minima independently.

Backup/restore use private sibling staging, file syncs, staging-directory sync,
atomic no-replace directory rename, and parent sync. Linux amd64/arm64 use
`renameat2(RENAME_NOREPLACE)`; development Darwin uses
`renameatx_np(RENAME_EXCL)`. Unsupported platform helpers fail explicitly.
There is no check-then-overwrite fallback. Require caller-controlled stable
parents and immutable backup sources.

VerifyBackup checks the exact file set, safe names, hashes, formats,
identities, record semantics and sequences of an immutable backup in place.
It reports the required record and cache limits. BackupInfo.CheckLimits
compares those requirements with a target configuration in memory.
Restore enforces the target limits before staging, then independently validates
the staged image and its size requirements before publishing a new closed
database. Existing destinations remain untouched. Uncertain output rename/parent sync is reported
explicitly and does not poison an otherwise healthy source DB.

## Sealed DATA protection and offline recovery

After rotation, a background worker reads the immutable DATA using a 64 KiB
stripe buffer. It builds parity and record anchors, checking the source against
the SHA-256 in the selected catalog. It reads back the staged sidecar to verify
its checksums and length before publication. Its DATA pin survives staging, file
sync, rename and directory sync. Healthy reads and ordinary commits never open
PARITY or wait for its sync. The worker briefly yields between stripe reads
during foreground writes or a backup. Background work still consumes CPU and
disk bandwidth. Restored databases build
missing sidecars on Open. Replaced DATA and its sidecar are retired together.
Compaction protects its private output before it becomes selected. File backups contain committed DATA bytes and the selected catalog with its sealed DATA digests.

Protection starts when the complete sidecar reaches disk. Active DATA and newly
sealed DATA waiting for the worker remain exposed to local media damage. Calling
`ProtectParity(ctx)` explicitly seals a nonempty active tail, validates existing
sidecars and builds missing or damaged ones from healthy DATA. It uses no index
checkpoint. Concurrent writes after the seal can create a new active tail. Healthy Close finishes
outstanding protection of sealed files. A staging space or write failure
can delay protection or be returned by explicit maintenance or Close. Earlier
successful commits retain their original durability contract.

### Automatic repair while serving reads

When a live read finds localized damage in a sealed DATA file, Tarp uses that
file's pinned catalog identity and PARITY sidecar. Concurrent reports for the
same file share a repair. Reconstruction uses a private file in the DB
workspace. Before replacement Tarp verifies the actual staged bytes against
the catalog-selected DATA digest and syncs the file. It publishes the replacement with a
rename and directory sync, and refreshes the pooled read descriptor before
retrying. This leaves the commit frontier, record addresses and sequences
unchanged.

`Get` rechecks concurrent changes to the key through its usual read path.
`BatchGet`, `Scan` and `ScanPrefix` keep their original snapshots. A pinned old segment
can be repaired after compaction while a snapshot still needs its bytes.
Backup, compaction and index checkpoint creation retry private work against
their captured input before publication. Recovery reads and staging do not
hold the DB writer gate. Other files remain available during reconstruction.

Automatic repair is triggered by an actual source read. A repair needs temporary
free space for the entire affected DATA file.

### Serving with unrecoverable sealed DATA

When Tarp confirms local DATA damage and cannot reconstruct the original
file, the request returns `ErrDataUnavailable`. This includes missing or invalid
parity, damage beyond its correction budget, and insufficient temporary space
when the unpublished repair can be safely discarded. The error retains the cause
and the file ID in `*UnavailableDataError`. The selected bytes remain in place.
For subsequent disk reads Tarp checks the affected file's header on the open
descriptor as well as each requested record. An intact record in the same file
can still be read when its file identity is valid. Repeated failures in one
file share a short retry delay to bound the cost of reconstruction.

The live index still identifies the current key versions. A failed read returns
an error for that version. Applications can overwrite or delete the key, and
new writes follow the ordinary commit protocol. A cache hit can return the
current value already held in memory. `BatchGet` keeps its all-or-error result
contract. `Scan` and `ScanPrefix` end with a terminal error if a record they
read is unavailable. `Keys` and `KeysPrefix` can still enumerate the index
without reading DATA. Individual `Get` calls during that enumeration see the
version current at each call.

Backup and index checkpoint creation report an error while their captured input
contains unavailable DATA. A successful recovery checkpoint lets subsequent
backups use the new, validated layout. Protection of other sealed files continues.
`Close` reports known unavailable DATA still in the selected layout and releases the directory lock. Writes
acknowledged during this period retain their ordinary durability contract.

The guarantee of continued service applies to an open DB with a trusted index
and commit frontier. `Full` startup validation still rejects damaged selected
DATA. `Indexed` recovery can reopen when a validated checkpoint covers the
damaged record bytes, covered segment headers and lengths validate, and the remaining log
can be replayed. An uncertain publication, a failure of the active DATA or an
untrusted index retains its existing fail-stop behavior.

### Recovering records during compaction

Manual and automatic compaction first attempt ordinary repair of the original
file. When a valid sidecar confirms multiple lost blocks in a stripe, the
automatic worker can run a full DATA checkpoint even if the damaged file would
normally be too small or too live to clean. Replacing all history through
the cutoff lets Tarp discard a lost current value without exposing an older Put.
If the current sequence is already checkpointed and there is no known DATA damage,
manual compaction only retries cleanup. It does not scrub the snapshot.

The checkpoint uses the selected file identity, catalog digest, sidecar block
checksums and record anchors. Each stripe checksum includes the selected DATA
identity, so copying an intact stripe from another sidecar fails validation.
The catalog supplies the canonical segment header, so damage limited to that
header can be corrected before applying XOR to another block in the stripe.
Tarp reconstructs correctable stripes, validates
records individually in damaged stripes and preserves verified live records
from the rest of the file. Raw records in a damaged block require a matching
original segment header. Tarp can use a matching live cache value when the
record header confirms the expected payload checksum. Obsolete damaged records
do not count as lost keys. A missing or invalid sidecar, a temporary staging
failure, or an unverified source error does not establish record loss. If a
damaged key could belong to an ambiguous update and the captured index cannot
resolve ownership, compaction returns `ErrDataUnavailable` and keeps serving.

At publication, concurrent Put and Delete operations keep their normal ordering.
A key dropped by the committed checkpoint is absent from Get, Keys, backups and
subsequent Full or Indexed opens. Put can create it again. Manual
`CompactResult.DroppedKeys` counts live keys discarded by the committed
checkpoint. `AutoCompactionStatus.DroppedKeys` accumulates the corresponding
automatic count since Open. File reclamation waits for pins on the old layout,
including scans or backups that began before recovery.

### Offline repair and salvage

`RepairDataFromParity(ctx, dir)` takes the database lock without opening the DB for
queries. Each selected sealed DATA is checked against its catalog length and hash.
For a damaged file it checks the recovery material, reconstructs one bad block per
stripe, verifies the private candidate against the original SHA-256, syncs it,
replaces the DATA atomically and syncs the directory. Files that exceed the
correction budget retain their original bytes. Independent repairable files can
still be restored in the same run. Inspect a returned error even when some files
were repaired. Retry an uncertain publication under the directory lock.
Successful completion includes validation of the committed database. Keep a
backup on independent storage for loss of the volume or the commit metadata.

For an unrepairable sealed file with a valid sidecar, `SalvageData` exports
checked historical records in ascending segment offset order. It reconstructs singly damaged
stripes, drops records touching unreadable blocks, then resumes at a verified
record start. Reported byte ranges identify the remaining gaps. A missing region
can hold a newer Put, Delete or another member of a batch. The callback can also
receive superseded values. Keep this output separate from a live DB and preserve
the error and gap report with the exported records. Stage external output until
the final result is known. The report uses one range per disjoint damaged area.

## Index checkpoints

`CheckpointIndex` seals a nonempty active log under the writer gate and pins
the captured DATA. It shares the maintenance slot with compaction. Foreground
writes and rotation can proceed while a private B+tree is bulk-built from the
validated record stream. After syncing the staging file, the publisher renames the INDEX and syncs
the database directory. Under the gate, it checks the covered catalog prefix against the latest
catalog. A new MANIFEST and CURRENT then select it
together with the latest committed tail. Cleaning and compaction invalidate
the descriptor when replacing its physical layout. Sealed DATA digests also
protect maintenance reads of segments opened with Indexed validation.

A failure building the optional index leaves existing acknowledged writes
committed. A failure during selector rename or directory sync has an unknown
publication outcome and requires reopening. Obsolete files are retired after
publication. A healthy B+tree Close waits for admitted operations and the
cleaner, refreshes the checkpoint if needed, and releases the lock even on
error. Memory handles checkpoint explicitly.

## Runtime index and scratch space

The backend is selected per Open. A B+tree stores pointer-free DATA addresses
in process-private scratch files. The database reserves `.tarp-work/` for
B+tree pages, replay sort runs, and indexes used by compaction and online
backup. Compaction also uses this directory when its relocation list spills
in either index mode. These files share the database volume.

Tarp unlinks private pager and replay files after opening them. The synced INDEX
staging file retains a temporary name until publication. A process exit between
creation and unlink can leave a name behind. On reopen, Tarp validates the
selected DATA and option limits before removing recognized leftover scratch
files. On close, it also removes the empty workspace before releasing the
database lock. Both operations check every workspace entry before removing any.
Unexpected entries, including symlinks, cause `ErrCorrupt` and are preserved.

Committed DATA determines the state after a crash. Before writing a single
mutation or atomic batch, Tarp stages the new locations in a private index
transaction. The existing DATA and CURRENT syncs
then establish the commit. Publication switches the index, cache and counters
together. A failure preparing scratch leaves the selected frontier intact.
After an uncertain durable publication, reopen to recover the selected state.

During full B+tree replay, the builder sorts bounded runs by exact binary key.
It validates each key’s transitions in the order in which its records were
replayed, then packs its final live address into the tree from the leaves up.
Backup uses the same reducer to validate its captured history.

`VerifyBackup` verifies the immutable directory with bounded key windows. A
large backup can take multiple sequential DATA passes. Use
`VerifyBackupWithScratch(ctx, source, scratchDir)` to sort the key history in
bounded runs using an existing external directory. The reducer verifies that
history and computes the live-record limits directly. Restore validates both
its source and the staged copy before the destination rename. If the source
exceeds the first key window, the stage starts its own replay with fresh scratch.
