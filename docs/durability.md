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
No multi-key transaction or atomic multi-call read is implied.

Hits capture immutable cached bytes under state and return a copy outside it.
Misses pin an indexed location, perform positional checked I/O outside state,
then recheck health and version. A changed/deleted/reinserted key causes retry;
an unchanged version may safely have moved physically during compaction.
Failed/missing reads do not create cache entries. Source failures are surfaced,
not hidden behind a cache-dependent fallback. A cache hit is not a continuous
on-disk corruption scrub; fail-stop begins when a failure is detected.

## Commit and recovery

The segmented full-value log supplies the WAL/recovery role; there is no second
duplicate-value redo WAL. A backup is not a WAL.

For an ordinary mutation:

1. Reject invalid or oversized input. The caller keeps the input stable through return.
2. Under the writer gate, append a whole record at the known active end.
3. Sync the data file.
4. Completely write and sync a private replacement `CURRENT`.
5. Rename it over `CURRENT` and sync the directory.
6. For a Put, copy the key and value for the index and cache. Publish the update
   under the state lock, then return nil. Writers waiting for the gate retain
   their callers' input slices.

This conservative protocol normally uses three sync barriers per mutation.
There is no asynchronous acknowledgment, disabled-sync setting, or group commit.
When an append would take a nonempty log past its target (4 MiB by default),
tarp writes the new segment header and first record, then syncs them together.
It writes and syncs the new catalog and syncs the directory before replacing
`CURRENT`. This one frontier publication selects both the new active log and
the committed record. Explicit full-checkpoint sealing instead selects an empty
active log at the existing sequence. Sequences, generations and file IDs never wrap.
No record spans files or overwrites previously committed bytes.

An unknown outer signature in an existing `CURRENT` returns
`ErrUnsupportedFormat` before any repair, initialization or cleanup. For the
supported layout, recovery locks the directory and validates the selected `CURRENT`, its
SHA-256-bound catalog, every selected segment header/identity, every committed
record/checksum/sequence, and all live option minima. It rebuilds the full index
with a cold value cache. Raw logs retain dense sequences. Cleaned intervals
preserve coverage even when they have zero records. Deletes through the catalog
pruning boundary may refer to a Put removed by cleaning. All selected physical
files must exist. See the [replay rules](format.md#catalog-payload).
Only after that validation may recovery truncate/sync the active suffix
strictly beyond the committed frontier or delete recognized unselected objects.

An incomplete or checksum-invalid final **committed** record is corruption, not
a torn tail to skip. Never select an older catalog, promote temporary metadata,
or infer commitment from a plausible final CRC. Missing/truncated/extended
selected sealed files and unknown/nonregular engine objects fail explicitly.
Only recognized UUID-bound private names can be garbage-collected.

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
| Source append/sync or frontier preparation before rename fails | `NotApplied` logically; unreachable bytes may remain | Fail-stop |
| Frontier rename attempted or directory sync uncertain | `Unknown`, matching `ErrCommitUncertain` | Fail-stop |
| All commit syncs finish but another read detects source failure before memory publication | Known `Committed`, unhealthy error | Fail-stop |
| Source read/checksum failure | Explicit original error, no cache fallback | Fail-stop |
| Backup destination failure | Output publication status only; not source rollback | Source may remain healthy |
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

On fresh Open and reopen one worker considers the selected immutable segments.
Per-file live Put and required Delete byte counts estimate replacement size.
For a candidate with source bytes S and estimated bytes to write W (including
metadata), the initial eligibility rule is `W <= S/2`. This gives estimated
net savings of at least one third of read-plus-write bytes. The worker ranks
eligible files by highest savings per I/O byte, with stable tie breaks.

When no single file qualifies, the worker looks in catalog order for 4–8
adjacent physical files of the same family (snapshots or sealed logs). Each
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

After copying, the worker takes the writer gate and rebases one replacement on
the latest catalog and frontier. A repacked log covers the full sequence
interval of its inputs and retains their Delete records. Replacement data
and catalog reach stable storage before the temporary CURRENT is synced,
renamed and directory-synced.
An uncertain publication fail-stops while the writer gate is still held and
retains both layouts for validated reopen. After durable publication, remapping
checks file, offset and sequence so later changes win. Reclamation waits for all
pins. No incremental publication advances the user sequence.

Manual `Compact(ctx)` durably captures a complete prefix at cutoff C, validates its segment
headers and records, and rebuilds cutoff state outside root locks. It groups live
Put records by source file, reads them in offset order with one source descriptor
open at a time, and writes snapshot files using the same size target as log appends,
preserving record sequences. Tombstones can
be dropped only because all earlier history is replaced. Publication combines the
checkpoint with **all** current tail files after C, then remaps each still-unchanged
index location under short state locks. Later Put/Delete wins.

Old paths remain until they are unselected, have no index references, and have
no reader/backup/compaction pins. The compaction releases its own capture pins
before cleanup; successful unlinks also remove retired registry objects.
Leased read descriptors close after their final release. Pin release performs
no disk I/O. The worker revisits its pending retirements, and full checkpoint
and validated reopen can also clean eligible files. `DB.MaintenanceStatus`
reports the worker's deferral reason, most recent error, pinned retired byte
count and logical bytes unlinked by the worker during this DB lifetime.

Provision space for current data, copied live records and tombstones, headers,
catalogs, backup files, pinned generations, concurrent tail growth and allocation
overhead. Free-space inspection is advisory. Insufficient scratch space defers
incremental work. Recoverable staging ENOSPC/EDQUOT leaves selected sources
intact. Maintain enough free space for concurrent foreground writes.

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

Restore checks exact file set, safe names, hashes, formats, identities, record
semantics, sequences, and smaller-limit compatibility; it then independently
validates its staged image before publishing a new closed database. Existing
destinations remain untouched. Uncertain output rename/parent sync is reported
explicitly and does not poison an otherwise healthy source DB.
