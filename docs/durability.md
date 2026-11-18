# Durability, concurrency, and operational contract

## Support boundary

There is **no completed production power-loss qualification yet**. The first
target is Linux amd64, a recorded Ubuntu 24.04 LTS 6.8-series kernel, and local
ext4 with `data=ordered`, normal journaling/flush barriers, no DAX, and an
honest-flush device/controller stack. Record exact kernel builds, ext4 features,
mount options, block-device/cache configuration, Go version, and controller
behavior before claiming that target qualified.

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

The importable root package owns public contracts, lifecycle admission, the
visible index/cache, writer coordination, and maintenance orchestration.
`internal/storage` owns file protocols/replay/leases; `internal/format` owns
binary codecs; `internal/cache` owns exact global LRU; `internal/platform` owns
small Linux/Darwin primitives. Root imports those leaves, storage imports only
format/platform, and the other internal packages import only the standard
library. No internal package imports the root.

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

1. Reject invalid/oversized input and take owned copies.
2. Under the writer gate, append a whole record at the known active end.
3. Sync the data file.
4. Completely write and sync a private replacement `CURRENT`.
5. Rename it over `CURRENT` and sync the directory.
6. Publish the index/cache update under state, then return nil.

This conservative protocol normally uses three sync barriers per mutation.
There is no asynchronous acknowledgment, disabled-sync setting, or group commit.
Rotation first durably creates/syncs a header-only segment and immutable catalog,
syncs names, and publishes a new frontier with unchanged user sequence, then
appends the pending record. Sequences, generations, and file IDs never wrap.
No record spans files or overwrites previously committed bytes.

Recovery locks the directory and validates the exact selected `CURRENT`, its
SHA-256-bound catalog, every selected segment header/identity, every committed
record/checksum/sequence, and all live option minima. It rebuilds the full index
with a cold value cache. Only afterward may it truncate/sync the active suffix
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

Close stops new admission, waits for all admitted calls including queued writers
and maintenance, closes handles, and releases ownership last. Concurrent/repeated
calls return the same cached joined close/unhealthy result. `ErrClosed` takes
precedence over unhealthy for newly called methods. Close does not repair
uncertainty or supply the first durability barrier.

## Maintenance, capacity, and backup

Compaction durably captures a complete prefix at cutoff C, validates its segment
headers and records, and rebuilds cutoff state outside root locks. It streams live
Put records into bounded snapshot files, preserving versions. Tombstones can
be dropped only because all earlier history is replaced. Publication combines the
checkpoint with **all** current tail files after C, then remaps each still-unchanged
index location under short state locks. Later Put/Delete wins.

Old paths remain until they are unselected, have no index references, and have
no reader/backup/compaction pins. The compaction releases its own capture pins
before cleanup; successful unlinks also remove retired registry objects.
Leased read descriptors are never evicted/closed. Releasing pins does no I/O;
later Compact/Open performs eligible cleanup. Cleanup errors do not claim a net
free-space gain or silently discard evidence.

There is no total quota or automatic background scheduler. Provision current
data plus copied live records/headers/catalogs, backup files, pinned generations,
concurrent tail growth, and filesystem allocation overhead. Free-space inspection
is advisory, not reserved capacity. If safe progress lacks headroom, free space
outside the engine or add capacity; never delete selected live data.

Online backup captures metadata bytes and exact committed prefixes at S under
the writer gate, pins them, and copies outside it. The active file can grow;
only its captured prefix is copied. Source CRCs are checked on copied bytes and
each output file is SHA-256 hashed. A versioned `BACKUP` manifest records advisory
source settings; restore derives actual live compatibility minima independently.

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
