# tarp

A standalone, file-backed Go key-value library. The published module,
tests, and measurement tools use the Go standard library only.

## Quick start

Use Go 1.26.0 or later on Linux or macOS (amd64 or arm64). This quick start
uses v0.10.0. For the previous release, see the
[v0.9.1 documentation](https://github.com/anaregdesign/tarp/tree/v0.9.1).

```sh
mkdir tarp-example
cd tarp-example
go mod init example.com/tarp-example
go get github.com/anaregdesign/tarp@v0.10.0
```

Save this as `main.go` to write a key to the local `store` directory and read it back.

```go
package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/anaregdesign/tarp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	db, err := tarp.Open("store", tarp.Options{})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, db.Close()) }()

	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		return err
	}
	value, err := db.Get([]byte("key"))
	if err != nil {
		return err
	}
	fmt.Println(string(value))
	return nil
}
```

Run the example. It prints `value` and closes the database.

```sh
go run .
```

## Storage and compatibility

The parent directory must exist. One process exclusively owns a directory, while
its `DB` supports concurrent goroutines without caller locks. Individual key operations are linearizable. Each batch call operates on its
keys atomically. Separate calls have their own publication or read points.

Use `OpenExisting(ctx, dir, opts)` when a missing database is an error, for
example when opening a restored database or checking a service's configured
path. An absent or empty directory matches `fs.ErrNotExist`. Use
`OpenContext(ctx, dir, opts)` for cancellable recovery with the usual
create-or-open behavior. After either function returns a DB, its lifetime is
controlled by `Close`. For typed access, use `Bind` on that DB.

**Every successful `Open` starts automatic incremental compaction.** Both new
and reopened stores use one unversioned storage and backup format. A database
or backup from the previous implementation returns `ErrUnsupportedFormat`.
Opening it leaves the files untouched and restoring it leaves the destination
unpublished. There is no migration path in this package.

The historical Linux/ext4 power-cut results and performance measurements cover
the earlier storage implementation. Evidence for the current format and cleaner
must be collected on their own revision. See the [durability contract](docs/durability.md),
[qualification record](docs/qualification.md) and [historical baseline](docs/measurement-baseline.md).

## Typed keys and values

`OpenTyped` infers the key and value types from the supplied codecs. For
example, this function stores a string key and returns an `int64` value.

```go
func storeScore(dir string) (score int64, err error) {
	db, err := tarp.OpenTyped(dir, tarp.Options{}, tarp.StringCodec{}, tarp.Int64Codec{})
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	if err := db.Put("score", 42); err != nil {
		return 0, err
	}
	return db.Get("score")
}
```

`OpenTyped` returns an owning `*TypedDB`, with the same backup, maintenance
and close operations as `*DB`. To lend an open DB to another component, use
`Bind`. The returned `*View[K,V]` can read, write, scan and batch the same
records. The opener controls the shared database lifetime.
An owner created with `OpenTyped` can lend its existing codecs with `db.View()`.

```go
scores, err := tarp.Bind(raw, tarp.StringCodec{}, tarp.Int64Codec{})
if err != nil {
    return err
}
return scores.Put("score", 42)
```

When upgrading from v0.4, replace `NewTypedDB(raw, ...)` with `Bind(raw, ...)`
and call `Close`, `Stats`, `Backup` and maintenance methods on the owner.
Existing v0.4 and v0.5 databases and backups use the same on-disk format as v0.6.

Built-in codecs cover `bool`, `string`, all integer, float and complex types,
and `[]byte`. Use `Uint8Codec` for `byte` and `Int32Codec` for `rune`. Numeric
encodings are fixed-width and big-endian, with eight bytes for `int`, `uint`
and `uintptr`. String and byte-slice codecs store their bytes verbatim, so an
empty string or slice used as a key returns `ErrEmptyKey`.

`TimeCodec` stores a `time.Time` instant with nanosecond precision and decodes
it in UTC. The original location and the process-local monotonic reading are
discarded. `DurationCodec` stores a `time.Duration` as signed 64-bit nanoseconds.

### Deriving a codec from a Go type

`NewCodec[T]()` composes the scalar codecs for slices, arrays, maps and structs
with exported fields. For example, this opens a DB of jobs keyed by name.

```go
type Job struct {
	Started time.Time
	Retries []time.Duration
	Labels  map[string]string
}

func openJobs(dir string) (*tarp.TypedDB[string, Job], error) {
	jobs, err := tarp.NewCodec[Job]()
	if err != nil {
		return nil, err
	}
	return tarp.OpenTyped(dir, tarp.Options{}, tarp.StringCodec{}, jobs)
}
```

Map entries are ordered by encoded key bytes. Encoding fails if distinct keys
would collide on disk or after decoding. Slices and maps preserve the difference
between nil and empty. This includes `NewCodec[[]byte]()`, which adds a presence
byte. Use `BytesCodec{}` when reading or writing verbatim byte slices.

Each struct field, array or slice element, and map key or value is framed by
its byte length as a minimal unsigned varint. A slice or map starts with a
presence byte (0 for nil, 1 for present). Byte slices carry their raw bytes
immediately after that marker.

The struct schema is positional. Fields are encoded in declaration order, and
field names and tags are omitted. Changing field order, adding a field or
changing its encoding requires migrating existing records or supplying a codec
that reads the older schema. `NewCodec` rejects pointers, interfaces, recursive
types and structs with unexported fields when it constructs the codec.

For a custom representation, implement `Codec[T]` with `Encode(T) ([]byte, error)`
and `Decode([]byte) (T, error)`. Use compatible codecs on reopen and after
`Restore`. See the runnable [TypedDB example](typed_db_test.go) and
[composite codec example](composite_codec_test.go).

## Batch operations

Use `BatchPut` to commit related values together and `BatchGet` to read a
consistent view of just the keys you need. For example, given an open
`*tarp.TypedDB[string, string]`:

```go
err := db.BatchPut([]tarp.TypedEntry[string, string]{
    {Key: "job:42:state", Value: "done"},
    {Key: "job:42:result", Value: "report.pdf"},
})
if err != nil {
    return err
}
keys := []string{"job:42:state", "job:42:result"}
results, err := db.BatchGet(keys)
if err != nil {
    return err
}
for i, r := range results {
    if r.Found {
        fmt.Println(keys[i], r.Value)
    }
}
return db.BatchDelete(keys)
```

The byte API takes `[]tarp.Entry` for writes and `[][]byte` for reads and
deletes. Results follow input order. A missing key has `Found == false`,
and the caller owns a separate value slice for each occurrence of a found
key. One `BatchGet` can return up to `Options.MaxBatchGetValueBytes` encoded value
bytes (64 MiB by default). Each occurrence counts, even when the key is read
from DATA only once. Empty values and missing keys cost zero bytes. Result
metadata and allocations inside a custom decoder are outside this limit.
Repeated puts apply in input order. Repeated deletes
remove a key once. If any requested delete key is missing, the whole call
returns an error matching `ErrNotFound` before writing. For an empty key in
any batch or a missing key in `BatchDelete`, use `errors.As` with
`*tarp.BatchKeyError`. Its `Index` gives the first occurrence of the offending
encoded key in the original input. For example, deleting `[a, a, missing]`
reports index 2 when `a` exists.

`BatchGet` preserves LRU recency and reads each distinct cold, live key once.
Concurrent `BatchGet` calls on the same DB, including typed views, share the
DATA read limit set by `Options.BatchGetReadConcurrency`. The default is 8.
For example, `tarp.Options{BatchGetReadConcurrency: 16}` allows up to 16 reads,
and a value of 1 serializes them. When reads of small records complete quickly,
the caller can read them sequentially. If reads slow down, the batch can use available workers
for the remaining records. A fully cached batch uses no read slots or workers. `Get` runs
independently of this limit, and each DB has its own read slots.

Each call accepts up to `tarp.MaxBatchEntries` (4,096) inputs, counting
duplicates. An empty batch succeeds on a healthy open DB. Write batches
must fit a single segment. The sum of encoded record sizes must fit the
effective `TargetSegmentBytes` minus the 64-byte segment header.
All put entries count, including repeated keys. For deletes, only distinct
keys consume record bytes. Exceeding a limit returns `ErrTooLarge`
before publication. Each nonempty write batch has one commit outcome.
Use `errors.As` with `*tarp.BatchLimitError` to distinguish input count,
per-entry size, total encoded write payload and total encoded value bytes returned
by `BatchGet`. Its `Index` is the original input position, or -1 for a
request-wide count error. For count, payload and result limits,
`Actual` and `Limit` report inputs or bytes respectively. A per-entry error
also exposes the underlying `*tarp.SizeError` with its record and cache charges.
Use `errors.As` with `*tarp.BatchCodecError` to get the zero-based input
index and the field (`key` or `value`) of a failed codec call. The error
also wraps the codec's cause. Typed batches encode keys in input order and
stop as soon as one encodes to zero bytes. `BatchPut` checks each encoded key
before calling the value codec for that entry.
See the [runnable batch example](typed_batch_test.go).

## Contract

`DB` provides `Get`, `Scan`, `Keys`, upserting `Put`, `Delete`, `Close`, online `Backup`,
`BatchGet`, `BatchPut`, `BatchDelete`, explicit online `Compact`, and `AutoCompactionStatus`. Use `Open` to create or
reopen a database and `Restore` to restore a backup offline.

- Keys and values are `[]byte`. Empty keys are rejected; empty values are valid.
  Missing `Get` and `Delete` return `ErrNotFound`.
- Retained inputs and returned values are copied. Do not mutate input backing
  arrays until every call using them has returned, including failed writes.
- `Get` uses an exact global LRU first. Misses read a checked indexed file record
  and cache it; updates/deletes cannot race a stale miss into visibility.
- If `Get`, `BatchGet` or `Scan` encounters a DATA read error, the error
  identifies the segment filename and the record's starting byte offset.
  Use `errors.Is` or `errors.As` to inspect the underlying cause.
- Successful writes include data, commit-frontier, and directory syncs before
  memory publication. Use `errors.As` to inspect the operation, phase and
  outcome in `*OpError`. After an uncertain database write, close and reopen
  the DB and inspect its contents before deciding whether to retry.
  `ErrUnhealthy` also requires close/reopen.
- Oversized single `Put` inputs return `ErrTooLarge` before changing files,
  values, or cache order. Ordinary fullness evicts LRU.
- `Close` rejects new calls, cancels and joins the worker, and waits for admitted calls. Repeated/concurrent
  closes return the same retained result. It is not the first durability barrier.

Use `for key, err := range db.Keys(ctx)` to list the live keys. Each iteration
captures its own index snapshot in unspecified order and returns caller-owned
keys. Keys reads no DATA files. Memory mode captures a copy of the live index.
B+tree mode captures a root and visits index pages on demand.

`for entry, err := range db.Scan(ctx)` reads a point-in-time snapshot in
unspecified order. Each iteration protects the DATA files referenced by the
captured index and reads one value at a time, preserving LRU recency. The
caller owns each key and value. A paused scan can retain old files on disk.
Break to release the snapshot early. When using `iter.Pull2`, defer `stop()`.
After an error, prior entries remain valid. Calling `Close` in the loop is
supported. A subsequent read yields `ErrClosed`.

No ordered scans, TTL, general read/write transactions, cross-file
records, or total disk quota is provided.

## Choosing the key index

`Options{}` uses an in-memory hash index for fast random lookups.
For datasets whose keys would consume too much heap, choose the disk index
when opening the database.

```go
db, err := tarp.Open(dir, tarp.Options{
	IndexMode:              tarp.IndexBTree,
	BTreeMemoryBudgetBytes: 16 << 20,
})
```

`IndexBTree` stores encoded keys and DATA addresses in private 8 KiB pages.
Tarp creates the backing files in `.tarp-work/` inside the database directory
passed to `Open` or `OpenExisting`. Size the database volume for DATA, index
pages, the persistent checkpoint, and temporary work during replay, compaction and online backup. Tarp
unlinks private pager files immediately after opening them. The OS releases its
space when its final descriptor closes.

The root and frequently used routing pages get priority in the cache. Value
LRU hits bypass the index. On a Value miss, cold routing, leaf and overflow
pages can add reads before loading DATA.

The configured budget is split between the foreground index (one half),
compaction and backup (one quarter each). It limits page-cache and index work
capacity. Decoded keys, request/iterator buffers, the file registry, Go's
allocator and the OS page cache also consume memory. `ValueCacheBudgetBytes`
limits entries kept in the value LRU. `MaxBatchGetValueBytes` limits encoded
value bytes returned by one batch read. Long-running readers can
hold previous page generations and DATA files on disk.

The checkpoint API introduced in v0.10.0 uses a newer storage format than v0.9.1.
Both index modes can reuse the same immutable `INDEX` file. A healthy B+tree handle
refreshes a missing or outdated checkpoint at `Close`. To checkpoint a
long-running DB or an in-memory index, call `db.CheckpointIndex(ctx)`. It seals
the active log and verifies and packs the committed prefix. Writes can continue
during the build. Size the volume for both the build workspace and the new INDEX,
and allow for the extra Close time. Checkpoint creation leaves the normal
per-write sync protocol intact.

| `Options.OpenValidation` | Work done during Open |
| --- | --- |
| `tarp.Full` (default) | Verify every committed DATA byte. Import the packed index when available and apply later commits. |
| `tarp.Indexed` | Verify the INDEX and selected DATA headers, then read and validate only the records after the checkpoint. |

`Indexed` can defer detection of disk damage. A value read verifies the record's
identity and CRC. `Has` and `Keys` can complete without reading its value. Reopen
with `Full` to check the complete selected history, including obsolete values.
Both modes rebuild from DATA when INDEX is missing or unusable. Compaction
invalidates indexes tied to the superseded files. Backups omit INDEX and remain
independently verifiable. See the [format](docs/format.md) and the
[checkpoint measurements](docs/benchmarks/index-checkpoints.md) and
[runtime index measurements](docs/benchmarks/index-modes.md).

## Limits

Each zero-valued field in `Options{}` selects its default. Negative or
inconsistent settings return `ErrInvalidOptions`. Normal writes always use
the commit sync protocol.

| Option | Default | What the limit applies to |
| --- | ---: | --- |
| `OpenValidation` | `Full` | Choose eager DATA verification or `Indexed` checkpoint recovery. |
| `IndexMode` | `IndexMemory` | Selects a built-in key index for this Open. |
| `BTreeMemoryBudgetBytes` | 16 MiB for `IndexBTree` | B+tree workspace budget, minimum 1 MiB. Leave zero for `IndexMemory`. |
| `ValueCacheBudgetBytes` | 64 MiB | Total logical charge of keys, values and entry metadata in the value LRU. A Put must fit as a single entry. |
| `MaxRecordBytes` | 1 MiB | One encoded record, including its 48-byte header, key and value. |
| `TargetSegmentBytes` | 4 MiB | Target size for new logs and checkpoint snapshots. Bounds the total input to a small-file repack and the output of grouped log cleaning. |
| `ReadFileHandles` | 64 | Maximum number of reusable DATA handles for cache misses. The DB can store more files than it keeps open. |
| `BatchGetReadConcurrency` | 8 | Maximum simultaneous DATA record reads across all `BatchGet` calls on this DB. |
| `MaxBatchGetValueBytes` | 64 MiB | Maximum total encoded value bytes in one BatchGet, including duplicate inputs. |

For a key of `K` bytes and a value of `V` bytes, a Put costs `48 + K + V` record
bytes and `128 + K + V` cache bytes. An entry that exceeds either limit returns
`ErrTooLarge`. For example, the default record limit permits a 1,048,512-byte
value with a 16-byte key. When several individually admissible entries fill the
cache, the least recently used entries are evicted. The selected index, Go
runtime overhead, in-flight operations and OS page cache use additional memory.

The effective target is at least `64 + MaxRecordBytes` so a whole record fits.
An exact fit stays in the current file. If the next append would exceed the
target, tarp seals the log and writes the record to a new file. The background
worker can reclaim obsolete records or combine small sealed files.
Manual `Compact` uses the same target to split its snapshot output.

For example, 32 files of 4 MiB total 128 MiB. The next write can create a
33rd file. Available disk space and filesystem quotas govern total capacity.
Backup and compaction also need temporary space.

Lowering the target still allows `Open` and `Restore` to read larger existing
files. If a live entry exceeds the new cache or record limit, they return
`ErrIncompatibleOptions`. Minimum settings are 129 cache bytes, 49 record
bytes and one read handle.

### Upgrading from v0.9.1 to v0.10.0

v0.10.0 changes the database and backup formats. To keep existing data,
export the live keys and values with v0.9.1, write them into a fresh v0.10.0
database, and verify the imported data before switching directories. Keep the
v0.9.1 directory and its backups available until verification is complete.

### Upgrading from v0.9.0 to v0.9.1

Existing Go callers, databases and backups are compatible. If you provisioned
space in the OS temporary directory for the B+tree, allow that space on the
database volume before upgrading. The index is rebuilt under `.tarp-work/`
when the database opens.

### Updating Go callers to v0.9.0

When updating from v0.8.0 to v0.9.0, use the names below. You can reopen
v0.8.0 databases and restore v0.8.0 backups with the same limits.

| Previously | Now |
| --- | --- |
| `Options.IndexMemoryBytes` (introduced on `main` after v0.8.0) | `Options.BTreeMemoryBudgetBytes` |
| `CacheBytes` on `Options`, `RestoreLimits`, and `SizeError` | `ValueCacheBudgetBytes` |
| `CompatibilityError.RequiredCacheBytes` | `CompatibilityError.RequiredValueCacheBudgetBytes` |
| `SizeError.CacheCharge` | `SizeError.ValueCacheChargeBytes` |
| `Stats.Cache` of type `CacheStats` | `Stats.ValueCache` of type `ValueCacheStats` |
| `Stats.MissingKeys` | `Stats.MissingKeyLookups` |
| `Options.BatchReadConcurrency` | `Options.BatchGetReadConcurrency` |
| `Options.MaxBatchResultBytes` | `Options.MaxBatchGetValueBytes` |
| `BatchResultBytesLimit` | `BatchGetValueBytesLimit` |
| `MaintenanceStatus` type and method | `AutoCompactionStatus` |
| `CompactResult.ReclaimedBytes` | `CompactResult.UnlinkedBytes` |
| `PinnedBytes` on `CompactResult` and the worker status | `PinnedRetiredDataBytes` |

## Backup, restore, and maintenance

```go
if err := db.Backup(ctx, backupDir); err != nil {
    return err
}
return tarp.Restore(ctx, tarp.RestoreRequest{
    Source:      backupDir,
    Destination: restoredDir,
})
```

`RestoreRequest.Limits` defaults to the same cache and record limits as
`Open`. To migrate a v0.4 call that passed `opts`, set
`Limits: opts.RestoreLimits()`. Configure handles, batch read concurrency
and the segment target when opening the restored DB.

To inspect an immutable backup, use the metadata returned by its verification.

```go
info, err := tarp.VerifyBackup(ctx, backupDir)
if err != nil {
    return err
}
fmt.Printf("database %x, sequence %d, live keys %d\n", info.DatabaseID, info.Sequence, info.LiveKeys)
if err := info.CheckLimits(opts.RestoreLimits()); err != nil {
    return err
}
```

`VerifyBackup` checks the exact file set and digests and replays the selected
records. It writes no files and can make multiple passes over large backups.
For a single DATA replay using temporary disk space, call
`VerifyBackupWithScratch(ctx, backupDir, scratchDir)` with an existing
directory outside the backup. `info.RequiredLimits` reports the largest logical cache charge and
encoded record among the live entries. Both
are zero for an empty backup. A verification error returns a zero report.
`info.CheckLimits` compares these sizes with a target configuration in memory.
`Restore` enforces the target limits and independently verifies the copied
database before publishing it.
Pass the restored directory to `OpenExisting`. Passing the backup directory
itself to `Open` or `OpenExisting` returns an error matching
`tarp.ErrBackupDirectory`. Use `VerifyBackup` to check its integrity.
When a required file cannot be read because of permissions, use
`errors.Is(err, fs.ErrPermission)`. A missing required file or a checksum
mismatch in the selected image matches `tarp.ErrCorrupt`.

If another caller creates the destination first, `Backup` and `Restore`
return an error matching `fs.ErrExist` with `OpError.Outcome == tarp.NotApplied`.
If either operation reports `ErrCommitUncertain` at destination rename
or parent directory sync, inspect the destination before retrying. For a
backup, validate the published image with `VerifyBackup`. For a restore,
use `OpenExisting` to inspect the published destination. A backup destination
error alone permits continued use of the source DB. If a backup encounters
correctable damage in protected sealed DATA, it repairs the source and retries
from the same point in time. If sealed DATA is confirmed unavailable, the backup
fails before publication and the open DB can keep serving other requests.

`Backup` captures a
consistent committed point in time and copies outside the writer gate while
writes continue. `Restore` checks hashes, formats, records, sequences, and live
limits, then publishes a new closed database. Existing destinations, even empty
directories, are never replaced. Parents must be caller-controlled; destinations
must be outside the source. Backups must be immutable during restore.

The worker first reclaims obsolete records from individual files when the
estimated replacement, including metadata, fits in half the source size.
When no file meets that threshold, the worker looks for 4–8 adjacent small
files to combine. At the default target each input may be at most 1 MiB and
their total at most 4 MiB. The other inputs together must be at least as large
as the largest. For example, four adjacent 40 KiB sealed logs qualify even if
every record is live. Snapshots are grouped with snapshots and logs with logs.

Both operations run in the background, yield for foreground writers and backup,
and defer when temporary space is scarce. A replaced file can remain on disk
while a reader or backup uses it.

`db.AutoCompactionStatus()` reports the worker's deferral reason, most recent
maintenance error, pinned retired bytes, and cumulative logical bytes actually
unlinked by this worker since `Open`. Filesystem allocation and staging writes
also affect free space. Provision room for current data, temporary replacements,
metadata, backups, pinned sources and concurrent growth.

Call `Compact(ctx)` for a full checkpoint through a captured sequence. It can
discard tombstones covered by that checkpoint and preserves all concurrent
tail writes. It serializes with the worker and can overlap one online backup.
Duplicate manual checkpoints or duplicate backups return `ErrBusy`. Its
`CompactResult.UnlinkedBytes` counts the logical lengths of files removed by
that call. `PinnedRetiredDataBytes` reports the retired DATA file bytes still
held by leases at cleanup. `DroppedKeys` counts live keys discarded by this
checkpoint after recovery. The worker reports its own cumulative `DroppedKeys`
in `AutoCompactionStatus`.
If the context is canceled while `Compact` or `Backup` waits for the initial
writer lock, the call returns an error matching `ctx.Err()` once it acquires
the lock, before starting checkpoint or backup work.

### Recovering damaged DATA

Every open DB generates recovery files for sealed DATA in the background.
Healthy reads and ordinary commits do no parity I/O. To protect the current
tail at a chosen point, call `db.ProtectParity(ctx)`. This seals a nonempty active DATA segment
and completes protection without building an index checkpoint.
A successful write by itself protects against crash loss according to the
commit contract. Correction of local media damage starts after parity publication.

A checked read in an open DB automatically repairs correctable corruption in
protected sealed DATA. The triggering operation retries after Tarp reconstructs
the file, verifies its original SHA-256, installs the replacement and refreshes
the read handle. Reads of other files and ordinary writes can continue during
reconstruction. Cached values are served from memory. Detection happens when
an operation reads the damaged bytes.

Each 64 KiB stripe can recover one missing or corrupt 4 KiB block. A full
stripe adds about 6.42% including checksums and record offsets. If corruption
prevents `Open`, run `tarp.RepairParity(ctx, dir)` while the DB is closed.
An independent backup covers whole-device failure.

If a sealed DATA file exceeds the correction budget, the read returns an error
matching `tarp.ErrDataUnavailable`. Keep the DB open. Healthy records, including
ones in the same file, remain readable, and `Put` and `Delete` continue to
commit. A replacement value supplied by the application can overwrite an
unreadable key. Retrieve the affected file ID with
`errors.As(err, &damage)`, where `damage` is a `*tarp.UnavailableDataError`.
A failed `BatchGet` returns no results. `Scan` reports the error and ends that
iteration. To read individual keys while serving traffic, enumerate them with
`Keys` and handle each `Get` result separately.

Compaction first tries to restore the complete DATA file. If two or more
damaged blocks in a stripe exceed its parity budget, a full checkpoint can
still keep independently verified records, including neighbors in those same
blocks. It can also recover a value from the live cache when its version and
record checksum match. A live key is dropped only when its current record
cannot be recovered. An older value or a deleted key cannot reappear.
After publication, `Get` returns `ErrNotFound` for a dropped key and a new
`Put` for that key works normally.

The automatic worker uses this full checkpoint when it learns of damage
beyond parity's correction budget. `Compact(ctx)` can also perform the
recovery explicitly. A missing or invalid sidecar, an I/O error without
verified loss, or insufficient staging space cannot authorize dropping keys.
Compaction verifies and syncs the replacement DATA and PARITY before switching
to the new files. Readers with pins keep their original files until release.

For offline recovery of historical records, run
`tarp.SalvageData(ctx, dir, fileID, maxRecordBytes, visit)` after closing the
DB. It requires a valid parity file. The callback receives historical Put and Delete
records. Check the returned error and the report's `Unrecoverable` byte ranges.
The source stays in place and the caller chooses where to export the records.
[Recovery guarantees](docs/durability.md#sealed-data-protection-and-offline-recovery)

## Metrics

Use `db.Stats()` on a raw or typed DB to collect storage size, cache activity
and write counters in one constant-time snapshot. A server can expose the
gauges and cumulative counters at `/metrics`. See the [field definitions
and Prometheus collector example](docs/stats.md).

## Development

Run `go test .` for the public API tests and examples. Each source file's unit
tests are in the matching `_test.go` file beside it. The [package example](doc_test.go)
covers backup, compaction and restore. The [benchmark directory](benchmark/)
contains the Go benchmarks, measurement drivers, guest scripts and controller tools.

Use the latest supported patches of Go 1.26 and 1.27 initially; advance this
matrix with Go's supported-release policy. The module minimum is Go 1.26.0.
The [recorded virtual Linux amd64/ext4 stack](docs/durability.md#support-boundary)
documents the configuration used for historical qualification.
Run targeted development checks on macOS. For example, use
`go test -run TestSoftRotationBoundary ./internal/engine` for rotation changes. Leave Linux
validation to CI. The following commands cover the full functional checks
and the bounded decoder fuzz target.

```sh
go test ./...
go test -race ./...
go vet ./...
go list -m all
go test ./internal/format -run '^$' -fuzz '^FuzzDecoders$' -fuzztime=15s -parallel=2
```

The [CI workflow](.github/workflows/ci.yml) runs full tests, race detection, vet,
and module dependency isolation in one Linux amd64 job using the latest stable
Go release. It runs for pull requests to `main`,
pushes to `main`, and manual dispatch. Hosted-runner checks are functional
evidence, not power-loss or bounded-device qualification.

See the [storage and backup format](docs/format.md), [durability and failure rules](docs/durability.md),
[safe qualification harness](docs/qualification.md), and
[measurement protocol and limitations](docs/measurements.md).
[Issue #14](https://github.com/anaregdesign/tarp/issues/14) tracks automatic compaction and the format cutover.

## License

tarp is licensed under the [MIT License](LICENSE).
