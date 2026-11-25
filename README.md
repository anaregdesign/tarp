# tarp

A standalone, file-backed Go key-value library. The published module,
tests, and measurement tools use the Go standard library only.

## Quick start

Use Go 1.26.0 or later on Linux or macOS (amd64 or arm64). This quick start
uses [v0.14.0](https://github.com/anaregdesign/tarp/tree/v0.14.0).

```sh
mkdir tarp-example
cd tarp-example
go mod init example.com/tarp-example
go get github.com/anaregdesign/tarp@v0.14.0
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

The [storage terminology guide](docs/terminology.md) defines DATA segments,
B+tree pages, parity stripes and the units used by APIs and metrics.

During v0, each revision supports its current Go API, database format and
backup format. Any of these can change between releases. To carry data across
a format change, export the live keys and values with the version that wrote
them, import them into a fresh database, and verify the result before switching.

v0.14.0 keeps the v0.13.0 database and backup formats. Existing calls to
`db.Compact(ctx)` keep their source-order checkpoint behavior. Both `DB` and
`TypedDB` now accept `...tarp.CompactOption`. If an interface declares
`Compact(context.Context) (tarp.CompactResult, error)`, update it and its
implementations to accept the variadic options. A callback with the old function
signature can wrap the method as `func(ctx context.Context) (tarp.CompactResult, error) { return db.Compact(ctx) }`.

v0.13.0 keeps the v0.12.0 database and backup formats. Update Go callers and
consumers of batch-limit diagnostics when upgrading from v0.12.0.

| v0.12.0 | v0.13.0 |
| --- | --- |
| `RepairParity` | `RepairDataFromParity` |
| `BackupInfo.Sequence` | `BackupInfo.LastUserSequence` |
| `BackupInfo.RequiredLimits.ValueCacheBudgetBytes` | `BackupInfo.LargestLiveCacheChargeBytes` |
| `BackupInfo.RequiredLimits.MaxRecordBytes` | `BackupInfo.LargestLiveRecordBytes` |
| `CompatibilityError.RequiredRecordBytes` | `CompatibilityError.RequiredMaxRecordBytes` |
| `DataRange` and `SalvageReport.Unrecoverable` | `DataByteRange` and `SalvageReport.UnrecoverableByteRanges` |
| `BatchPayloadLimit` (`payload-bytes`) | `BatchRecordBytesLimit` (`encoded-record-bytes`) |
| `BatchGetValueBytesLimit` (`result-bytes`) | `BatchGetValueBytesLimit` (`result-value-bytes`) |

v0.12.0 can open v0.11.0 databases and restore v0.11.0 backups directly.
`Options{}` now selects the disk-backed B+tree. Allow space on the database
volume for B+tree pages and index checkpoints, or set `IndexMode: tarp.IndexMemory`
to keep using the previous in-memory default. See [Choosing the key index](#choosing-the-key-index).

v0.11.0 changes the BACKUP header. You can open a v0.10.0 database directly
with v0.11.0 and create a new backup. If you have only a v0.10.0 backup,
restore it with v0.10.0 first, then open the restored database with v0.11.0.

The parent directory must exist. One process exclusively owns a directory, while
its `DB` supports concurrent goroutines without caller locks. Individual key operations are linearizable. Each batch call operates on its
keys atomically. Separate calls have their own publication or read points.

Use `OpenExisting(ctx, dir, opts)` when a missing database is an error, for
example when opening a restored database or checking a service's configured
path. An absent or empty directory matches `fs.ErrNotExist`. Use
`OpenContext(ctx, dir, opts)` for cancellable recovery with the usual
create-or-open behavior. After either function returns a DB, its lifetime is
controlled by `Close`. For typed access, use `Bind` on that DB.

**Every successful `Open` starts automatic incremental compaction.** An unrecognized
outer signature in CURRENT or BACKUP returns `ErrUnsupportedFormat`. Opening
such a database preserves its files. Restoring such a backup leaves the
destination unpublished.

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

### Order-preserving key codecs

`NewCodec` uses length-prefixed framing, which is compact for arbitrary values
but does not sort lexicographically by field value. When key order matters for
`ScanRange` or `KeysRange`, use the built-in `Ordered*Codec` types
(`OrderedStringCodec`, `OrderedBytesCodec`, `OrderedIntCodec`, `OrderedInt8Codec`,
`OrderedInt16Codec`, `OrderedInt32Codec`, `OrderedInt64Codec`,
`OrderedFloat32Codec`, `OrderedFloat64Codec`, `OrderedDurationCodec`,
`OrderedTimeCodec`) or derive a composite codec with `NewOrderedCodec[T]()`.

`NewOrderedCodec[T]()` supports `bool`, `string`, `[]byte`, `[N]byte`, signed
and unsigned integers (`int*`, `uint*`, `uintptr`), `float32`, `float64`,
`time.Duration`, `time.Time`, and structs (including nested structs) of exported
orderable fields in declaration order. Encoded byte order under `bytes.Compare`
matches Go's natural `<` order:

- Unsigned integers, `bool`, and `[N]byte` use big-endian / verbatim fixed-width bytes.
- Signed integers and `time.Duration` use big-endian bytes with the sign bit flipped.
- `time.Time` uses 8-byte sign-flipped Unix seconds followed by 4-byte unsigned nanoseconds (`0..999_999_999`), decoded in UTC.
- `float32` and `float64` canonicalize `-0.0` to `+0.0`, order `-Inf < negative < 0.0 < positive < +Inf`, and reject `NaN` on encode and decode.
- `string` and `[]byte` escape `0x00` bytes as `0x00 0xFF` and end with a `0x00 0x00` terminator.

Use `EncodeOrderedStringPrefix` or `EncodeOrderedBytesPrefix` to escape a raw
prefix without appending the `0x00 0x00` terminator, or
`EncodeOrderedTuplePrefix(fields...)` to encode one or more complete leading
struct fields (including string/bytes terminators) for `PrefixRange` bounds.

```go
type EventKey struct {
	Tenant string
	Seq    int64
}

func scanTenantEvents(ctx context.Context, db *tarp.TypedDB[EventKey, string], tenant string) error {
	prefix, err := tarp.EncodeOrderedTuplePrefix(tenant)
	if err != nil {
		return err
	}
	lower, upper := tarp.PrefixRange(prefix)
	for entry, err := range db.ScanRange(ctx, lower, upper, tarp.WithReverse()) {
		if err != nil {
			return err
		}
		fmt.Println(entry.Key.Seq, entry.Value)
	}
	return nil
}
```

## Prefix iteration

`KeysPrefix(ctx, prefix)` yields matching keys using the index alone.
`ScanPrefix(ctx, prefix)` yields matching key/value entries and reads their
values from DATA. Both methods are available on `DB`, `TypedDB` and `View`
starting in v0.13.0. On a raw DB with keys such as `job/42`, use
`db.ScanPrefix(ctx, []byte("job/"))`.

The prefix is a `[]byte` matching the start of the stored key encoding. Both
methods copy it before returning. A nil or empty prefix matches all live keys.
Each iteration captures a fresh snapshot and yields in unspecified order. A
nonempty prefix lets the B+tree seek to the matching range. The memory hash
index examines all live keys and copies the matching addresses when capturing
the snapshot. Typed views decode only the selected keys or entries.

For a composite key encoded with `NewCodec`, encode the leading fields using
the same field types and order. This includes the byte-length framing and
distinguishes a complete tail such as `"a"` from `"ab"`. The example reads
destination keys from a view using `NewCodec[EdgeKey]()` and `Float32Codec{}`.

```go
type EdgeKey struct{ Tail, Head string }

func printHeads(ctx context.Context, edges *tarp.View[EdgeKey, float32], tail string) error {
	type tailField struct{ Tail string }
	codec, err := tarp.NewCodec[tailField]()
	if err != nil {
		return err
	}
	prefix, err := codec.Encode(tailField{Tail: tail})
	if err != nil {
		return err
	}
	for key, err := range edges.KeysPrefix(ctx, prefix) {
		if err != nil {
			return err
		}
		fmt.Println(key.Head)
	}
	return nil
}
```

## Ordered range iteration

`KeysRange(ctx, lower, upper, opts...)` yields matching keys using the index
alone (zero DATA reads), and `ScanRange(ctx, lower, upper, opts...)` yields
matching key/value entries. Both methods are available on `DB`, `TypedDB`,
`View`, `Snapshot`, and `TypedSnapshot`, and produce strictly ordered results in
both `IndexBTree` and `IndexMemory`:

- Bounds are half-open `[lower, upper)` over encoded key bytes. A nil or empty
  `lower` is unbounded below; a nil or empty `upper` is unbounded above. If
  `len(upper) > 0 && bytes.Compare(lower, upper) >= 0`, iteration yields zero
  items.
- Default order is strictly ascending bytewise lexicographic order
  (`bytes.Compare(a, b) < 0`).
- Pass `tarp.WithReverse()` to iterate `[lower, upper)` in strictly descending
  bytewise lexicographic order (`bytes.Compare(a, b) > 0`).
- `tarp.PrefixRange(prefix)` returns cloned `(lower, upper)` bounds matching
  `bytes.HasPrefix(key, prefix)`, returning `(nil, nil)` for an empty prefix and
  `(bytes.Clone(prefix), nil)` when `prefix` consists solely of `0xff` bytes.

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
per-entry size, total encoded record bytes written and total encoded value bytes returned
by `BatchGet`. Its `Index` is the original input position, or -1 for a
request-wide count error. For input-count, encoded-record-byte and result-value-byte limits,
`Actual` and `Limit` report inputs or bytes respectively. A per-entry error
also exposes the underlying `*tarp.SizeError` with its record and cache charges.
Use `errors.As` with `*tarp.BatchCodecError` to get the zero-based input
index and the field (`key` or `value`) of a failed codec call. The error
also wraps the codec's cause. Typed batches encode keys in input order and
stop as soon as one encodes to zero bytes. `BatchPut` checks each encoded key
before calling the value codec for that entry.
See the [runnable batch example](typed_batch_test.go).

### Mixed atomic `WriteBatch`

When one atomic commit needs both puts and deletes, or idempotent deletes that
succeed when a key is already absent, build a `tarp.WriteBatch` (or
`tarp.TypedWriteBatch[K, V]`) and commit it with `db.Apply(&batch)`:

```go
var batch tarp.WriteBatch
batch.Put([]byte("active/42"), []byte("running"))
batch.Delete([]byte("queued/42")) // idempotent: no error if already absent
if err := db.Apply(&batch); err != nil {
	return err
}
batch.Reset() // retains backing capacity for reuse
```

`WriteBatch.Put` and `WriteBatch.Delete` copy their byte slices immediately so
callers can reuse scratch buffers across calls. `DB.Delete` and `DB.BatchDelete`
retain their `ErrNotFound` semantics on missing keys, whereas `WriteBatch.Delete`
is idempotent at commit time: deletes targeting non-live keys are elided without
writing orphan tombstones. Duplicate keys within one `WriteBatch` coalesce in
input order so the last operation per key wins before checking segment capacity
(`BatchRecordBytesLimit`). A nil, empty, or fully elided batch checks database
health and returns `nil` without writing DATA or advancing `WriteCommits`.

## Consistent read snapshots

Call `db.Snapshot()` on a `DB`, `TypedDB`, or `View` to capture a consistent
point-in-time read view across multiple operations (`Sequence()`, `Get`,
`BatchGet`, `Scan`, `ScanPrefix`, `ScanRange`, `Keys`, `KeysPrefix`, `KeysRange`):

```go
snap, err := db.Snapshot()
if err != nil {
	return err
}
defer snap.Close()

seq := snap.Sequence()
val, err := snap.Get([]byte("config/version"))
```

Holding an open `Snapshot` never blocks foreground writes (`Put`, `Delete`,
`BatchPut`, `BatchDelete`, `Apply`) or manual/automatic compaction; segments
referenced by the snapshot remain pinned until `snap.Close()` (which is
idempotent). Reads through an older snapshot bypass and never mutate the live
value cache once any later write has published, preventing stale or future value
leakage.

## Contract

`DB` provides `Get`, `Scan`, `ScanPrefix`, `ScanRange`, `Keys`, `KeysPrefix`, `KeysRange`, `Snapshot`,
upserting `Put`, `Delete`, `Apply`, `Close`, online `Backup`, `BatchGet`, `BatchPut`, `BatchDelete`,
explicit online `Compact`, and `AutoCompactionStatus`. Use `Open` to create or
reopen a database and `Restore` to restore a backup offline.

- Keys and values are `[]byte`. Empty keys are rejected; empty values are valid.
  Missing `Get` and `Delete` return `ErrNotFound`.
- Retained inputs and returned values are copied. Do not mutate input backing
  arrays until every call using them has returned, including failed writes.
- `Get` uses an exact global LRU first. Misses read a checked indexed file record
  and cache it; updates/deletes cannot race a stale miss into visibility.
- If `Get`, `BatchGet`, `Scan`, `ScanPrefix` or `ScanRange` encounters a DATA read error, the error
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
caller owns each key and value. `ScanPrefix` also pins the DATA layout while
iterating. A paused scan can retain old files on disk.
Break to release the snapshot early. When using `iter.Pull2`, defer `stop()`.
After an error, prior entries remain valid. Calling `Close` in the loop is
supported. A subsequent read yields `ErrClosed`.

No TTL, general read/write transactions, cross-file
records, or total disk quota is provided.

## Choosing the key index

`Options{}` and `IndexDefault` use a disk-backed B+tree with a 16 MiB
working-memory budget. The index mode is selected on each Open, so an
existing database can use either backend without migration.
If random lookup latency is a priority and every key and its DATA address fit
in the available heap, select the in-memory hash index with `IndexMemory`.

```go
db, err := tarp.Open(dir, tarp.Options{
	IndexMode: tarp.IndexMemory,
})
```

`IndexBTree` stores encoded keys and DATA addresses in private 8 KiB B+tree pages.
Tarp creates the backing files in `.tarp-work/` inside the database directory
passed to `Open` or `OpenExisting`. Size the database volume for DATA, index
pages, the index checkpoint, and temporary work during replay, compaction and online backup. Tarp
unlinks private pager files immediately after opening them. The OS releases its
space when its final descriptor closes.

The root and frequently used branch pages get priority in the cache.
Streaming overflow-key pages are evicted before validated leaf pages, so
long-key reads do not repeatedly displace and revalidate the same tree nodes.
Value LRU hits bypass the index. On a value-cache miss, cold branch, leaf and
overflow pages can add reads before loading DATA.

The configured budget is split between the foreground index (one half),
compaction and backup (one quarter each). It limits B+tree page-cache and index work
capacity. Decoded keys, request/iterator buffers, the file registry, Go's
allocator and the OS page cache also consume memory. `ValueCacheBudgetBytes`
limits entries kept in the value LRU. `MaxBatchGetValueBytes` limits encoded
value bytes returned by one batch read. Long-running readers can
hold previous B+tree root generations and retired DATA segments on disk.

Both index modes can reuse the same immutable `INDEX` file. A healthy B+tree handle
refreshes a missing or outdated index checkpoint at `Close`. To checkpoint a
long-running DB or an in-memory index, call `db.CheckpointIndex(ctx)`. It seals
a nonempty active log and packs the index for the captured DATA history. Writes can continue
during the build. Size the volume for both the build workspace and the new INDEX,
and allow for the extra Close time. Checkpoint creation leaves the normal
per-write sync protocol intact.

| `Options.OpenValidation` | Work done during Open |
| --- | --- |
| `tarp.Full` (default) | Verify every committed DATA byte. Import the packed index when available and apply later commits. |
| `tarp.Indexed` | Verify the INDEX and selected DATA headers, then read and validate only the records after the checkpoint. |

`Indexed` can defer detection of disk damage. A value read verifies the record's
identity and CRC. `Keys` and `KeysPrefix` enumerate keys without reading DATA. Reopen
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
| `OpenValidation` | `Full` | Select how Open verifies DATA covered by an index checkpoint. Both runtime backends support both validation modes. |
| `IndexMode` | `IndexBTree` | Selects a built-in key index for this Open. |
| `BTreeMemoryBudgetBytes` | 16 MiB for `IndexBTree` | B+tree workspace budget, minimum 1 MiB. Leave zero for `IndexMemory`. |
| `ValueCacheBudgetBytes` | 64 MiB | Total logical charge of keys, values and entry metadata in the value LRU. A Put must fit as a single entry. |
| `MaxRecordBytes` | 1 MiB | One encoded record, including its 48-byte header, key and value. |
| `TargetSegmentBytes` | 4 MiB | Target size for each new log or snapshot segment. Bounds the total input to small-segment repacking and the output of grouped log cleaning. |
| `ReadFileHandles` | 64 | Maximum number of reusable DATA handles for cache misses. The DB can store more files than it keeps open. |
| `BatchGetReadConcurrency` | 8 | Maximum simultaneous DATA record reads across all `BatchGet` calls on this DB. |
| `MaxBatchGetValueBytes` | 64 MiB | Maximum total encoded value bytes in one BatchGet, including duplicate inputs. |

For an encoded key of `K` bytes and an encoded value of `V` bytes, a Put has
`K + V` record payload bytes, `48 + K + V` encoded record bytes, and a logical
value-cache charge of `128 + K + V` bytes. An entry that exceeds either limit returns
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
`Open`. Set `Limits: opts.RestoreLimits()` to reuse explicit limits.
Configure handles, batch read concurrency
and the segment target when opening the restored DB.

To inspect an immutable backup, use the metadata returned by its verification.

```go
info, err := tarp.VerifyBackup(ctx, backupDir)
if err != nil {
    return err
}
fmt.Printf("database %x, sequence %d, live keys %d\n", info.DatabaseID, info.LastUserSequence, info.LiveKeys)
if err := info.CheckLimits(opts.RestoreLimits()); err != nil {
    return err
}
```

`VerifyBackup` checks the exact file set and digests and replays the selected
records. It writes no files and can make multiple passes over large backups.
For a single DATA replay using temporary disk space, call
`VerifyBackupWithScratch(ctx, backupDir, scratchDir)` with an existing
directory outside the backup. `info.LargestLiveCacheChargeBytes` and
`info.LargestLiveRecordBytes` measure the largest value-cache charge and
encoded record length across live entries. Both
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

The background worker selects the first eligible maintenance policy in this order.

1. **Single-segment cleaning** reclaims obsolete records when the estimated
   replacement, including metadata, fits in half the source bytes. It ranks
   eligible files by estimated reclaimable space relative to DATA and PARITY
   work. Reader and backup pins can defer reclamation.
2. **Grouped log cleaning** shares one publication across consecutive sealed
   logs in catalog order. It examines up to eight catalog entries, retains
   required tombstones and fits the combined DATA output in one segment target.
3. **Small-segment repacking** combines four to eight small segments of the
   same family in catalog order. Each input fits in one quarter of the target,
   their total fits in one target, and the other inputs together are at least
   as large as the largest. Four consecutive 40 KiB log segments can qualify
   at the default target even when every record is live.

The worker yields for foreground writers and backup, and defers when temporary
space is scarce. A replaced file can remain on disk
while a reader or backup uses it.

`db.AutoCompactionStatus()` reports the worker's deferral reason, most recent
maintenance error, pinned retired bytes, and cumulative logical bytes actually
unlinked by this worker since `Open`. Filesystem allocation and staging writes
also affect free space. Provision room for current data, temporary replacements,
metadata, backups, pinned sources and concurrent growth.

Call `Compact(ctx)` for a full DATA checkpoint through the captured checkpoint
cutoff sequence. A source-order build validates the captured DATA history,
including obsolete records. If replay confirms that every record in a snapshot
segment still wins at its original address, the checkpoint can keep that DATA
segment and its PARITY instead of rewriting them. It can discard covered
tombstones and preserves all concurrent tail writes. It serializes with the
worker and can overlap one online backup.
Duplicate manual full DATA checkpoints or duplicate backups return `ErrBusy`. Its
`CompactResult.UnlinkedBytes` counts the logical lengths of files removed by
that call. `PinnedRetiredDataBytes` reports the retired DATA file bytes still
retained by segment pins or live index references at cleanup. `DroppedKeys` counts live keys discarded by this
checkpoint after recovery. The worker reports its own cumulative `DroppedKeys`
in `AutoCompactionStatus`.
If the context is canceled while `Compact` or `Backup` waits for the initial
writer lock, the call returns an error matching `ctx.Err()` once it acquires
the lock, before starting checkpoint or backup work.

### Key-ordered compaction (unreleased)

Call `db.Compact(ctx, tarp.WithKeyOrder())` on a raw or typed DB after an import
or in a maintenance window to place related values together for `ScanPrefix` and `ScanRange`.
It writes retained DATA records in encoded-key order across snapshot segments.
`Scan` and `ScanPrefix` order remains unspecified. Later writes append to the log. With a
verified ordered base, the next ordered call sorts the new DATA log suffix and
rewrites affected snapshot segments or writes new ones in gaps. A healthy,
unchanged ordered checkpoint with no pending cleanup returns without reading
DATA or writing a MANIFEST. Full Open can recover the order proof during record
replay. If Open imports an INDEX covering the snapshot, the first ordered call
must still check the snapshot order.
A delta checkpoint can leave latent damage in a reused segment for its next
checked read. Schedule this work in a quiet window when foreground latency
matters. In the measured workload, overlapping
durable `Put` p99 ranged from 1.12× to 2.30× the foreground-only baseline.
See the [runnable example](doc_test.go) and
[key-ordered compaction and measurements](docs/key-ordered-compaction.md).

### Recovering damaged DATA

Every open DB generates PARITY sidecars for sealed DATA in the background.
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

Each full parity stripe covers 64 KiB of DATA and can recover one missing or corrupt 4 KiB DATA block. A full
stripe adds about 6.42% including checksums and record offsets. If corruption
prevents `Open`, run `tarp.RepairDataFromParity(ctx, dir)` while the DB is closed.
An independent backup covers whole-device failure.

If a sealed DATA file exceeds the correction budget, the read returns an error
matching `tarp.ErrDataUnavailable`. Keep the DB open. Healthy records, including
ones in the same file, remain readable, and `Put` and `Delete` continue to
commit. A replacement value supplied by the application can overwrite an
unreadable key. Retrieve the affected file ID with
`errors.As(err, &damage)`, where `damage` is a `*tarp.UnavailableDataError`.
A failed `BatchGet` returns no results. `Scan` and `ScanPrefix` report the error and end that
iteration. To read individual keys while serving traffic, enumerate them with
`Keys` and handle each `Get` result separately.

Compaction first tries to restore the complete DATA file. If two or more
damaged blocks in a stripe exceed its parity budget, a full DATA checkpoint can
still keep independently verified records, including neighbors in those same
blocks. It can also recover a value from the live cache when its version and
record checksum match. A live key is dropped only when its current record
cannot be recovered. An older value or a deleted key cannot reappear.
After publication, `Get` returns `ErrNotFound` for a dropped key and a new
`Put` for that key works normally.

The automatic worker uses this full DATA checkpoint when it learns of damage
beyond parity's correction budget. `Compact(ctx)` can also perform the
recovery explicitly. A missing or invalid sidecar, an I/O error without
verified loss, or insufficient staging space cannot authorize dropping keys.
Compaction verifies and syncs the replacement DATA and PARITY before switching
to the new files. Readers with pins keep their original files until release.

For offline recovery of historical records, run
`tarp.SalvageData(ctx, dir, fileID, maxRecordBytes, visit)` after closing the
DB. It requires a valid PARITY sidecar. The callback receives historical Put and Delete
records. Check the returned error and the report's `UnrecoverableByteRanges`.
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
go test ./internal/format -run '^$' -fuzz '^FuzzDecoders$' -fuzztime=10000x -parallel=2
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
