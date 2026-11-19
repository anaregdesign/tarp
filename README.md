# tarp

A standalone, file-backed, hash-indexed Go key-value library. The published module,
tests, and measurement tools use the Go standard library only.

## Quick start

Use Go 1.26.0 or later on Linux or macOS (amd64 or arm64). To try v0.4.0
in a new module, run the commands below. In an existing module, run just
the `go get` command.

```sh
mkdir tarp-example
cd tarp-example
go mod init example.com/tarp-example
go get github.com/anaregdesign/tarp@v0.4.0
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

`TypedDB` also supports `Delete`, snapshot iteration with `Scan`, and the same
backup and maintenance operations as `DB`. Use `NewTypedDB` to wrap an open
`*DB`. Both handles share the same records and closing either closes both.

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
key. The result uses memory proportional to all returned values, including
duplicates. Repeated puts apply in input order. Repeated deletes
remove a key once. If any requested delete key is missing, the whole call
returns `ErrNotFound` before writing.

`BatchGet` preserves LRU recency and reads each distinct cold, live key once.
Concurrent `BatchGet` calls on the same DB, including typed views, share the
DATA read limit set by `Options.BatchReadConcurrency`. The default is 8.
For example, `tarp.Options{BatchReadConcurrency: 16}` allows up to 16 reads,
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
See the [runnable batch example](typed_batch_test.go).

## Contract

`DB` provides `Get`, `Scan`, upserting `Put`, `Delete`, `Close`, online `Backup`,
`BatchGet`, `BatchPut`, `BatchDelete`, explicit online `Compact`, and `MaintenanceStatus`. Use `Open` to create or
reopen a database and `Restore` to restore a backup offline.

- Keys and values are `[]byte`. Empty keys are rejected; empty values are valid.
  Missing `Get` and `Delete` return `ErrNotFound`.
- Retained inputs and returned values are copied. Do not mutate input backing
  arrays until every call using them has returned, including failed writes.
- `Get` uses an exact global LRU first. Misses read a checked indexed file record
  and cache it; updates/deletes cannot race a stale miss into visibility.
- Successful writes include data, commit-frontier, and directory syncs before
  memory publication. An error need not mean rollback: use `errors.Is` and
  inspect `*OpError` with `errors.As` to distinguish `NotApplied`, `Committed`, and `Unknown`.
  `ErrCommitUncertain` or `ErrUnhealthy` requires close/reopen before further use.
- Oversized single `Put` inputs return `ErrTooLarge` before changing files,
  values, or cache order. Ordinary fullness evicts LRU.
- `Close` rejects new calls, cancels and joins the worker, and waits for admitted calls. Repeated/concurrent
  closes return the same retained result. It is not the first durability barrier.

`for entry, err := range db.Scan(ctx)` reads a point-in-time snapshot in
unspecified order. Each new iteration captures the live index and reads one
value at a time, preserving LRU recency. The caller owns each key and value.
Capturing the index takes O(live keys) time and memory. During capture,
`Get` and the final stage of `Put` can wait for the state lock. A slow consumer can delay retirement of old files.
Break to release the snapshot early. When using `iter.Pull2`, defer `stop()`.
After an error, prior entries remain valid. Calling `Close` in the loop is
supported. A subsequent read yields `ErrClosed`.

No ordered scans, TTL, general read/write transactions, cross-file
records, or total disk quota is provided.

## Limits

Each zero-valued field in `Options{}` selects its default. Negative or
inconsistent settings return `ErrInvalidOptions`. Normal writes always use
the commit sync protocol.

| Option | Default | What the limit applies to |
| --- | ---: | --- |
| `CacheBytes` | 64 MiB | Total logical charge of entries in this DB's LRU cache. A Put must fit as a single entry. |
| `MaxRecordBytes` | 1 MiB | One encoded record, including its 48-byte header, key and value. |
| `TargetSegmentBytes` | 4 MiB | Target size for new logs and checkpoint snapshots. Also bounds the total DATA bytes read in one automatic repack. |
| `ReadFileHandles` | 64 | Maximum number of reusable DATA handles for cache misses. The DB can store more files than it keeps open. |
| `BatchReadConcurrency` | 8 | Maximum simultaneous DATA record reads across all `BatchGet` calls on this DB. |

For a key of `K` bytes and a value of `V` bytes, a Put costs `48 + K + V` record
bytes and `128 + K + V` cache bytes. An entry that exceeds either limit returns
`ErrTooLarge`. For example, the default record limit permits a 1,048,512-byte
value with a 16-byte key. When several individually admissible entries fill the
cache, the least recently used entries are evicted. The full key index, Go
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
err := db.Backup(ctx, "/existing-parent/new-backup")
err = tarp.Restore(ctx, "/existing-parent/new-backup",
	"/existing-parent/new-restored-db", tarp.Options{})
result, err := db.Compact(ctx)
```

Handle each result before starting the next operation. `Backup` captures a
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

`db.MaintenanceStatus()` reports the worker's deferral reason, most recent
maintenance error, pinned retired bytes, and cumulative logical bytes actually
unlinked by this worker since `Open`. Filesystem allocation and staging writes
also affect free space. Provision room for current data, temporary replacements,
metadata, backups, pinned sources and concurrent growth.

Call `Compact(ctx)` for a full checkpoint through a captured sequence. It can
discard tombstones covered by that checkpoint and preserves all concurrent
tail writes. It serializes with the worker and can overlap one online backup.
Duplicate manual checkpoints or duplicate backups return `ErrBusy`. Its
`CompactResult` reports bytes reclaimed or still pinned by that operation.
Supply cancellable contexts for manual maintenance and backup.

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
