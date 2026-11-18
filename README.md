# tarp

A standalone, file-backed, hash-indexed Go key-value library. The published module,
tests, and measurement tools use the Go standard library only.

**Every successful `Open` starts automatic incremental compaction.** Both new
and reopened stores use one unversioned storage and backup format. A database
or backup from the previous implementation returns `ErrUnsupportedFormat`.
Opening it leaves the files untouched and restoring it leaves the destination
unpublished. There is no migration path in this package.

The historical Linux/ext4 power-cut results and performance measurements cover
the earlier storage implementation. Evidence for the current format and cleaner
must be collected on their own revision. See the [durability contract](docs/durability.md),
[qualification record](docs/qualification.md) and [historical baseline](docs/measurement-baseline.md).

```go
package example

import (
	"errors"
	"fmt"

	"github.com/anaregdesign/tarp"
)

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

The parent directory must exist. One process exclusively owns a directory, while
its `DB` supports concurrent goroutines without caller locks. Individual key
operations are atomic and linearizable; separate calls are not a transaction.

## Contract

`Open`, `Get`, upserting `Put`, `Delete`, `Close`, online `Backup`, offline
`Restore`, explicit online `Compact`, and `DB.MaintenanceStatus` form the public API.

- Keys and values are `[]byte`. Empty keys are rejected; empty values are valid.
  Missing `Get` and `Delete` return `ErrNotFound`.
- Retained inputs and returned values are copied. Do not mutate input backing
  arrays until every call using them has returned, including failed writes.
- Reads use an exact global LRU first. Misses read a checked indexed file record
  and cache it; updates/deletes cannot race a stale miss into visibility.
- Successful writes include data, commit-frontier, and directory syncs before
  memory publication. An error need not mean rollback: use `errors.Is` and
  inspect `*OpError` with `errors.As` to distinguish `NotApplied`, `Committed`, and `Unknown`.
  `ErrCommitUncertain` or `ErrUnhealthy` requires close/reopen before further use.
- Oversized single `Put` inputs return `ErrTooLarge` before changing files,
  values, or cache order. There is no cache bypass. Ordinary fullness evicts LRU.
- `Close` rejects new calls, cancels and joins the worker, and waits for admitted calls. Repeated/concurrent
  closes return the same retained result. It is not the first durability barrier.

No initial enumeration, ordered scans, TTL, multi-key transactions, cross-file
records, or total disk quota is provided.

## Limits

Zero selects a default, not disabled caching or syncing. Negative settings fail.

| Option | Default | Meaning |
| --- | ---: | --- |
| `CacheBytes` | 64 MiB | Logical cached key/value/entry charges |
| `MaxRecordBytes` | 1 MiB | Complete encoded record |
| `TargetSegmentBytes` | 4 MiB | Soft rotation target for new log appends, including the header |
| `MaxDataFileBytes` | 128 MiB | Complete data file, including its 64-byte header |
| `ReadFileHandles` | 64 | Reusable read-handle pool, not all process descriptors |

For `K` key bytes and `V` value bytes, the record is `48+K+V` bytes and cache
charge is `128+K+V` bytes. Each must fit its respective bound; the record must also
fit `MaxDataFileBytes-64`. Minimum settings are 129 cache bytes, 49 record bytes,
113 file bytes, and one handle. The record limit cannot exceed the file limit
minus 64. Arithmetic is checked.

The effective rotation target fits one maximum-size record and its header and
is capped at `MaxDataFileBytes`. The next append rotates a nonempty log only
when the whole record would exceed that target. An existing file larger than
4 MiB can be opened or restored under compatible hard limits. Smaller log
segments reduce the usual amount of data examined per cleaning step and
increase rotation, catalog publication, and sync frequency.

The cache bound excludes the full in-memory key index, map/allocator/GC overhead,
in-flight callers, maintenance maps/buffers, and the OS page cache. It is not a
Go heap or RSS bound. Defaults are starting hypotheses, not measured optima.

Reopening with smaller limits validates every live record/cache charge and every
selected physical file before truncation or cleanup. Incompatible settings fail
with `ErrIncompatibleOptions` and typed required minima; persisted values are
never silently made unreadable or admitted without caching. Obsolete records
are still checked even when larger than a new admission bound.

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

The worker estimates how much disk space each immutable file can yield. It
cleans one sealed log or snapshot at a time, copying live Puts and required
Deletes. Its initial threshold requires estimated net savings of at least a
third of the I/O bytes to read and write. It yields for foreground writers and
backup, and defers when temporary space is scarce. Cache-hit `Get` takes its
usual path. A reclaimable file can remain on disk while a reader or backup uses it.

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

## Development

Use the latest supported patches of Go 1.26 and 1.27 initially; advance this
matrix with Go's supported-release policy. The module minimum is Go 1.26.0.
The [recorded virtual Linux amd64/ext4 stack](docs/durability.md#support-boundary)
documents the configuration used for historical qualification.
Run targeted development checks on macOS. For example, use
`go test -run TestSoftRotationBoundary .` for rotation changes. Leave Linux
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
