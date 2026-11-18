# tarp

A standalone, file-backed, hash-indexed Go key-value library. The published module,
tests, and measurement tools use the Go standard library only.

**Implementation is available; production power-loss qualification is not complete.**
Native macOS and isolated Linux functional results are not a production durability
certification. See [the support and durability contract](docs/durability.md) and
[qualification procedure](docs/qualification.md).

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
`Restore`, and explicit online `Compact` form the public API.

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
- `Close` rejects new calls and waits for admitted calls. Repeated/concurrent
  closes return the same retained result. It is not the first durability barrier.

No initial enumeration, ordered scans, TTL, multi-key transactions, cross-file
records, background compaction, or total disk quota is provided.

## Limits

Zero selects a default, not disabled caching or syncing. Negative settings fail.

| Option | Default | Meaning |
| --- | ---: | --- |
| `CacheBytes` | 64 MiB | Logical cached key/value/entry charges |
| `MaxRecordBytes` | 1 MiB | Complete encoded record |
| `MaxDataFileBytes` | 128 MiB | Complete data file, including its 64-byte header |
| `ReadFileHandles` | 64 | Reusable read-handle pool, not all process descriptors |

For `K` key bytes and `V` value bytes, the record is `48+K+V` bytes and cache
charge is `128+K+V` bytes. Each must fit its respective bound; the record must also
fit `MaxDataFileBytes-64`. Minimum settings are 129 cache bytes, 49 record bytes,
113 file bytes, and one handle. The record limit cannot exceed the file limit
minus 64. Arithmetic is checked.

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

Schedule `Compact` to reclaim obsolete updates/deletes. It copies a complete
prefix into a checkpoint, publishes before retiring sources, and preserves
concurrent tails. Provision space for current data **plus** checkpoint/backup
copies, metadata, pinned generations, filesystem overhead, and concurrent growth.
It never deletes live data to satisfy capacity pressure. `ReclaimedBytes` counts
unlinked logical file lengths, not a net free-space increase; `PinnedBytes`
counts retired bytes still protected by readers/backups. Release of a pin does
not perform hidden cleanup: call `Compact` again or reopen.

One backup and one compaction may overlap; duplicate calls of either kind return
`ErrBusy`. Supply non-nil cancellable contexts. Cancellation is checked between
chunks and before publication, not inside a blocking syscall or after a commit
as a fictitious rollback. Long maintenance can delay `Close`.

## Development

Use the latest supported patches of Go 1.26 and 1.27 initially; advance this
matrix with Go's supported-release policy. The module minimum is Go 1.26.0.
Linux amd64/ext4 is the first **qualification target**, not a blanket Linux
guarantee. macOS amd64/arm64 is development-only; Windows is not required.

```sh
go test ./...
go test -race ./...
go vet ./...
go list -m all
go test ./internal/format -run '^$' -fuzz '^FuzzDecoders$' -fuzztime=15s -parallel=2
```

The [CI workflow](.github/workflows/ci.yml) runs full tests, race detection, vet,
and module dependency isolation on Linux amd64 and native macOS with the latest
patches of both supported Go families. It runs for pull requests to `main`,
pushes to `main`, and manual dispatch. Hosted-runner checks are functional
evidence, not power-loss or bounded-device qualification.

See the [version-one format](docs/format.md), [durability and failure rules](docs/durability.md),
[safe qualification harness](docs/qualification.md), and
[measurement protocol and limitations](docs/measurements.md).
The implementation tracking entry point is [#1](https://github.com/anaregdesign/tarp/issues/1).
