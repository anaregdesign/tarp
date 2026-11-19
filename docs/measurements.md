# Reproducible measurements

No latency, throughput, capacity SLO, or universally-fastest claim is implied.
The cleaner's one-third savings threshold and 4 MiB rotation target are tuning
hypotheses. Smaller active logs can reduce the size of a cleaning step and
increase foreground catalog publications and syncs. Record the effective
segment target, cache and record limits, worker status and revision for any new comparison.
Development measurements do not qualify a production filesystem or prove
physical storage reads.

Benchmarks, smoke measurement profiles, and the VM measurement workflow run only
on a separate explicit user request. Ordinary functional and race tests provide
the routine PR checks.

## Public batch APIs and Stats

Run `benchmark/scripts/api-benchmarks.sh` to compare grouped and single reads,
durable puts/deletes, exact-fit writes with and without rotation, and Stats
collection at idle or alongside foreground Gets. The [v0.2.0 native
results](benchmarks/v0.2.0-native.md) include workload definitions, per-key
and per-group units, all five rounds and follow-up diagnostics.

To compare concurrent hot Gets, use the same test driver and Go toolchain
for both revisions. Alternate which binary runs first in each of five rounds.

```sh
go test -c -o /tmp/tarp-api.test ./benchmark
GOMAXPROCS=4 /tmp/tarp-api.test -test.run '^$' -test.bench '^BenchmarkParallelGetDuringStats$' -test.benchtime 2100ms -test.benchmem
```

The baseline and one-second ticker share the same 128 resident keys. Parallel
ns/op divides wall time by all completed Gets. For heap and RSS, run each
mode in a fresh process. The diagnostic fills the default segment with six
mixed-size records, parks the cleaner, and samples the live heap after forced
GC during the first data write, after publication, and with a single 32-byte
value retained. Peak RSS covers the whole process.

```sh
for mode in empty rotate; do
    GOMAXPROCS=4 TARP_BATCH_PUT_MEMORY=$mode /tmp/tarp-api.test -test.run '^TestBatchPutMemory$' -test.v
done
```

## Compaction comparison

`BenchmarkCompaction` and the opt-in JSONL drivers compare two operations on private
copies of the same closed database. The fixture has one eligible sealed log with a
surviving Put and a retained Delete, plus an active log containing live data.
Each value size (64 B, 1 KiB, 4 KiB) uses a segment target of one file header
plus 32 full Put records for both log rotation and manual checkpoint output.

| Mode | Timed interval |
| --- | --- |
| `automatic-incremental` | Release the real worker on reopen through its post-publication retirement directory sync. It replaces one sealed log and preserves the active log. |
| `manual-full-checkpoint` | Run one public `Compact(ctx)` with the worker parked. It checkpoints the entire committed prefix, including the active records. |

Fixture creation, cloning, Open, and verification take place outside the timer.
For each mode the driver checks the original files that disappeared, counts the
newly published DATA file lengths (including headers), then reopens the result
and verifies the same keys, values, and committed sequence. Net DATA bytes freed
equals unlinked DATA bytes minus published DATA bytes. Manifest sizes and
filesystem allocation are recorded separately where sampled. The JSONL rows
`compaction_automatic-incremental` and `compaction_manual-full-checkpoint`
include effective options, checkpoint cutoff, pruning boundary and the state
digest. The two modes perform different amounts of work.

## Foreground writes and reads during automatic repacking

`BenchmarkRepackForeground` uses two private copies of the same closed database.
The fixture has eight sealed logs containing eight live keys each and one
existing writer key in the active log. Reopening at the default 4 MiB target
makes all eight sealed logs eligible to be packed together. Both arms read the
same warm key and durably overwrite the same active key.

Each arm performs 128 synced Puts and 1,024 Gets, with eight reads per write.
The writer waits 1 ms between calls in both arms so the cleaner can run between
writes. The gap is included in wall time and excluded from individual latencies.
The `parked` arm holds the worker for the whole window. In the `concurrent`
arm the first copy and foreground operations start together. After syncing its
output the worker waits for the first Get and Put if either is still pending.
`coordination_wait_ns` reports that deliberate wait. The repack interval
ends when retirement of the source files has been directory-synced.

Both rows report foreground latency samples and p50/p99, including calls that
finish after the repack. The concurrent row also reports counts and p50/p99
for only the Gets and Puts overlapping the repack interval. With fewer than
100 overlapping calls, the nearest-rank p99 is the largest observation.
Each arm checks the physical DATA count, active log, committed sequence and
complete contents after reopen. The expected DATA count is nine
with the worker parked and two after repacking (one combined log and the active log).

This command runs one iteration per arm at 4 KiB.

```sh
GOMAXPROCS=4 go test -run '^$' -bench '^BenchmarkRepackForeground$/^4096$' -benchtime=1x -count=1 ./benchmark
```

The native and primary VM measurement drivers include the same scenario for
64 B, 1 KiB and 4 KiB values. Each repetition emits one `repack_parked` and
one `repack_concurrent` row. The drivers alternate which arm runs first.

## Scan workloads

The primary VM profile runs an untimed traversal to warm the OS file cache,
then measures `scan_full_warm` over all 4,096 live keys. Time to the first
entry includes capturing the live index and loading the first record. Total
traversal time includes the consumer checking every key and value. Reported
bytes count value payloads.

In `scan_paused_checkpoint`, a consumer reads one of 65 entries and pauses.
The benchmark overwrites and deletes unread keys, adds a new key and completes
a manual checkpoint before resuming the iterator. It verifies both the old
snapshot and the updated DB, checks which source files are retained, and waits
for the worker to unlink retired files. The three mutation times describe
progress while the snapshot is held. The workload reports checkpoint time
and file reclamation separately.

To sample both scenarios locally at 4 KiB, run
`go test ./benchmark -run '^$' -bench '^BenchmarkScan/4096/' -benchtime=1x`.
The local warm traversal uses 128 keys.

## Selected-target bounded driver

All six 64 B/1 KiB/4 KiB and Go 1.26.11/1.27.4 profiles completed on the
historical virtual stack at revisions `e2bb779` and `36d5f40`, before the
format cutover and automatic compaction. The [recorded baseline](measurement-baseline.md)
contains actual run links, results, environment differences and interpretation.
The separately guarded manual workflow implements the reproducible driver;
its implementation alone is not evidence that a different run passed.
Run each size with each supported family's latest patch:

```sh
gh workflow run qualification.yml --ref REVIEWED_REF \
  -f confirmation=disposable-vm-only -f mode=measure \
  -f go-version=stable -f value-bytes=1024
```

Select `64`, `1024`, or `4096`; `1.26.x` selects the other initially supported
family. This is not a normal CI matrix. Each job reruns actual volatile-device
controls, then runs one plain measurement guest and a separate diagnostic guest.
Both use the pinned amd64 Ubuntu 24.04 / 6.8 / ext4 ordered / honest-flush NBD
stack. Primary/diagnostic guests are bounded to 30/15 minutes, the controller
to 50 minutes and the whole manual job to 60 minutes. Ordinary CI stays one
Linux/latest-stable job.

The **plain** build prepares 4,096 16-byte keys once and records five explicitly
warmed measurement windows. Options are 64 KiB logical cache, 8 KiB records,
a 1 MiB segment target and four read handles. Every value fits individually,
while every complete working set exceeds cache capacity. Eight hot keys fit at all sizes.
The interference profiles add one separately identified writer key.

- Hit profiles retain all 8,192 individual durations for one/four workers,
  plus a 10,000-call batch ns/op and allocated-byte sample, allocations/op and
  an asserted zero Get-storage-read count. Required returned-value copying stays
  enabled. Both one- and four-worker batches report ns/op, bytes/op and
  allocations/op. Four workers/channels are prestarted/preallocated, and each
  worker warms its stack before the process Mallocs/TotalAlloc snapshots;
  release/wait, goroutine completion and runtime overhead remain included.
  The separate sequential AllocsPerRun probe temporarily uses GOMAXPROCS=1;
  timings use 2.
- Cyclic warm-OS application misses assert 8,192 actual Get file reads. A
  reproducible 4,096-operation, four-worker uniform-key workload records both
  combined and separate read/durable-write distributions. Its 3,891 Gets and
  205 durable Puts use the same keys as the historical single-writer
  `mixed_95r_5w` row. The new `mixed_multi_writer` schedule assigns 51–52
  Puts to each worker and reports each worker’s counts and the number of
  Puts whose API calls overlapped another Put. Run both schedules on matched
  1 KiB clones with `go test -run '^$' -bench '^BenchmarkMixedWriters$' -benchtime=1x -v ./benchmark`. Sixty-four fully durable Put/Delete samples cover ordinary and
  record-by-record-rotation configurations without disabling any sync. One
  fully synced Put prefills the segment outside timing; all measured Put
  active IDs must match their declared rotating/nonrotating mode. Each
  rotating Delete follows a fully synced full-segment Put outside its timer,
  and the driver verifies all 64 Delete rotations.
- Each of baseline, backup, manual checkpoint, their overlap and pinned-backup overlap
  starts from a fresh private copy of the **same closed fixture** and runs for
  at least 500 ms and until maintenance completes. The worker is parked during
  fixture preparation and throughout all five timed windows. The plain fixture has 4,096
  seed keys plus 1,024 updates and 256 deletes/reinsertions; the diagnostic
  fixture has 512 keys plus 512 updates and 128 deletes/reinsertions. Preparation,
  complete-state digesting and hot-key warmup occur outside the timed window;
  fixture copies are removed after each mode. This closed test-fixture copy is
  not an online backup strategy. Bounded Algorithm R
  reservoirs retain at most 2,048 samples per worker. Five-millisecond sampled
  heap, logical/allocated temporary disk, available headroom and retired pins
  are reported; the sampler's own I/O/writer-lock acquisitions perturb all modes.
  Backup and compaction each retain actual copied DATA-file bytes (including
  headers, excluding metadata) and their complete API elapsed time, separately
  from the combined window. For manual checkpoints, `compact_copy_source_opens`
  and `compact_records_copied` count source opens and successfully copied live
  records during the copy phase. The preceding validation scan is excluded.
  The driver records the overlap between the two API calls.
  Both overlap modes park the first backup DATA write after capturing its frontier.
  `backup-compact` releases it at the first checkpoint record write.
  `pinned-backup-compact` holds it until the checkpoint returns and records which
  retired source files its backup still pins. The `pinned_backup` object reports
  coordination time, retained logical bytes and file count. After the timed window
  and restore, the fixture releases the automatic worker and verifies the exact
  source files disappear at a successful retirement directory sync. It records
  their unlinked byte and file counts and the worker's separate reclamation time.
  The diagnostic writer-lock row includes the same witness.
  Copy bytes divided by the respective API time give reproducible effective
  copy throughput, not a physical copy-engine utilization claim. Each produced
  backup is restored, reopened at its independently observed staging-CURRENT
  capture sequence and checked against the complete pre-window fixture digest
  and live cardinality. The only concurrent mutation is an already-present
  fixed-value writer key, so full expected contents remain invariant.
- Current/peak RSS, Go heap/inuse/idle/released, stacks, Sys and GC metrics are
  separate from logical cache charges. Actual read/write data descriptors,
  file-handle configuration and live/obsolete/pinned encoded storage are reported.
- An additional 8,192 updates, 1,024 deletes and 1,024 reinsertions over 1,024
  keys form a disclosed 10,240-operation history. Five warm and five guest-cold
  file-only reopens **each** before and after checkpoint record elapsed startup
  and the selected replay layout. Each probe opens a copy of a frozen image
  with the worker parked; the default-history image may contain already-cleaned
  intervals. Empty cleaned intervals add no file or header. Retained Delete
  bytes are tracked separately from obsolete Put bytes.
  Each cold Open has independently acknowledged external NBD read intervals;
  cache purge and handshake time are excluded from Open latency.
- Complete-index retention is measured by retaining then releasing the **actual**
  rebuilt key map, owned keys, inline locations and shared file registry after
  closing cache/handles. It is not the old cloned-map proxy. Fixed DB/catalog/
  frontier state is excluded; raw GC deltas remain noisy, not a process limit.
- Thirty-two Gets per repetition clear application cache and purge **only this
  guarded guest's** clean page cache. A start/end acknowledgment through the
  independent read-only 9p channel brackets the operation against external NBD
  counters. Each declared cold Get must show actual virtual-block reads.
  Drop/handshake time is outside Get timing. The 32-sample p99 is effectively
  a sample maximum, not a precise population-tail estimate.

The **diagnostic-only** `tarpprofile` build separately uses 512 keys (still larger
than cache for every size), five repetitions and the five interference controls. It
records actual acquired-lock-to-Unlock-entry spans for writer/state/lifecycle
locks, including all-observation count/sum/max and bounded raw-prefix samples.
Recording extends actual holds but is excluded from that reported span.
Writer statistics include the disclosed resource/copy inventory, not just mutations.
These are not wait-time proxies and instrumented timings are not the primary
latency baseline. The plain library remains a direct `sync.Mutex` alias.
With 1, 8 or 32 callers waiting at the writer gate, the diagnostic driver
records actual input copies and process memory. It checks zero owned input
bytes while queued and the full expected byte count after all Puts return.
Heap, stack and RSS deltas also include caller and runtime costs.

`measurement.jsonl` syncs every bounded raw/summary row outside the guest.
Per-VM console logs, exact source/library/binary/image hashes, host and guest
CPU/RAM, mounts, toolchain, options, seeds, sample counts and external device
counter intervals are retained. The host cache and physical media are **not**
purged: report guest-OS-cold virtual-device latency, never physical-media-cold
latency or a general physical-device performance result. No numeric SLO was
agreed, and these smaller fixture limits do not tune the published defaults.

## Focused foreground diagnostics

Use the following macOS commands to isolate a maintenance path with real durable writes. Both use the ordinary engine with observation hooks and report timings separately from the plain VM profile.

```sh
GOMAXPROCS=2 TARP_MAINTENANCE_BENCH=1 go test -run '^TestMaintenanceForeground$' -count=1 -v ./benchmark
GOMAXPROCS=4 TARP_REPACK_TRACE=/tmp/repack-new.jsonl go test -run '^TestRepackStageTrace$' -count=1 ./benchmark
```

The first command uses the same 4 KiB fixture and 128 foreground Puts per arm. Its JSON records align individual requests with maintenance, writer-gate waits and foreground syncs. It verifies the checkpoint or restored backup against the captured data. The second command needs a new output filename. It runs five paired 64 B repacks and records timestamps for writer waits, syncs, catalog publication, index updates and file retirement. Compare the slowest overlapping Put against an equal-size prefix of the parked control.

## Native development smoke driver

Choose a **new** output filename on a volume with headroom. It is created
exclusively; raw JSONL and a sibling `.mutex.pprof` are retained.

```sh
GOMAXPROCS=4 \
TARP_MEASURE_OUTPUT=/existing-evidence-directory/run.jsonl \
TARP_MEASURE_ENVIRONMENT='record OS/kernel/CPU/RAM/storage/filesystem/mounts here' \
go test -run '^TestMeasurementProfiles$' -count=1 -timeout 8m -v ./benchmark
```

Run on an otherwise quiet recorded system, and retain the output and command
log. The native development matrix is not a qualified production benchmark.
Use macOS for local checks. The disposable Linux guest is a separate, explicitly requested profile.

The standard-library test driver uses 64 B, 1 KiB, and 4 KiB fixed-pattern values,
16-byte binary-safe hex-text keys, 128 live seed keys, seed 42, five repetitions,
and 2,048 samples per standalone read profile. Explicit options are recorded:
a 64 KiB segment target, 8 KiB records,
eight read handles, and cache charge exactly
128 entries for hits or eight entries for misses. These deliberately bounded
smoke datasets are not a capacity claim.

Each repetition records:

- Warm 1-worker and 4-worker hit p50/p95/p99 and raw nanosecond samples. A hook
  asserts no Get file reads. Returned-value copying is enabled. Allocations/op
  use `testing.AllocsPerRun`; bytes/op use `TotalAlloc` deltas.
- Sequential cyclic application misses over a working set larger than cache,
  asserting one file read per call. OS data is expected warm from setup and is
  not deliberately evicted; physical residency is not independently verified.
- A separately labeled state-lock hit critical-section **probe**, not an
  end-to-end operation or exact hold time of every production lock.
- Heap, runtime Sys, process peak RSS, logical cache bytes, complete-index
  logical fields and a retained cloned map/key heap estimate, process descriptor
  count, handle limit, live/obsolete/data/directory bytes, and filesystem numbers.
- Two full update passes and deletion of one quarter of keys, followed by reopen
  timings before and after checkpointing. These are bounded histories, not a
  production-scale startup qualification.
- Read plus synced-writer interference on identical private fixtures during baseline, backup, manual checkpoint,
  backup+compaction, and a backup held through checkpoint completion. Each lasts at least
  30 ms and through maintenance completion. The automatic worker stays parked during timing.
  The pinned-backup workload releases it after restore to verify retirement. Per-worker Algorithm R reservoirs retain at most 2,048 durations.
- Maintenance time, actual operation counts, sampled heap/disk peaks and
  retired pinned bytes, and CompactResult. Outputs are restored/validated.
- Thirty-two fully synced Put samples with and without forced record-by-record
  rotation and thirty-two fully synced Delete samples. No sync is disabled.

The JSONL gives operation counts and wall durations for throughput and
sample durations for mean/percentiles. Includes Go/OS/architecture/GOMAXPROCS,
warmup method, limits, seeds, and repetition numbers. Supply exact hardware,
storage, mount, load/isolation and power-policy details in the environment note
and accompanying report. Record failed runs too; do not blend them into passes.

## Standard Go benchmarks

```sh
GOMAXPROCS=4 go test -run '^$' -bench 'Benchmark(Get|DurablePut|Compaction)$' \
  -benchmem -benchtime=100x -count=5 ./benchmark
go tool pprof -top /existing-evidence-directory/run.jsonl.mutex.pprof
```

Fixed operation counts keep rotating-write file creation bounded.
`BenchmarkGet/.../application-hit-parallel` uses Go's `RunParallel` with
GOMAXPROCS workers. `BenchmarkDurablePut` records observed rotations per timed
Put. The `default-target` case can rotate during longer runs. The
`one-record-target` case changes only the soft rotation target between arms
and prefills one record before timing. Both arms use the same cache and record
limits. The native JSONL driver times 32 Puts per case.

## Development smoke interpretation limits

Per-operation clock calls, hooks, sampling, GC, scheduler activity, and shared
host load affect the result. The driver also enables mutex profiling at
fraction 1; profiles measure sampled contention/blocking, not exact universal
lock hold time. The separate cache critical-section probe excludes lifecycle,
contention acquisition, health checks, and returned-value copying; it must not
be represented as end-to-end Get latency.

RSS is process high-water, not isolated current DB RSS. Map/key clone deltas are
noisy GC estimates including cloned map overhead but sharing file metadata; the
logical index estimate omits allocator/bucket overhead. Pending callers retain
their input slices. Caller concurrency can increase heap independently of the
cache budget. Interference reports one writer's caller buffer and process-level heap.

Five-millisecond resource sampling can miss short peaks; directory-byte counts
are logical file lengths, not allocated blocks or guaranteed free-space deltas.
The sampler itself adds descriptors, I/O, and short writer-gate acquisition.
Small datasets do not establish large-index or long-history scaling.

Keep application misses distinct from OS page-cache misses. No shared-host cache
dropping, disk exhaustion, or power cutting is allowed. The selected-target
driver and [baseline](measurement-baseline.md), not these smoke numbers, supply
the separately executed guest-cold profiles, larger disclosed histories/working
sets, actual lock spans, queued buffers and repeated release-target results for
[#5](https://github.com/anaregdesign/tarp/issues/5). Neither driver establishes
unbounded scale, physical-media-cold performance, or a numeric SLO.
