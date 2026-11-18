# Reproducible measurements

No latency, throughput, capacity SLO, or universally-fastest claim is implied.
The initial defaults and contention assumptions are tuning hypotheses.
Development measurements do not qualify a production filesystem or prove
physical storage reads.

## Selected-target bounded driver

The separately guarded manual VM workflow implements the release-target driver;
its implementation alone is not a measurement result. Run each size with each
supported family's latest patch after establishing the qualification target:

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
1 MiB files and four read handles: every value fits individually, while every
complete working set exceeds cache capacity. Eight hot keys fit at all sizes.
Maintenance adds one separately identified writer key.

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
  combined and separate read/durable-write distributions at approximately
  95%/5%. Sixty-four fully durable Put/Delete samples cover ordinary and
  record-by-record-rotation configurations without disabling any sync. One
  fully synced Put prefills the segment outside timing; all measured Put
  active IDs must match their declared rotating/nonrotating mode. Each
  rotating Delete follows a fully synced full-segment Put outside its timer,
  and the driver verifies all 64 Delete rotations.
- Each of baseline, backup, compaction, their overlap and slow-copy overlap
  starts from a fresh private copy of the **same closed fixture** and runs for
  at least 500 ms and until maintenance completes. The plain fixture has 4,096
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
  from the combined window. Their observed API-interval intersection is recorded.
  Overlap modes pause the first backup copy until the first checkpoint record
  write, with that controlled coordination included in reported API time.
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
  and committed record-scan bytes (logical scan bytes, not assumed physical I/O).
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
than cache for every size), five repetitions and all maintenance modes. It
records actual acquired-lock-to-Unlock-entry spans for writer/state/lifecycle
locks, including all-observation count/sum/max and bounded raw-prefix samples.
Recording extends actual holds but is excluded from that reported span.
Writer statistics include the disclosed resource/copy inventory, not just mutations.
These are not wait-time proxies and instrumented timings are not the primary
latency baseline. The plain library remains a direct `sync.Mutex` alias.
With 1/8/32 callers held at the post-copy writer queue, logical copied key/value
bytes and actual heap/stack/RSS/runtime changes characterize pending buffering;
process deltas include harness/goroutine costs and are not an engine-only cap.

`measurement.jsonl` syncs every bounded raw/summary row outside the guest.
Per-VM console logs, exact source/library/binary/image hashes, host and guest
CPU/RAM, mounts, toolchain, options, seeds, sample counts and external device
counter intervals are retained. The host cache and physical media are **not**
purged: report guest-OS-cold virtual-device latency, never physical-media-cold
latency or a general physical-device performance result. No numeric SLO was
agreed, and these smaller fixture limits do not tune the published defaults.

## Legacy development smoke driver

Choose a **new** output filename on a volume with headroom. It is created
exclusively; raw JSONL and a sibling `.mutex.pprof` are retained.

```sh
GOMAXPROCS=4 \
TARP_MEASURE_OUTPUT=/existing-evidence-directory/run.jsonl \
TARP_MEASURE_ENVIRONMENT='record OS/kernel/CPU/RAM/storage/filesystem/mounts here' \
go test -run '^TestMeasurementProfiles$' -count=1 -timeout 8m -v .
```

Run on an otherwise quiet recorded system, and retain the output and command
log. The native development matrix is not a qualified production benchmark.
For isolated Linux, use the restricted container from `qualification.md`, with
only an explicit evidence directory additionally mounted writable; /tmp must
remain disposable container storage. Do not share host data devices.

The standard-library test driver uses 64 B, 1 KiB, and 4 KiB fixed-pattern values,
16-byte binary-safe hex-text keys, 128 live seed keys, seed 42, five repetitions,
and 2,048 samples per standalone read profile. Explicit options are recorded:
whole files 64 KiB, records 8 KiB, eight read handles, and cache charge exactly
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
- Read plus synced-writer interference during baseline, backup, compaction,
  backup+compaction, and deliberately slow backup+compaction. Each lasts at least
  30 ms and through maintenance completion. Slow copies sleep 1 ms per copy
  write. Per-worker Algorithm R reservoirs retain at most 2,048 durations.
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
GOMAXPROCS=4 go test -run '^$' -bench 'BenchmarkGet|BenchmarkDurablePut' \
  -benchmem -benchtime=100x -count=5 .
go tool pprof -top /existing-evidence-directory/run.jsonl.mutex.pprof
```

Fixed operation counts keep rotating-write file creation bounded.
`BenchmarkGet/.../application-hit-parallel` uses Go's `RunParallel` with
GOMAXPROCS workers. `force-rotation-false` uses a 1 MiB file and may still
rotate during longer custom benchmark runs; `true` forces one-record files.
The repeated JSON driver uses 32 normal writes that fit its 1 MiB file.

## Legacy smoke interpretation limits

Per-operation clock calls, hooks, sampling, GC, scheduler activity, and shared
host load affect the result. The driver also enables mutex profiling at
fraction 1; profiles measure sampled contention/blocking, not exact universal
lock hold time. The separate cache critical-section probe excludes lifecycle,
contention acquisition, health checks, and returned-value copying; it must not
be represented as end-to-end Get latency.

RSS is process high-water, not isolated current DB RSS. Map/key clone deltas are
noisy GC estimates including cloned map overhead but sharing file metadata; the
logical index estimate omits allocator/bucket overhead. Pending callers are not
globally capped: Put copies inputs before waiting for the writer, so caller
concurrency can increase heap independently of the cache budget. Interference
reports one writer's caller buffer plus process-level heap, not a hard aggregate
pending-buffer bound.

Five-millisecond resource sampling can miss short peaks; directory-byte counts
are logical file lengths, not allocated blocks or guaranteed free-space deltas.
The sampler itself adds descriptors, I/O, and short writer-gate acquisition.
Small datasets do not establish large-index or long-history scaling.

Keep application misses distinct from OS page-cache misses. No shared-host cache
dropping, disk exhaustion, or power cutting is allowed. OS-cold storage profiles,
production-scale histories/working sets, detailed lock-hold instrumentation, and
release-quality repetition on the qualified amd64/ext4 target remain separate
external work. Preserve these limits in
[#5](https://github.com/anaregdesign/tarp/issues/5), rather than treating smoke
numbers as completed release acceptance.
