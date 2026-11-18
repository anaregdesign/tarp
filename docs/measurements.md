# Reproducible measurements

No latency, throughput, capacity SLO, or universally-fastest claim is implied.
The initial defaults and contention assumptions are tuning hypotheses.
Development measurements do not qualify a production filesystem or prove
physical storage reads.

## Bounded repeated profile driver

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

## Interpretation limits

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
