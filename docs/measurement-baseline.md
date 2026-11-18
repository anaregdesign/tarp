# Selected virtual-stack measurement baseline

Recorded 2026-11-18. All six profiles passed after the
[composed durability qualification](qualification.md#final-selected-target-execution-matrix).
This is a reproducible baseline, not a latency/throughput/capacity SLO, an
optimized-default claim, or physical-device certification.

## Revisions, environment, and retained evidence

| Go | Value bytes | Actual successful run |
| --- | ---: | --- |
| 1.27.4 | 64 | [44302595065](https://github.com/anaregdesign/tarp/actions/runs/44302595065) |
| 1.26.11 | 64 | [44304411029](https://github.com/anaregdesign/tarp/actions/runs/44304411029) |
| 1.27.4 | 1,024 | [44304410799](https://github.com/anaregdesign/tarp/actions/runs/44304410799) |
| 1.26.11 | 1,024 | [44304410865](https://github.com/anaregdesign/tarp/actions/runs/44304410865) |
| 1.27.4 | 4,096 | [44304411080](https://github.com/anaregdesign/tarp/actions/runs/44304411080) |
| 1.26.11 | 4,096 | [44304410816](https://github.com/anaregdesign/tarp/actions/runs/44304410816) |

The first run used `e2bb779484be25ff9f8a9fafd317b2e9a6c7219a`; the other five
used `36d5f40063ae6e74c3121aa01b51a60c18ac2448`. Only durability/qualification
documentation differs between those revisions. Every run's ordinary-library
source-manifest fingerprint is
`52f6ccde9bd3ca02be8e98465994a7130ab50b9fca763e5963b5e9d50c5bf8c8`.
The earlier `44299912658` mutable-fixture run is preliminary evidence, not part
of these tables.

The exact guest is Ubuntu 24.04.5 amd64, kernel `6.8.0-147-generic`, QEMU 8.2.2
q35/TCG/CPU max, two virtual CPUs, 1 GiB RAM and a 256 MiB data disk with 4 KiB
ext4 blocks, internal journal and `rw,relatime,data=ordered`; no DAX, quotas,
KVM, NIC or host block devices. The external RAM-only NBD layer honors FLUSH
and range-only FUA. Each job independently passed flushed-survives and
unflushed-is-discarded controls, actually discarding 4,096 overlay-page bytes.
Observed frontend FUA count was zero. The
[qualification record](qualification.md#final-selected-target-execution-matrix)
contains the image/kernel hashes and complete storage assumptions.

Hosts ran Linux `7.0.0-1014-aws`, with four CPUs and 15.3-15.6 GiB RAM.
Go 1.26.11's 64 B/1 KiB jobs used AMD EPYC 9V74 hosts; the other four used Intel
Xeon 6975P-C hosts. These are shared hosted runners executing emulated guests:
**do not rank Go releases or physical processors using these results**.
All timings include emulator/runtime/observer effects and preserve outliers.

The [protocol](measurements.md#selected-target-bounded-driver) specifies all
limits, preparation and sampling. Primary builds are uninstrumented, use
GOMAXPROCS=2, 4,096 16-byte seed keys, 64 KiB cache, 8 KiB records, 1 MiB files,
four read handles and five explicitly warmed windows. The separate diagnostic
build uses 512 keys. These are bounded datasets, not unbounded-scale evidence.
All copying, exact global LRU, fit checks and durability syncs remain enabled.

Each artifact contains `measurement.jsonl` (462 primary plus 91 diagnostic
rows), both serial logs, exact source/binary/boot manifests, device-control
witnesses, host CPU/RAM and guest environment. All 216 five-window latency
groups' retained raw samples reproduce their recorded quantiles. Across the six
jobs, 1,050 externally acknowledged NBD intervals include 960 cold Gets and 60
cold Opens; every declared cold operation reached the virtual block device.
Only the guest's clean OS cache was purged, never host/physical caches.

All 150 primary maintenance windows start at sequence 5,633 with 4,097 live
keys and the same complete-state digest for their value size, across both Go
families. Ninety produced backups were restored/reopened at independently
witnessed capture sequences with matching full contents/cardinality. All 60
declared overlapping windows had positive observed API overlap. All 90
1/8/32-caller diagnostic probes reached the exact post-copy queue.

Download `qualification-RUN_ID-1` from the linked runs. Raw artifacts have
30-day Actions retention; preserve the full files/manifests for later releases.
For raw duration chunks, concatenate `samples_ns` in `offset` order per
role/kind/repetition, sort, and take index `ceil(N*p/100)-1`. The report below
pools retained samples across five windows; sampled maintenance/diagnostic
distributions remain reservoirs/bounded prefixes, not all-observation tails.
Other numeric columns are independently computed medians of five windows.
Throughput and copy rates are first calculated per window from actual counts/
bytes and elapsed time, then summarized; ratios of aggregate columns can differ.

## Reads and required copying

All values are microseconds, `p50 / p95 / p99`. Four-worker figures are individual
call latencies, not batch throughput. Cold Gets retain 160 samples per profile
(32/window); their per-window p99 is effectively the sample maximum.

| Go / value B | Hit, one worker | Hit, four workers | Application miss, OS warm | Guest-OS-cold virtual Get |
| --- | --- | --- | --- | --- |
| 1.27.4 / 64 | 1.92 / 2.34 / 6.99 | 2.89 / 4.73 / 27.41 | 14.34 / 27.10 / 70.16 | 555.67 / 1,035.37 / 1,307.34 |
| 1.26.11 / 64 | 2.57 / 2.96 / 11.03 | 2.71 / 6.95 / 42.48 | 18.38 / 38.00 / 89.18 | 746.47 / 1,271.50 / 1,815.37 |
| 1.27.4 / 1,024 | 2.32 / 12.47 / 29.46 | 3.48 / 14.76 / 65.94 | 26.22 / 58.84 / 124.28 | 514.82 / 1,142.41 / 2,134.57 |
| 1.26.11 / 1,024 | 3.07 / 16.07 / 45.14 | 3.37 / 16.28 / 105.15 | 29.62 / 62.93 / 134.51 | 765.24 / 1,581.92 / 2,877.75 |
| 1.27.4 / 4,096 | 5.07 / 23.31 / 171.76 | 5.69 / 43.79 / 507.30 | 60.12 / 157.60 / 464.92 | 1,103.64 / 2,980.04 / 3,475.30 |
| 1.26.11 / 4,096 | 4.97 / 21.84 / 153.14 | 5.62 / 45.18 / 532.56 | 58.89 / 137.28 / 391.63 | 946.80 / 2,305.62 / 3,122.63 |

Separate 10,000-operation batches reduce individual-clock overhead. Each cell
is `ns/op / allocated B/op / allocations/op`. Four workers/channels/stacks
are prestarted/warmed; release/wait, completion and runtime overhead remain.
The independent sequential allocation probe uses GOMAXPROCS=1. Hits assert zero
Get file reads; warm cyclic misses assert one Get file read each.

| Go / value B | One-worker batch | Four-worker batch |
| --- | --- | --- |
| 1.27.4 / 64 | 1,008.025 / 64 / 1 | 1,156.367 / 64 / 1 |
| 1.26.11 / 64 | 1,506.992 / 64 / 1 | 1,405.711 / 64 / 1 |
| 1.27.4 / 1,024 | 3,712.718 / 1,024 / 1 | 3,488.732 / 1,024 / 1 |
| 1.26.11 / 1,024 | 4,800.246 / 1,024 / 1 | 4,159.320 / 1,024.0112 / 1.0001 |
| 1.27.4 / 4,096 | 11,881.090 / 4,096 / 1 | 9,539.116 / 4,096.0112 / 1.0001 |
| 1.26.11 / 4,096 | 10,853.281 / 4,096 / 1 | 9,439.008 / 4,096.0224 / 1.0002 |

Four-worker per-window maxima reach 1.0006 allocations/op; preserve this
small harness/runtime noise instead of claiming an exact aggregate one-allocation
limit. Increasing value-copy cost and shared recency/lifecycle contention affect
tails. No copy removal or approximate/sharded LRU optimization was substituted.

## Fully durable writes and mixed traffic

Cells are `p50 / p99 milliseconds (median timed API operations/second)`, 64 calls
per window. Each Put batch starts with an outside-timer prefill; every measured
Put verifies its declared active-file rotation. Every rotating Delete has its own outside-timer
full-segment Put and verifies rotation. Rates exclude these prefills and
post-call assertions, not any sync inside the measured API.

| Go / value B | Put, no rotation | Put, rotation | Delete, no rotation | Delete, rotation |
| --- | --- | --- | --- | --- |
| 1.27.4 / 64 | 7.350 / 9.286 (135.0) | 18.936 / 21.444 (52.8) | 7.340 / 9.253 (134.0) | 19.277 / 23.112 (51.6) |
| 1.26.11 / 64 | 8.274 / 73.190 (102.0) | 22.322 / 219.210 (37.7) | 8.067 / 88.070 (104.5) | 21.412 / 230.823 (39.1) |
| 1.27.4 / 1,024 | 7.501 / 9.183 (132.3) | 19.131 / 21.117 (52.2) | 7.329 / 9.130 (134.1) | 19.596 / 22.314 (50.9) |
| 1.26.11 / 1,024 | 8.339 / 230.502 (70.4) | 20.546 / 115.141 (37.6) | 7.955 / 36.108 (112.3) | 21.293 / 122.970 (36.7) |
| 1.27.4 / 4,096 | 8.281 / 9.914 (120.2) | 20.323 / 23.037 (48.7) | 7.516 / 10.026 (129.6) | 20.076 / 24.432 (49.4) |
| 1.26.11 / 4,096 | 7.937 / 9.091 (125.6) | 19.636 / 22.465 (50.8) | 7.302 / 9.016 (135.5) | 19.119 / 21.599 (52.4) |

Ordinary mutations retain data sync, synced replacement CURRENT, rename and
directory sync; rotation adds header/catalog/frontier publications. Millisecond
cost and serialized-writer throughput are the explicit conservative durability
tradeoff, not comparable to asynchronous acknowledgment.

| Go / value B | Four-worker 95% Get / 5% Put ops/s | Read p99 ms | Durable Put p99 ms |
| --- | ---: | ---: | ---: |
| 1.27.4 / 64 | 2,455.6 | 1.081 | 19.808 |
| 1.26.11 / 64 | 1,887.1 | 0.973 | 58.316 |
| 1.27.4 / 1,024 | 2,221.5 | 4.203 | 20.387 |
| 1.26.11 / 1,024 | 1,912.9 | 3.956 | 61.679 |
| 1.27.4 / 4,096 | 1,638.4 | 10.673 | 24.390 |
| 1.26.11 / 4,096 | 1,701.3 | 10.012 | 21.532 |

## Recovery, index and process resources

An additional 10,240-operation update/delete/reinsert history precedes these
five warm and five guest-cold Opens on each side of checkpoint. Warm/cold
times are medians, milliseconds; scan counts are separately recorded logical
committed-record bytes, not assumed physical bytes. Each cold Open additionally
has its own positive external NBD read interval.

| Go / value B | Warm Open before / after | Cold Open before / after | Logical scan bytes before / after | Actual index+registry B/live key before / after |
| --- | --- | --- | --- | --- |
| 1.27.4 / 64 | 176.982 / 52.730 | 194.798 / 57.982 | 1,900,672 / 524,288 | 147.404 / 147.467 |
| 1.26.11 / 64 | 239.202 / 67.732 | 246.511 / 72.660 | 1,900,672 / 524,288 | 147.354 / 147.354 |
| 1.27.4 / 1,024 | 348.726 / 97.488 | 435.473 / 123.203 | 15,664,192 / 4,456,448 | 147.977 / 147.553 |
| 1.26.11 / 1,024 | 406.983 / 117.335 | 500.008 / 144.813 | 15,664,192 / 4,456,448 | 147.988 / 147.494 |
| 1.27.4 / 4,096 | 796.328 / 232.455 | 1,118.010 / 314.131 | 59,707,456 / 17,039,360 | 149.938 / 148.164 |
| 1.26.11 / 4,096 | 786.827 / 225.489 | 1,101.178 / 315.308 | 59,707,456 / 17,039,360 | 149.824 / 148.031 |

The index experiment releases the actual rebuilt map, keys, inline locations
and shared file registry after closing cache/handles. It excludes fixed DB/
catalog/frontier state. These GC-based estimates are not a hard heap bound.
Checkpoint reduces scan history, not the live key count; segment metadata and
GC noise explain small bytes/key differences.

The raw `resources` samples additionally retain heap inuse/idle/released,
stack, runtime Sys, GC count/pause, goroutines, free space and configured handle
limits. The following medians distinguish logical cache from process memory.

| Go / value B | Heap / current RSS / peak RSS / runtime Sys MiB | Cache B | Read / write / total sampled FDs | Live / obsolete encoded MiB |
| --- | --- | ---: | --- | --- |
| 1.27.4 / 64 | 0.97 / 10.81 / 13.39 / 16.85 | 65,520 | 1 / 1 / 9 | 0.50 / 0.08 |
| 1.26.11 / 64 | 0.95 / 10.34 / 13.48 / 16.60 | 65,520 | 1 / 1 / 9 | 0.50 / 0.08 |
| 1.27.4 / 1,024 | 0.97 / 11.15 / 15.80 / 20.85 | 65,408 | 4 / 1 / 12 | 4.25 / 0.64 |
| 1.26.11 / 1,024 | 0.96 / 10.50 / 15.52 / 20.85 | 65,408 | 4 / 1 / 12 | 4.25 / 0.64 |
| 1.27.4 / 4,096 | 0.99 / 11.05 / 17.55 / 16.85 | 63,600 | 4 / 1 / 12 | 16.25 / 2.44 |
| 1.26.11 / 4,096 | 0.97 / 10.71 / 19.40 / 25.10 | 63,600 | 4 / 1 / 12 | 16.25 / 2.44 |

All queued writers hold copied keys/values before the writer gate:

| Value B | Exact logical owned B, 1 / 8 / 32 callers | Go 1.27.4 heap delta KiB, 1 / 8 / 32 | Go 1.26.11 heap delta KiB, 1 / 8 / 32 |
| --- | --- | --- | --- |
| 64 | 80 / 640 / 2,560 | -40.11 / -11.95 / -9.44 | -37.56 / -10.70 / -2.05 |
| 1,024 | 1,040 / 8,320 / 33,280 | -34.84 / -4.20 / 20.17 | -22.69 / -3.20 / 21.25 |
| 4,096 | 4,112 / 32,896 / 131,584 | -26.75 / 19.55 / 119.41 | -27.77 / 20.80 / 119.79 |

Negative process deltas are retained: GC frees other process allocations between
snapshots. Actual heap/stack/RSS/runtime changes include harness/goroutine costs
and are **not** engine-only allocation estimates or a pending-memory cap.
The queue count and logical copies are exact; caller concurrency can grow
buffering independently of the 64 KiB cache budget.

## Attributable maintenance and actual lock spans

All five modes run for at least 500 ms and through maintenance completion,
using fresh private clones of the same closed, obsolete-history fixture.
Preparation/warmup/restore verification is outside timing. Only the existing
writer key is updated to its unchanged value, preserving full expected contents.
The tables below select **Go 1.27.4 / 1 KiB** for readable comparison; all six
profiles retain the same five-mode/five-window raw resource/latency/copy results.

| Mode | Retained read p99 us | Retained Put p99 ms | Median reads/s | Median durable Puts/s |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 87.87 | 26.145 | 69,960.4 | 71.1 |
| Backup | 91.01 | 67.795 | 61,752.6 | 38.1 |
| Compact | 68.68 | 66.408 | 67,831.5 | 37.4 |
| Backup + compact | 90.83 | 75.980 | 63,854.8 | 34.3 |
| Slow backup + compact | 71.96 | 34.575 | 67,646.6 | 62.4 |

Copy cells are `DATA bytes / complete API ms / effective MiB/s`, including
segment headers and excluding metadata. API intervals include coordinated
overlap waits; intersection is observed API overlap, not physical copy-engine
utilization. The slow destination sleeps 1 ms per copy write.

| Mode | Backup copy | Compact copy | API overlap ms |
| --- | --- | --- | ---: |
| Baseline | 0 / 0 / 0 | 0 / 0 / 0 | 0 |
| Backup | 5,866,944 / 857.1 / 6.528 | 0 / 0 / 0 | 0 |
| Compact | 0 / 0 / 0 | 4,457,856 / 2,218.3 / 1.917 | 0 |
| Backup + compact | 5,866,944 / 1,477.0 / 3.788 | 4,457,856 / 2,433.2 / 1.747 | 1,455.6 |
| Slow backup + compact | 5,866,944 / 38,886.9 / 0.144 | 4,457,856 / 2,197.7 / 1.934 | 2,197.7 |

| Mode | Sampled peak heap MiB | Extra allocated disk MiB | Retired pinned MiB | Minimum available MiB |
| --- | ---: | ---: | ---: | ---: |
| Baseline | 4.09 | 0.04 | 0 | 202.38 |
| Backup | 4.33 | 5.64 | 0 | 196.82 |
| Compact | 7.66 | 4.36 | 0 | 198.05 |
| Backup + compact | 7.51 | 9.97 | 0 | 192.45 |
| Slow backup + compact | 7.66 | 12.38 | 5.60 | 190.04 |

Five-millisecond resource sampling can miss peaks and itself takes file inventory
and the writer gate. Checkpointing copies a prefix while a tail accumulates;
slow backups preserve retired sources and increase disk requirements rather than
discarding live data. The separate real ENOSPC campaign tests failure/retry;
these timing windows do not claim a reserved free-space guarantee.

The separately instrumented 512-key fixture measures actual lock spans, not
wait-time proxies. Cells are `median all-observation mean us / pooled retained
p99 us / maximum observed us across all five windows`:

| Mode | Writer | State | Lifecycle |
| --- | --- | --- | --- |
| Baseline | 4,862.76 / 17,723.11 / 22,413.41 | 3.81 / 31.89 / 3,376.86 | 3.12 / 21.74 / 5,049.74 |
| Backup | 6,021.84 / 29,669.93 / 39,088.73 | 3.39 / 28.72 / 12,367.94 | 3.19 / 13.51 / 9,836.33 |
| Compact | 7,423.30 / 38,332.41 / 47,755.44 | 3.66 / 38.63 / 4,305.53 | 2.80 / 29.36 / 3,307.33 |
| Backup + compact | 9,051.20 / 57,405.34 / 94,100.09 | 3.94 / 26.45 / 14,576.98 | 3.21 / 19.22 / 15,296.39 |
| Slow backup + compact | 5,936.03 / 21,886.37 / 58,878.14 | 3.76 / 34.49 / 6,139.37 | 3.09 / 16.27 / 5,763.08 |

Raw count/sum/max cover all observations; p99 uses the bounded retained prefix,
not an unbiased population tail. Scheduler pauses remain part of wall-clock
holds. Writer spans include sync and inventory; state/lifecycle spans exclude
value-copy/I/O work. Recording extends actual holds but is excluded from reported
spans. These diagnostic numbers must not replace the plain-build latency table.

## Acceptance and operational interpretation

The required composed correctness campaigns and reproducible baseline have
actual evidence; no numeric performance threshold was invented. Hits avoid file
reads but retain copy allocation and exact-global-recency serialization. File
misses, OS-cold virtual reads, three-barrier durable writes and rotation are
different costs. Checkpointing trades temporary space/memory/copy interference
for shorter recovery; slow backup pins increase headroom needs.

The published 64 MiB cache, 1 MiB record bound, 128 MiB files and 64 read handles
remain starting hypotheses, not optima established by these smaller fixtures.
No default, copying/admission/LRU policy, sync barrier or support minimum was
changed to improve measurements. Physical-media-cold behavior, arbitrary scale,
other kernels/filesystems and particular SSD/firmware certification remain
outside these claims, not silently passed tests.
