# Memory-index checkpoint relocation

[Issue #108](https://github.com/anaregdesign/tarp/issues/108) follows the [same-host investigation](compact-64-same-host.md) into manual Compact with the default in-memory index. Each comparison uses v0.9.0 as its baseline and runs both binaries on the same guest.

The final implementation reduced median manual Compact time under traffic by **43.0% and 50.3%** in two independent runner jobs. Compact finished sooner in all twelve matched pairs. Both versions used the default in-memory index, a shared 4,097-key fixture with 64-byte values, and a single durable writer.

## Compact and foreground latency

Each cell gives the median of six uninstrumented windows. Each round alternated which version ran first. The traffic window runs a hot Get worker, a durable Put worker, and the original 5 ms inventory sampler until Compact completes, subject to a 500 ms minimum. The parked control stops all three. Each p50, p95 and p99 is calculated within a window before taking the median across windows.

| Metric | Run 1 v0.9.0 | Run 1 candidate | Run 2 v0.9.0 | Run 2 candidate |
| --- | ---: | ---: | ---: | ---: |
| Compact with traffic (ms) | 1,125.7 | 642.0 | 1,367.1 | 679.2 |
| Compact with traffic parked (ms) | 310.8 | 278.3 | 300.8 | 266.2 |
| Durable Put p50 (ms) | 13.9 | 21.1 | 14.1 | 18.4 |
| Durable Put p95 (ms) | 37.7 | 50.7 | 43.1 | 52.7 |
| Durable Put p99 (ms) | 61.5 | 57.0 | 78.1 | 62.3 |
| Hot Get p99 (µs) | 25.8 | 25.1 | 31.0 | 18.4 |

The higher Put p50 and p95 occurred in shorter candidate windows. Individual candidate windows contained 26–34 Puts, against 36–132 in v0.9.0. The equal-duration control below measures what happens when foreground traffic continues after Compact. [Run 1](https://github.com/anaregdesign/tarp/actions/runs/44761046027) · [Run 2](https://github.com/anaregdesign/tarp/actions/runs/44761087353)

## Equal five-second foreground windows

Two further jobs ran six alternating pairs each. Foreground traffic and the sampler continued for five seconds around exactly one Compact per clone. Every Compact returned before the horizon. The last in-flight Put could finish just after it. Rates use successful calls divided by each worker's actual wall time. The table gives medians of per-window values.

| Metric | EPYC 7763 v0.9.0 | EPYC 7763 candidate | EPYC 9V74 v0.9.0 | EPYC 9V74 candidate |
| --- | ---: | ---: | ---: | ---: |
| Compact (ms) | 2,573 | 1,403 | 1,622 | 749 |
| Durable Put (ops/s) | 47.2 | 44.1 | 60.4 | 56.7 |
| Durable Put p50 (ms) | 18.1 | 19.2 | 13.4 | 14.2 |
| Durable Put p95 (ms) | 43.2 | 44.7 | 35.1 | 36.6 |
| Durable Put p99 (ms) | 65.6 | 70.8 | 62.5 | 62.0 |
| Hot Get (ops/s) | 56,843 | 53,889 | 91,726 | 93,217 |
| Hot Get p99 (µs) | 109.5 | 93.3 | 50.0 | 40.2 |

The fixed windows show a foreground cost alongside the shorter Compact. Median durable Put throughput fell **6.5% and 6.2%**, and p95 rose **3.5% and 4.5%**. Per-window p99 rose 8.0% on one runner and fell 0.7% on the other. Pooling all Puts within each job gives p99 of 66.3 → 74.7 ms (1,413 → 1,347 Puts) and 63.9 → 64.7 ms (1,804 → 1,709). A deployment's result will depend on how often manual Compact overlaps writes. [EPYC 7763 campaign](https://github.com/anaregdesign/tarp/actions/runs/44762043960) · [EPYC 9V74 campaign](https://github.com/anaregdesign/tarp/actions/runs/44762126335)

## Relocation while a writer syncs

While publishing the checkpoint, Compact tracks the keys staged by the single active writer. A disjoint relocation batch checks each old physical address and updates its index entry and file accounting together under the index and state locks. An overlapping batch waits for the writer and rechecks the addresses after commit or abort. The set borrows key strings from at most one foreground batch (up to 4,096 inputs). Spool I/O runs outside these locks. Each relocation batch allows up to 128 moves within 8 KiB of encoded data, with an oversized key processed alone. Automatic segment cleaning and the B+tree use the existing publication path.

The diagnostic binaries ran four additional pairs in each job. The fixture produced 39 relocation batches. Each Compact on the candidate took the durable writer lock **zero or one time** during relocation, down from 129. It acquired the index gate 39–40 times. The timers add overhead and the intervals below overlap.

| Median diagnostic interval (ms) | Run 1 v0.9.0 | Run 1 candidate | Run 2 v0.9.0 | Run 2 candidate |
| --- | ---: | ---: | ---: | ---: |
| Relocating all entries | 977.5 | 36.2 | 450.5 | 46.2 |
| Total wait for the durable writer lock | 817.2 | 2.3 | 347.6 | 8.9 |
| Longest durable writer lock hold per Compact | 6.96 | 0.50 | 4.25 | 0.95 |
| Total wait for the index gate | 0.47 | 2.81 | 0.39 | 0.50 |
| Longest index gate hold per Compact | 3.06 | 1.35 | 2.05 | 2.25 |

## Increasing the batch size

The [first candidate](https://github.com/anaregdesign/tarp/commit/e7cc52d05d22819a398fa3d6a003e3f892af3a7e) raised the count limit from 32 to 128, kept the 8 KiB encoded byte limit, and still acquired the writer lock for every batch. Each 16-byte key costs 76 encoded bytes, so 107 moves fit in one batch. This reduced writer acquisitions from 129 to 39 in all eight diagnostic pairs. The median longest writer hold increased by 74% in both campaigns. The six uninstrumented traffic pairs favored this version in three rounds on the Intel runner and five on the AMD runner. This result motivated testing whether disjoint keys could move during a foreground write's sync.

| Median metric | Intel v0.9.0 | Intel larger batch | AMD v0.9.0 | AMD larger batch |
| --- | ---: | ---: | ---: | ---: |
| Compact with traffic (ms) | 1,195.7 | 1,181.0 | 2,859.3 | 2,282.1 |
| Compact with workers and sampler parked (ms) | 303.8 | 313.4 | 561.6 | 545.5 |
| Durable Put p99 (ms) | 65.9 | 59.9 | 69.3 | 80.6 |
| Hot Get p99 (µs) | 25.4 | 30.2 | 91.1 | 85.5 |
| Relocation writer wait per Compact (ms, diagnostic) | 521.1 | 583.3 | 1,000.8 | 677.8 |
| Longest relocation writer hold per Compact (ms, diagnostic) | 3.09 | 5.37 | 7.35 | 12.78 |

[First campaign, Intel Xeon 6975P-C](https://github.com/anaregdesign/tarp/actions/runs/44758847671) · [Replication, AMD EPYC 7763](https://github.com/anaregdesign/tarp/actions/runs/44759132327)

## Correctness and reproduction

Each successful Compact copied 4,097 live records (524,480 bytes). After each trial the driver reopened the clone and verified its UUID, live-key count, state digest and final sequence against the acknowledged writes. Both release and candidate passed the pinned-backup restore and retirement controls. Engine tests pause staged mutations across relocation and cover overwrite, duplicate-key writes, delete, abort, reopen, scan leases and retirement of the source file.

Both versions were built in the same official Go image, `golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`, using `go version go1.27.4 linux/amd64`. Each job ran its complete comparison on one QEMU guest with two vCPUs, GOMAXPROCS=2 and ext4 in ordered mode. Every arm used a fresh process and a clone of the same closed fixture. Get percentiles use a reservoir of up to 2,048 calls per window. All recorded durable Puts fit in that limit.

To reproduce, use the workflow and drivers from the linked comparison commits, select `compact-compare`, `value-bytes=64` and `confirmation=disposable-vm-only`. Use `compact-pair=candidate` for traffic, parked and diagnostic windows or `compact-pair=candidate-fixed` for the equal five-second windows. Enter the recorded image digest to use the same toolchain. The tested library sources are identical in the [main comparison](https://github.com/anaregdesign/tarp/commit/d389ef68bfa56bb0085ec92fd2bcdbae5689422e) and the [fixed-window comparison](https://github.com/anaregdesign/tarp/commit/325432bcf835abe782eb797425c23add5ecff04f).

The [evidence archive](compact-memory-relocation.tar.gz) contains the 174 JSON rows from all six jobs, original serial logs, flush controls, verdicts, toolchain details, revision IDs and binary hashes. Directories are named for the corresponding Actions run. Archive SHA-256 is `18507bd6046fa33786f9042d2b34ab7dfdb58369e7e1082ca9d93adc9349800a`.
