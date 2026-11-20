# Foreground Get, Put, Delete and batch experiments

Five prototypes were measured independently against `357e758`. Three changes shipped together. The integrated comparison below uses the final reviewed code. [Issue #116](https://github.com/anaregdesign/tarp/issues/116) · [Raw logs, individual patches, drivers, checksums and reproduction commands](./foreground-116.tar.gz).

## Integrated public API results

Each mean time is the median of six run means. For every pair we calculated throughput from the inverse of mean ns per call, then took the median of the six percentage differences. We used the same method for the latency percentiles. A win means higher throughput or lower p99. Separate medians and median paired ratios can differ. Relative values compare matching workload and API calls.

| Index | Public operation | Mean time per call, baseline → integrated | Paired throughput (wins) | Paired p99 (wins) | Δ allocations/call |
|---|---|---:|---:|---:|---:|
| memory | Get, repeated cached key | 101.40 → 101.07 ns | +1.9% (3/6) | +0.0% (1/6) | +0.0 |
| memory | Put, overwrite | 1.18 → 0.88 ms | +25.7% (4/6) | -22.1% (4/6) | -1.0 |
| memory | Delete, existing key | 0.77 → 0.94 ms | -14.8% (1/6) | +37.1% (3/6) | +0.0 |
| memory | BatchGet, 32 adjacent DATA records | 12.51 → 5.78 µs | +117.0% (6/6) | -34.1% (6/6) | -0.1 |
| memory | BatchGet, 32 scattered DATA records | 13.29 → 13.58 µs | -1.1% (3/6) | +13.0% (2/6) | +0.0 |
| memory | BatchPut, 16 scattered overwrites | 0.93 → 0.89 ms | -2.3% (3/6) | -7.2% (4/6) | -16.0 |
| memory | BatchPut, 16 adjacent overwrites | 0.89 → 0.99 ms | -8.8% (2/6) | +18.2% (2/6) | -16.0 |
| memory | BatchDelete, 16 existing keys | 0.90 → 0.99 ms | -8.7% (1/6) | +17.6% (2/6) | -0.0 |
| btree | Get, repeated cached key | 113.15 → 111.26 ns | +4.6% (4/6) | -45.0% (4/6) | +0.0 |
| btree | Put, overwrite | 0.79 → 0.96 ms | -11.8% (2/6) | +40.5% (2/6) | -183.3 |
| btree | Delete, existing key | 1.02 → 1.14 ms | -9.8% (2/6) | +27.3% (2/6) | +0.0 |
| btree | BatchGet, 32 adjacent DATA records | 25.65 → 17.64 µs | +46.8% (6/6) | -25.9% (6/6) | -0.1 |
| btree | BatchGet, 32 scattered DATA records | 26.35 → 27.17 µs | -1.8% (2/6) | +0.9% (3/6) | +0.0 |
| btree | BatchPut, 16 scattered overwrites | 1.29 → 1.03 ms | +20.7% (6/6) | -39.5% (6/6) | -2674.4 |
| btree | BatchPut, 16 adjacent overwrites | 1.12 → 1.08 ms | +1.2% (4/6) | -3.6% (4/6) | -1571.5 |
| btree | BatchDelete, 16 existing keys | 1.28 → 1.31 ms | +5.5% (4/6) | +0.4% (3/6) | -0.0 |

The table uses 64-byte values and a read concurrency limit of 8. For cache misses the working set was explicitly removed from the value cache. Seed order determined physical adjacency. The warm 32-record read cases used 1,600 calls per run. A larger control applied a page-cache eviction advisory before every call. For eight groups of four neighboring keys, that control showed +63.3% throughput (6/6 pairs), -17.5% p99 in Memory mode and +59.2% throughput (6/6 pairs), -19.1% p99 in B+tree mode. Cold sweeps reported Linux process read_bytes.

## Adoption decisions

1. **Large-record header + key read — deferred.** Both implementations reduced a 64 KiB value with a 16-byte key from three logical DATA reads to two. The 8 KiB pool variant delivered -7.0% throughput (2/6 pairs), +6.4% p99 for a 16-key Memory batch. A right-sized buffer saved one allocation per key, but the 64 KiB B+tree Get after cache advice showed -8.3% throughput (0/6 pairs), +26.5% p99. Neither variant delivered a consistent public API gain across 9 KiB and 64 KiB values.
2. **Value LRU — adopted.** A hit on the current head skips relinking. Overwrites reuse the entry and replace its owned key and value, so the Memory index and the LRU can share the latest key allocation. Capacity accounting, eviction order and per-batch duplicate marking remain exact. The isolated, ownership-corrected cache overwrite produced +27.1% throughput (6/6 pairs), -63.0% p99 and saved one allocation per cached overwritten key. A public repeated-key Memory Get produced +24.9% throughput (6/6 pairs), -25.1% p99.
3. **Fixed-width B+tree overwrite — adopted.** The transaction copies the encoded page and patches its address or child pointer, then recalculates the checksum. Published pages and snapshots stay immutable. A separate 16,384-key index test measured median overwrite time of 33.40 → 6.92 µs and 369 → 21 allocations/op. Across the six public 384-call pairs, isolated B+tree Put overwrites showed +17.3% throughput (5/6 pairs), -7.7% p99 and scattered BatchPut16 showed +15.0% throughput (4/6 pairs), -4.4% p99. The earlier 128-call test gave -28.1% throughput (1/6 pairs), +40.8% p99 for single Put. Sync time varies substantially across runs.
4. **Reusable directory descriptor — deferred.** This saved an open/close pair and roughly 168 B and three allocations per ordinary commit, at a cost of one permanent descriptor per open database. The longer isolated run showed +5.9% throughput (3/6 pairs), +23.1% p99 for a Memory Put and +27.9% throughput (4/6 pairs), -16.8% p99 for a B+tree Put. The tradeoff did not justify changing every open database.
5. **Adjacent BatchGet — adopted for small read plans.** Plan a range only when the first two physical reads are contiguous, the plan has at most 32 descriptors, and every grouped record fits in the same 8 KiB window in one DATA file. Up to 32 small contiguous records then share one range read. Multiple jobs respect the DB-wide concurrency limit. Returned values retain their own storage and each record checks key, sequence and checksum. The first prototype made caller key slices escape on scattered requests. The revised one borrows keys through private result slots. A larger-plan prototype improved throughput but raised p99 on warm 256-key Memory batches. The merged version leaves those plans on the per-record scheduler.

## Prior 16-key write regression

We compared `6c2c3d2` and `357e758` separately with the same hook instrumentation, one writer, a parked cleaner, and exactly 128 timed calls per condition per process. Every sample verified the committed sequence and whether rotation occurred. Each run reopened the database and checked the keys.

For 4 MiB segments and 64 KiB values, the paired mean call time increased 3.6% for BatchPut and 5.0% for BatchDelete without rotation. The median sum of measured sync phases rose from 1.126 to 1.211 ms for Put and from 0.660 to 0.702 ms for Delete. Index staging took about 4 µs, uncontended writer wait under 0.1 µs. With rotation, mean call time fell 5.3% and 2.1%. All four smaller-segment controls were faster on mean. This isolates the sync contribution under a parked cleaner. The earlier live-cleaner result can also include contention.

## Measurement boundary

The campaign started on 2026-11-21 JST with Go 1.27.4 linux/arm64, the latest stable Go then available. Docker Desktop used a private ext4 volume on LinuxKit, with four CPUs and GOMAXPROCS=4. The official image digest for every arm was `sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. Timing arms ran sequentially, alternating order for six matched pairs, with fresh databases under /data. The public write confirmation used 8,192 seeded keys and 384 calls per operation per round. All retained write timings include DATA and CURRENT publication. Each process reported its own allocation counters and latency samples. p99 of a short cold sweep can represent only one or two samples, and observed differences across six pairs are descriptive. These are targeted foreground workloads with a single caller and parked cleaner.
