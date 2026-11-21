# Selectable index comparison

Tarp’s exact in-memory map and its derived 8 KiB B+tree ran under identical Linux/arm64 conditions. Each of three rounds ran both backends, first at 2,048 live keys and then at 20,480. Each backend/cardinality combination ran in a fresh process. The table reports the median of the three per-process nearest-rank percentiles. Each time is an observed API call, including simple result validation.

## Point and batch operations

Times in microseconds on the 20,480-key fixture. A page load means a read from the private B+tree file. The ratio counts loads per call, including all 32 keys of a batch.

| Operation | Samples per round | Memory p50 / p95 / p99 | B+tree p50 / p95 / p99 | B+tree pages per call |
| --- | ---: | ---: | ---: | ---: |
| Hot Get | 256 | 0.17 / 0.21 / 0.33 | 0.17 / 0.17 / 0.33 | 0.00 |
| Value-cold Get | 256 | 0.75 / 1.08 / 2.83 | 11.83 / 16.21 / 18.46 | 0.98 |
| Missing Get | 256 | 0.08 / 0.17 / 0.25 | 9.50 / 12.96 / 15.00 | 0.96 |
| Contiguous BatchGet(32) | 256 | 13.50 / 17.46 / 22.67 | 35.96 / 51.00 / 96.75 | 1.09 |
| Scattered BatchGet(32) | 256 | 13.42 / 17.92 / 22.79 | 324.12 / 518.75 / 1,372.46 | 28.73 |
| Durable overwrite | 96 | 659.58 / 753.04 / 2,605.79 | 685.38 / 752.29 / 2,247.25 | 0.96 |
| Durable BatchPut(32) | 32 | 695.58 / 742.96 / 851.42 | 1,955.62 / 5,029.00 / 5,124.38 | 31.94 |
| First Keys yield | 64 | 271.46 / 1,689.08 / 2,662.46 | 0.42 / 0.54 / 17.62 | 0.02 |
| First Scan yield | 64 | 395.12 / 1,598.42 / 2,087.25 | 0.92 / 1.21 / 7.50 | 0.00 |
| Durable overwrite during paused Scan | 96 | 651.29 / 1,897.67 / 2,309.08 | 678.29 / 762.25 / 4,136.83 | 0.99 |
| 80% Get / 20% durable overwrite | 256 | 1.33 / 665.00 / 731.29 | 15.83 / 852.83 / 2,092.92 | 1.00 |
| 9 KiB shared-prefix Get | 256 | 6.79 / 9.12 / 26.38 | 660.88 / 1,827.62 / 2,049.29 | 119.12 |
| Durable overwrite concurrent with Backup | 96 | 653.92 / 847.62 / 3,919.42 | 705.83 / 1,879.54 / 2,468.96 | 0.96 |

The mixed trace contains one durable Put for every four Get calls on distinct keys. The overflow trace cycles through 128 additional 9,224-byte keys. All share the same binary 9 KiB prefix, and each suffix is a big-endian integer. A final foreground writer runs while a separate worker repeatedly backs up this extended fixture. The first timed write releases the background worker to copy its captured layout.

## Throughput and index page I/O

Throughput is the median number of completed calls per second over the three 20,480-key runs, including loop overhead. Page loads are medians of the instrumented B+tree reads per call, grouped by page kind.

| Operation | Memory calls/s | B+tree calls/s | Branch / leaf / overflow reads per B+tree call |
| --- | ---: | ---: | ---: |
| Hot Get | 4,726,125 | 4,788,806 | 0.00 / 0.00 / 0.00 |
| Value-cold Get | 1,139,256 | 80,999 | 0.00 / 0.98 / 0.00 |
| Missing Get | 7,111,111 | 104,323 | 0.00 / 0.96 / 0.00 |
| Contiguous BatchGet(32) | 72,147 | 23,746 | 0.00 / 1.09 / 0.00 |
| Scattered BatchGet(32) | 70,999 | 2,919 | 0.00 / 28.73 / 0.00 |
| 80% Get / 20% durable overwrite | 6,565 | 4,840 | 0.00 / 1.00 / 0.00 |
| 9 KiB shared-prefix Get | 138,750 | 1,352 | 0.00 / 0.83 / 118.29 |
| Durable overwrite concurrent with Backup | 1,374 | 1,163 | 0.00 / 0.96 / 0.00 |

## Recovery, maintenance and full enumeration

Each cell is the median of three whole operations in milliseconds. The open DB mode selects the backup workspace. Standalone verification and restore use the same implementation in both arms.

| Operation | Memory, 2,048 | B+tree, 2,048 | Memory, 20,480 | B+tree, 20,480 |
| --- | ---: | ---: | ---: | ---: |
| OpenExisting | 0.55 | 1.15 | 5.73 | 11.58 |
| Keys | 0.26 | 0.45 | 5.12 | 5.49 |
| Scan | 1.03 | 1.16 | 12.83 | 13.26 |
| Backup | 2.25 | 4.63 | 12.33 | 23.77 |
| VerifyBackup | 0.90 | 0.99 | 8.19 | 8.54 |
| VerifyBackupWithScratch | 1.38 | 1.37 | 10.57 | 11.40 |
| Restore | 3.40 | 6.38 | 18.72 | 23.75 |
| Compact | 10.61 | 62.85 | 60.26 | 527.20 |

## Memory and index space

After reopening, the driver warms one key, forces Go GC and samples heap and resident process memory before the hot phase. The index page gauge charges cache entries and page buffers. The configured disk index budget also reserves memory for operations and a maintenance backend. Go, file metadata, keys returned to callers, the measurement queries and transient work use additional memory. Sampled heap peaks include recent allocations awaiting GC. Sampling can miss short spikes.

| Mode / keys | After-GC process heap, MiB | Process RSS, MiB | Resident index page charge, bytes | Derived file size after Open, bytes |
| --- | ---: | ---: | ---: | ---: |
| memory / 2,048 | 1.14 | 13.26 | 0 | 0 |
| btree / 2,048 | 0.97 | 13.37 | 108,992 | 0 |
| memory / 20,480 | 3.18 | 15.55 | 0 | 0 |
| btree / 20,480 | 1.12 | 15.16 | 259,904 | 696,320 |

The map holds its complete index in Go heap and reports zero paged-backend charges. The next table gives the median of the per-round sampled heap peaks (MiB) during long operations.

| Mode / keys | Open | Backup | VerifyBackup | Restore | Compact |
| --- | ---: | ---: | ---: | ---: | ---: |
| memory / 2,048 | 1.00 | 1.49 | 1.43 | 2.33 | 2.64 |
| btree / 2,048 | 1.73 | 3.11 | 1.41 | 2.31 | 3.71 |
| memory / 20,480 | 3.35 | 6.32 | 6.33 | 8.06 | 12.08 |
| btree / 20,480 | 3.63 | 3.50 | 3.82 | 4.13 | 5.08 |

## Default disk-index budget

Each round included a fresh B+tree run with the 16 MiB default budget on the same 20,480-key fixture. The minimum-budget runs above use 1 MiB. This table compares the median per-process p99 and page loads per call for each budget, then gives the observed cache charge and heap after opening.

| Operation | 1 MiB p99 (µs) | 16 MiB p99 (µs) | 1 MiB reads/call | 16 MiB reads/call |
| --- | ---: | ---: | ---: | ---: |
| Value-cold Get | 18.46 | 2.83 | 0.98 | 0.00 |
| Missing Get | 15.00 | 1.04 | 0.96 | 0.00 |
| Scattered BatchGet(32) | 1,372.46 | 78.46 | 28.73 | 0.00 |
| 80% Get / 20% durable overwrite | 2,092.92 | 2,163.71 | 1.00 | 0.00 |
| 9 KiB shared-prefix Get | 2,049.29 | 797.42 | 119.12 | 0.00 |

The 1 MiB and 16 MiB configurations initially charged 259,904 and 964,160 bytes of resident index pages, respectively. After-GC process heap was 1.12 and 1.78 MiB. The 16 MiB default leaves room for repeated point reads, batch lookups and overflow pages on this fixture. Use the 1 MiB minimum when limiting memory takes precedence and budget for the additional reads shown above. A working set that exceeds either budget can incur page reads.

## Reader retention, page-cache high water and spill

The table shows the B+tree just after 96 durable overwrites while an older Scan is paused. Page-cache, transient read-workspace and scratch peaks are cumulative since Open. The read-workspace peak charges reserved concurrent search capacity. A paused Scan releases this reservation between yields. Logical scratch lengths include sparse regions and allocator spill files. Retired pages can be reused once the oldest reader releases its generation and a writer reclaims them. Spill time sums synchronous dirty-page writes inside the timed operation.

| Keys | Active views / generations | Retired pages | Peak page charge, bytes | Peak read workspace / limit, bytes | Peak scratch, MiB | Evictions | Spill writes / time (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2,048 | 1 / 1 | 192 | 259,904 | 49,152 / 98,304 | 1.60 | 248 | 174 / 0.56 |
| 20,480 | 1 / 1 | 192 | 259,904 | 49,152 / 98,304 | 2.39 | 286 | 162 / 0.48 |

## Interpretation

At 20,480 keys, a Value LRU hit loaded zero index pages in every run. Median hot Get was 0.17 µs for Memory and 0.17 µs for B+tree. Timings this short are sensitive to timer resolution and scheduling.

At the minimum B+tree budget, p99 was 6.5× Memory for a value-cold point lookup and 60.2× for a scattered 32-key batch. Keys with a 9 KiB common prefix require exact comparisons along overflow chains. Compact took 8.7× as long as Memory on this fixture. These measurements informed the original choice of Memory as the default. They predate the [long-key comparison fix](https://github.com/anaregdesign/tarp/pull/142). For current defaults and guidance on selecting a backend, see [Choosing the key index](../../README.md#choosing-the-key-index).

## Environment and reproduction

The benchmark used Go 1.27.4 in `docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`, Linux/arm64, kernel 6.12.76-linuxkit, Docker Desktop’s `desktop-linux` context, a private Docker volume, four CPUs and `GOMAXPROCS=4`. Each run seeded the same ascending 16-byte keys and 64-byte values using the Memory backend, then reopened the files in the selected mode. Value LRU capacity was 64 KiB, the B+tree budget 1 MiB or 16 MiB and the DATA segment target 8 MiB. The workloads use deterministic distinct-key strides. “Value-cold” means an empty Value LRU and keeps guest filesystem pages warm. The 5 ms sampler runs only during long operations. The three rounds used backend orders Memory/B+tree, B+tree/Memory and Memory/B+tree. The additional 16 MiB trial followed the two matched modes at 20,480 keys in each round.

Linux recorded positive process `read_bytes` in 0 of 315 phase windows. A private index page load may be served from the guest page cache. The timings cover this guest storage configuration. Three rounds characterize the trade-off on these fixtures, and are insufficient for general tail-latency guarantees.

Raw output includes every latency sample, Linux process and memory counters, index counters and source-file hashes in [index-modes.jsonl.gz](index-modes.jsonl.gz). SHA-256 of compressed data is `fb6123a0dce320a1507275da43fe37db2ae643985c8a3332342e2e4315cb7a58`. The source content manifest SHA-256 is `c003c767310d75172a6262e9755ffbe80fe081f72e8cecace9791a644511d070`. The Linux test executable SHA-256 is `4714067ee06af9f44b5798209e60fbf0e279b81f5e8d01e5441fdd3c51efc044`. The implementation and fixture match commit `53479b39d2a970da29e1c8ac67a4706b05170c06`, based on v0.8.0.

With explicit benchmark authorization, compile the fixture once in the fixed image and invoke each arm in a fresh process. From the repository root, replace the image digest with the stable Go digest selected for a new campaign and record its version. Use one unshared volume for the campaign.

```sh
docker run --rm --network none --cpus 4 \
  --mount "type=bind,src=$PWD,dst=/src,readonly" --mount type=volume,dst=/work \
  -w /src -e GOMAXPROCS=4 -e GOTOOLCHAIN=local -e GOWORK=off -e GOPROXY=off \
  -e TMPDIR=/work/tmp -e GOCACHE=/work/cache \
  docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea sh -ec '
    mkdir -p "$TMPDIR"
    go version
    go test -c -o /work/bench.test ./benchmark
    for round in 1 2 3; do
      modes="memory btree"
      if [ "$round" = 2 ]; then modes="btree memory"; fi
      for keys in 2048 20480; do
        for mode in $modes; do
          TARP_INDEX_TRIAL_KEYS="$keys" TARP_INDEX_TRIAL_MODE="$mode" \
            /work/bench.test -test.run "^TestIndexModeCampaign$" -test.v
        done
        if [ "$keys" = 20480 ]; then
          TARP_INDEX_TRIAL_BUDGET=16777216 TARP_INDEX_TRIAL_KEYS="$keys" TARP_INDEX_TRIAL_MODE=btree \
            /work/bench.test -test.run "^TestIndexModeCampaign$" -test.v
        fi
      done
    done'
```
