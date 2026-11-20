# Selectable index comparison

Tarp’s exact in-memory map and its derived 8 KiB B+tree ran under identical Linux/arm64 conditions. Each of three rounds ran both backends, first at 2,048 live keys and then at 20,480. Each backend/cardinality combination ran in a fresh process. The table reports the median of the three per-process nearest-rank percentiles. Each time is an observed API call, including simple result validation.

## Point and batch operations

Times in microseconds on the 20,480-key fixture. A page load means a read from the private B+tree file. The ratio counts loads per call, including all 32 keys of a batch.

| Operation | Samples per round | Memory p50 / p95 / p99 | B+tree p50 / p95 / p99 | B+tree pages per call |
| --- | ---: | ---: | ---: | ---: |
| Hot Get | 256 | 0.17 / 0.17 / 0.38 | 0.17 / 0.17 / 0.38 | 0.00 |
| Value-cold Get | 256 | 0.71 / 0.96 / 2.67 | 12.79 / 16.25 / 19.04 | 0.98 |
| Missing Get | 256 | 0.08 / 0.12 / 0.17 | 10.58 / 13.25 / 15.75 | 0.96 |
| Contiguous BatchGet(32) | 256 | 12.42 / 15.29 / 22.62 | 38.17 / 56.21 / 129.33 | 1.09 |
| Scattered BatchGet(32) | 256 | 13.25 / 18.33 / 21.46 | 343.38 / 526.38 / 1,361.08 | 28.73 |
| Durable overwrite | 96 | 669.17 / 867.17 / 2,561.75 | 761.08 / 2,148.96 / 5,123.46 | 0.96 |
| Durable BatchPut(32) | 32 | 716.42 / 880.08 / 1,270.21 | 1,947.38 / 3,642.25 / 5,092.42 | 31.94 |
| First Keys yield | 64 | 307.25 / 1,652.00 / 2,256.42 | 0.42 / 0.54 / 18.25 | 0.02 |
| First Scan yield | 64 | 297.21 / 1,785.00 / 2,353.62 | 0.88 / 1.17 / 11.62 | 0.00 |
| Durable overwrite during paused Scan | 96 | 677.29 / 838.67 / 3,471.67 | 704.21 / 2,033.58 / 2,167.71 | 0.99 |
| 80% Get / 20% durable overwrite | 256 | 1.50 / 742.17 / 837.54 | 15.08 / 787.79 / 1,815.42 | 1.00 |
| 9 KiB shared-prefix Get | 256 | 7.12 / 10.04 / 30.25 | 685.17 / 1,912.62 / 2,074.00 | 119.12 |
| Durable overwrite concurrent with Backup | 96 | 681.88 / 3,477.00 / 7,406.46 | 753.04 / 2,147.67 / 2,291.96 | 0.96 |

The mixed trace contains one durable Put for every four Get calls on distinct keys. The overflow trace cycles through 128 additional 9,224-byte keys. All share the same binary 9 KiB prefix, and each suffix is a big-endian integer. A final foreground writer runs while a separate worker repeatedly backs up this extended fixture. The first timed write releases the background worker to copy its captured layout.

## Throughput and index page I/O

Throughput is the median number of completed calls per second over the three 20,480-key runs, including loop overhead. Page loads are medians of the instrumented B+tree reads per call, grouped by page kind.

| Operation | Memory calls/s | B+tree calls/s | Branch / leaf / overflow reads per B+tree call |
| --- | ---: | ---: | ---: |
| Hot Get | 5,102,956 | 4,833,928 | 0.00 / 0.00 / 0.00 |
| Value-cold Get | 1,232,001 | 77,404 | 0.00 / 0.98 / 0.00 |
| Missing Get | 7,613,383 | 94,513 | 0.00 / 0.96 / 0.00 |
| Contiguous BatchGet(32) | 78,503 | 23,341 | 0.00 / 1.09 / 0.00 |
| Scattered BatchGet(32) | 71,689 | 2,790 | 0.00 / 28.73 / 0.00 |
| 80% Get / 20% durable overwrite | 6,628 | 5,305 | 0.00 / 1.00 / 0.00 |
| 9 KiB shared-prefix Get | 120,833 | 1,320 | 0.00 / 0.83 / 118.29 |
| Durable overwrite concurrent with Backup | 1,047 | 1,083 | 0.00 / 0.96 / 0.00 |

## Recovery, maintenance and full enumeration

Each cell is the median of three whole operations in milliseconds. The open DB mode selects the backup workspace. Standalone verification and restore use the same implementation in both arms.

| Operation | Memory, 2,048 | B+tree, 2,048 | Memory, 20,480 | B+tree, 20,480 |
| --- | ---: | ---: | ---: | ---: |
| OpenExisting | 0.70 | 1.17 | 8.93 | 10.00 |
| Keys | 0.24 | 0.45 | 4.37 | 5.62 |
| Scan | 1.03 | 1.22 | 11.98 | 13.69 |
| Backup | 2.94 | 4.40 | 16.90 | 20.51 |
| VerifyBackup | 0.92 | 0.95 | 8.44 | 8.94 |
| VerifyBackupWithScratch | 1.36 | 1.34 | 9.58 | 11.02 |
| Restore | 3.14 | 3.17 | 20.09 | 21.86 |
| Compact | 10.14 | 74.19 | 63.82 | 496.01 |

## Memory and index space

After reopening, the driver warms one key, forces Go GC and samples heap and resident process memory before the hot phase. The index page gauge charges cache entries and page buffers. The configured disk index budget also reserves memory for operations and a maintenance backend. Go, file metadata, keys returned to callers, the measurement queries and transient work use additional memory. Sampled heap peaks include recent allocations awaiting GC. Sampling can miss short spikes.

| Mode / keys | After-GC process heap, MiB | Process RSS, MiB | Resident index page charge, bytes | Derived file size after Open, bytes |
| --- | ---: | ---: | ---: | ---: |
| memory / 2,048 | 1.14 | 13.29 | 0 | 0 |
| btree / 2,048 | 0.97 | 13.39 | 108,992 | 0 |
| memory / 20,480 | 3.18 | 15.73 | 0 | 0 |
| btree / 20,480 | 1.12 | 15.29 | 259,904 | 696,320 |

The map holds its complete index in Go heap and reports zero paged-backend charges. The next table gives the median of the per-round sampled heap peaks (MiB) during long operations.

| Mode / keys | Open | Backup | VerifyBackup | Restore | Compact |
| --- | ---: | ---: | ---: | ---: | ---: |
| memory / 2,048 | 1.90 | 2.53 | 1.43 | 2.33 | 1.88 |
| btree / 2,048 | 1.73 | 3.30 | 1.42 | 2.31 | 3.95 |
| memory / 20,480 | 4.51 | 7.85 | 6.41 | 6.49 | 12.36 |
| btree / 20,480 | 1.98 | 3.59 | 4.05 | 4.14 | 5.54 |

## Default disk-index budget

Each round included a fresh B+tree run with the 16 MiB default budget on the same 20,480-key fixture. The minimum-budget runs above use 1 MiB. This table compares the median per-process p99 and page loads per call for each budget, then gives the observed cache charge and heap after opening.

| Operation | 1 MiB p99 (µs) | 16 MiB p99 (µs) | 1 MiB reads/call | 16 MiB reads/call |
| --- | ---: | ---: | ---: | ---: |
| Value-cold Get | 19.04 | 3.83 | 0.98 | 0.00 |
| Missing Get | 15.75 | 1.04 | 0.96 | 0.00 |
| Scattered BatchGet(32) | 1,361.08 | 92.04 | 28.73 | 0.00 |
| 80% Get / 20% durable overwrite | 1,815.42 | 1,839.38 | 1.00 | 0.00 |
| 9 KiB shared-prefix Get | 2,074.00 | 597.83 | 119.12 | 0.00 |

The 1 MiB and 16 MiB configurations initially charged 259,904 and 964,160 bytes of resident index pages, respectively. After-GC process heap was 1.12 and 1.78 MiB. The 16 MiB default leaves room for repeated point reads, batch lookups and overflow pages on this fixture. Use the 1 MiB minimum when limiting memory takes precedence and budget for the additional reads shown above. A working set that exceeds either budget can incur page reads.

## Reader retention, page-cache high water and spill

The table shows the B+tree just after 96 durable overwrites while an older Scan is paused. Page-cache, transient read-workspace and scratch peaks are cumulative since Open. The read-workspace peak charges reserved concurrent search capacity. A paused Scan releases this reservation between yields. Logical scratch lengths include sparse regions and allocator spill files. Retired pages can be reused once the oldest reader releases its generation and a writer reclaims them. Spill time sums synchronous dirty-page writes inside the timed operation.

| Keys | Active views / generations | Retired pages | Peak page charge, bytes | Peak read workspace / limit, bytes | Peak scratch, MiB | Evictions | Spill writes / time (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2,048 | 1 / 1 | 192 | 259,904 | 49,152 / 98,304 | 1.60 | 248 | 174 / 0.55 |
| 20,480 | 1 / 1 | 192 | 259,904 | 49,152 / 98,304 | 2.39 | 286 | 162 / 0.51 |

## Interpretation

At 20,480 keys, a Value LRU hit loaded zero index pages in every run. Median hot Get was 0.17 µs for Memory and 0.17 µs for B+tree. Timings this short are sensitive to timer resolution and scheduling.

At the minimum B+tree budget, p99 was 7.1× Memory for a value-cold point lookup and 63.4× for a scattered 32-key batch. Keys with a 9 KiB common prefix require exact comparisons along overflow chains. Compact took 7.8× as long as Memory on this fixture. These measured cold paths and maintenance costs favor keeping Memory as the default. B+tree offers a bounded index for larger datasets and streams the first enumeration result without copying the entire index.

## Environment and reproduction

The benchmark used Go 1.27.4 in `docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`, Linux/arm64, kernel 6.12.76-linuxkit, Docker Desktop’s `desktop-linux` context, a private Docker volume, four CPUs and `GOMAXPROCS=4`. Each run seeded the same ascending 16-byte keys and 64-byte values using the Memory backend, then reopened the files in the selected mode. Value LRU capacity was 64 KiB, the B+tree budget 1 MiB or 16 MiB and the DATA segment target 8 MiB. The workloads use deterministic distinct-key strides. “Value-cold” means an empty Value LRU and keeps guest filesystem pages warm. The 5 ms sampler runs only during long operations. The three rounds used backend orders Memory/B+tree, B+tree/Memory and Memory/B+tree. The additional 16 MiB trial followed the two matched modes at 20,480 keys in each round.

Linux recorded positive process `read_bytes` in 0 of 315 phase windows. A private index page load may be served from the guest page cache. The timings cover this guest storage configuration. Three rounds characterize the trade-off on these fixtures, and are insufficient for general tail-latency guarantees.

Raw output includes every latency sample, Linux process and memory counters, index counters and source-file hashes in [index-modes.jsonl.gz](index-modes.jsonl.gz). SHA-256 of compressed data is `04dcb2fc6f5fdf3acd3c824cb92465f07c6d33682334248ee7569ed3d43329e4`. The source content manifest SHA-256 is `ffb40cb7503f539532fe507aef2362958ec7afe1f23b8a5a035714ea7ca5e131`. The Linux test executable SHA-256 is `b31db971b071a67c85238fe59bbac444bc1dcc1cfef6ca4c3d01ecc5df8d06f2`. The implementation and fixture match commit `531b8136035cead556046e1ef8b6d162e4fb648a`, based on v0.8.0. A later change adds error handling for an index address that references a missing DATA file. The timings above precede that change.

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
