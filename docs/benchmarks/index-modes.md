# Selectable index comparison

Tarp’s exact in-memory map and its derived 8 KiB B+tree ran under identical Linux/arm64 conditions. Each of three rounds ran both backends, first at 2,048 live keys and then at 20,480. Each backend/cardinality combination ran in a fresh process. The table reports the median of the three per-process nearest-rank percentiles. Each time is an observed API call, including simple result validation.

## Point and batch operations

Times in microseconds on the 20,480-key fixture. A page load means a read from the private B+tree file. The ratio counts loads per call, including all 32 keys of a batch.

| Operation | Samples per round | Memory p50 / p95 / p99 | B+tree p50 / p95 / p99 | B+tree pages per call |
| --- | ---: | ---: | ---: | ---: |
| Hot Get | 256 | 0.17 / 0.25 / 0.75 | 0.17 / 0.17 / 0.67 | 0.00 |
| Value-cold Get | 256 | 1.12 / 1.54 / 4.29 | 12.50 / 18.62 / 59.38 | 0.98 |
| Missing Get | 256 | 0.17 / 0.25 / 0.50 | 10.58 / 13.79 / 58.12 | 0.97 |
| Contiguous BatchGet(32) | 256 | 18.29 / 38.21 / 76.62 | 45.17 / 67.96 / 249.33 | 1.32 |
| Scattered BatchGet(32) | 256 | 21.83 / 31.88 / 75.50 | 368.58 / 1,141.88 / 1,789.12 | 32.00 |
| Durable overwrite | 96 | 759.67 / 1,901.21 / 4,385.21 | 697.67 / 1,113.83 / 2,874.92 | 1.00 |
| Durable BatchPut(32) | 32 | 793.29 / 1,081.88 / 1,410.58 | 2,576.12 / 15,054.00 / 36,569.46 | 31.94 |
| First Keys yield | 64 | 369.17 / 1,910.79 / 2,128.50 | 0.42 / 0.92 / 44.75 | 0.02 |
| First Scan yield | 64 | 403.83 / 1,749.58 / 2,268.12 | 1.00 / 1.46 / 15.12 | 0.00 |
| Durable overwrite during paused Scan | 96 | 752.75 / 3,519.25 / 12,086.17 | 862.17 / 8,620.62 / 13,356.62 | 0.99 |

## Recovery, maintenance and full enumeration

Each cell is the median of three whole operations in milliseconds. The open DB mode selects the backup workspace. Standalone verification and restore use the same implementation in both arms.

| Operation | Memory, 2,048 | B+tree, 2,048 | Memory, 20,480 | B+tree, 20,480 |
| --- | ---: | ---: | ---: | ---: |
| OpenExisting | 0.88 | 30.38 | 11.78 | 304.42 |
| Keys | 0.33 | 0.42 | 5.10 | 7.47 |
| Scan | 1.54 | 1.11 | 14.76 | 14.62 |
| Backup | 8.80 | 74.34 | 15.71 | 332.26 |
| VerifyBackup | 1.03 | 1.06 | 9.30 | 9.48 |
| VerifyBackupWithScratch | 48.19 | 41.44 | 294.68 | 341.09 |
| Restore | 5.08 | 4.63 | 24.03 | 22.10 |
| Compact | 12.29 | 123.97 | 65.78 | 1,071.31 |

## Memory and index space

After reopening, the driver warms one key, forces Go GC and samples heap and resident process memory before the hot phase. The index page gauge charges cache entries and page buffers. The configured disk index budget also reserves memory for operations and a maintenance backend. Go, file metadata, keys returned to callers, the measurement queries and transient work use additional memory. Sampled heap peaks include recent allocations awaiting GC; sampling can miss short spikes.

| Mode / keys | After-GC process heap, MiB | Process RSS, MiB | Resident index page charge, bytes | Derived file size after Open, bytes |
| --- | ---: | ---: | ---: | ---: |
| memory / 2,048 | 1.14 | 13.18 | 0 | 0 |
| btree / 2,048 | 1.07 | 13.48 | 217,984 | 212,992 |
| memory / 20,480 | 3.18 | 15.46 | 0 | 0 |
| btree / 20,480 | 1.11 | 14.53 | 259,904 | 1,884,160 |

The map holds its complete index in Go heap and reports zero paged-backend charges. The next table gives the median of the per-round sampled heap peaks (MiB) during long operations.

| Mode / keys | Open | Backup | VerifyBackup | Restore | Compact |
| --- | ---: | ---: | ---: | ---: | ---: |
| memory / 2,048 | 1.90 | 2.54 | 1.44 | 2.33 | 2.84 |
| btree / 2,048 | 3.53 | 3.94 | 1.41 | 2.30 | 4.46 |
| memory / 20,480 | 4.85 | 7.86 | 6.33 | 6.73 | 12.84 |
| btree / 20,480 | 3.89 | 4.23 | 4.05 | 4.23 | 4.69 |

## Interpretation

Value LRU hits loaded zero index pages in every run. At the larger cardinality, contiguous and scattered BatchGet requests show the difference between sharing a leaf and touching many leaves. The paged backend provides a streaming first yield and limits its resident index pages as cardinality increases. Replaying DATA into a B+tree, validating online backup workspace and relocating records at Compact incur additional work. Cold-key and maintenance costs favor keeping Memory as the default for the workloads measured. Use B+tree when index residency is the limiting resource.

## Environment and reproduction

The benchmark used Go 1.27.4 in `docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`, Linux/arm64, kernel 6.12.76-linuxkit, Docker Desktop’s `desktop-linux` context, a private Docker volume, four CPUs and `GOMAXPROCS=4`. Each run seeded the same ascending 16-byte keys and 64-byte values using the Memory backend, then reopened the files in the selected mode. Value LRU capacity was 64 KiB, the B+tree budget 1 MiB and the DATA segment target 8 MiB. The workloads use deterministic distinct-key strides; “value-cold” means an empty Value LRU and keeps guest filesystem pages warm. The 5 ms sampler runs only during long operations. The three rounds used backend orders Memory/B+tree, B+tree/Memory and Memory/B+tree.

Linux recorded positive process `read_bytes` in 0 of 216 phase windows. A private index page load may be served from the guest page cache. The timings cover this guest storage configuration. Three rounds characterize the trade-off on these fixtures, and are insufficient for general tail-latency guarantees.

Raw output includes every latency sample, Linux process and memory counters, index counters and source-file hashes: [index-modes.jsonl.gz](index-modes.jsonl.gz). SHA-256 of compressed data is `c9e9caa84892249f02cb5027464d5061d78bf62a82d8c554c862f5f07b6423ad`. The source content manifest SHA-256 is `4cacd7c383adb20a488b8e9e465b5114f821df3c6385cc3795b6368324eba9d9`; the Linux test executable SHA-256 is `a91342c925d83b119fc3922142745527da8e4e4e7a00a2b76c3f3a0d578a8ff5`. The manifest fingerprints the implementation tested against the v0.8.0 base. Revisions may include later correctness or documentation changes.

With explicit benchmark authorization, compile the fixture once in the fixed image and invoke each arm in a fresh process. From the repository root, replace the image digest with the stable Go digest selected for a new campaign and record its version. For example:

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
      done
    done'
```
