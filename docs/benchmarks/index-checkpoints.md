# Persistent B+tree checkpoint measurements

On a fixture with 6,144 live keys and eight generations of writes, a cold-advised `Open` took **84.36 ms** for full replay, **20.10 ms** for a checkpoint hit with `Full` validation, and **5.59 ms** for a hit with `Indexed` validation. `Full` verified the covered DATA digests while reusing the packed tree. `Indexed` read the selected file headers and the committed tail, deferring verification of covered payloads. [GitHub Actions run 44843469107](https://github.com/anaregdesign/tarp/actions/runs/44843469107)

## Open time and file reads

Each time is the median of three Opens on independent copies of the same fixture. Warm trials read all fixture files before timing. Cold-advised trials applied `fsync` and `POSIX_FADV_DONTNEED` to each file. Actual eviction and underlying device caches depend on the host.

| Fixture | Cache | Replay (ms) | Full (ms) | Indexed (ms) |
| --- | --- | ---: | ---: | ---: |
| Long overwrite history | Warm | 76.91 | 12.75 | 3.32 |
| Long overwrite history | Cold-advised | 84.36 | 20.10 | 5.59 |
| Mostly live | Warm | 7.06 | 4.28 | 3.12 |
| Mostly live | Cold-advised | 7.77 | 5.33 | 3.66 |
| After compaction | Warm | 6.32 | 6.47 | 6.47 |
| After compaction | Cold-advised | 7.69 | 7.48 | 7.79 |

Counts are bytes returned by DATA and INDEX file reads during each Open, including repeated header reads. They were identical across the six trials per fixture and arm. Both hit fixtures had 40,960 bytes of new records after the checkpoint. Every Indexed hit read zero covered DATA payload bytes. Compaction invalidated the checkpoint on the third fixture, so every arm replayed its complete compacted DATA.

| Fixture | Replay DATA | Full DATA | Indexed DATA | INDEX on Full / Indexed |
| --- | ---: | ---: | ---: | ---: |
| Long overwrite history | 15,772,864 | 15,771,840 | 42,176 | 295,136 |
| Mostly live | 2,007,616 | 2,007,488 | 41,280 | 295,024 |
| After compaction | 1,966,464 | 1,966,464 | 1,966,464 | 0 |

## Creation, Close and foreground writes

The published INDEX occupied 295,136 bytes for the long history and 295,024 bytes for the mostly-live fixture. A clean Close with that exact checkpoint took 0.052 ms and 0.035 ms respectively. With the 128-record tail, Close refreshed the checkpoint. Median refresh times across the 18 calls per fixture were 84.30 ms and 9.26 ms. After compaction, Close created a new checkpoint in a median 8.22 ms.

| Fixture | Explicit creation (ms, one call) | Creation peak heap above baseline / scratch (MiB) | Full payback, warm / cold | Indexed payback, warm / cold |
| --- | ---: | ---: | ---: | ---: |
| Long overwrite history | 85.52 | 2.24 / 5.71 | 2 / 2 opens | 2 / 2 opens |
| Mostly live | 7.33 | 2.05 / 0.38 | 3 / 4 opens | 2 / 2 opens |

Payback divides one measured creation cost by the median Open time saved against replay and rounds up. This estimates reuse of a single checkpoint. Subsequent refreshes incur their own costs. During Open, the median sampled peak heap increase / scratch size across six trials was 3.42 / 5.86 MiB for replay, 2.40 / 0.28 MiB for Full, and 1.89 / 0.28 MiB for Indexed on the long history. For mostly-live data, the corresponding pairs were 2.85 / 0.55, 1.92 / 0.28 and 1.87 / 0.28 MiB. The 2 ms sampler measured process heap objects and apparent lengths of open index and replay scratch files. Short peaks can fall between samples.

One additional pair of runs per hit fixture timed 32 serial durable overwrites while idle or while a checkpoint was being built. Long-history Put p95 went from 0.412 to 0.610 ms, with all 32 writes starting during the checkpoint. Mostly-live Put p95 went from 0.452 to 0.517 ms, with 30 of 32 starting during the checkpoint. These p95 values cover all 32 calls in each run.

## Fixture and environment

Each fixture used 6,144 16-byte keys, 256-byte values, batches of 128 and a parked automatic cleaner. The long history had 49,152 records at capture and the mostly-live fixture had 6,144. Both subsequently overwrote 128 keys. The third fixture compacted the long history and its tail. The replay arm omitted the optional INDEX from its copy. Arm order rotated across three rounds. Each Open used the B+tree backend with a 4 MiB working budget, a 1 MiB value cache and a 1 MiB target segment size. Every trial checked the full logical-state SHA-256 outside the timer. [Measurement driver](../../benchmark/index_checkpoint_test.go)

Commit `89aa45874d9dfdeee47b4824d4aa1b0baa187ff0` ran on a GitHub-hosted Ubuntu 24.04 runner with Linux `7.0.0-1014-aws` (amd64), a private ext4 Docker volume, two container CPUs and `GOMAXPROCS=2`. The campaign used `go version go1.27.4 linux/amd64` in the official Bookworm image `docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. The container script records the selected stable toolchain, image digest, kernel and mount details. [Reproduction script](../../benchmark/scripts/container-index-checkpoint.sh)

The 70 raw JSON rows include all timings, byte counters and foreground samples. The compressed file has SHA-256 `e10dec97f7f87038e282a5a0e7d7234eb97452a957aa13add5cc4162f9380418`. [Raw results](index-checkpoints.jsonl.gz)
