# Durable Put after releasing two Scan snapshots

At production revision `adc1b21`, each independent episode started from a fresh default B+tree with 512 live keys. Every sampled Put acknowledged, both scans returned their full capture-time contents, and the final Close and reopen preserved all keys and the expected sequence. Read-only qualification probes sampled current root, allocator high water mark and the retired-page queue outside the timed interval. The 16-step and 256-step profiles used qualification commits `caef230` and `b1cc625` respectively. The separate page identity witness used `b368978`.

| Churn steps / Compacts | Matched pairs | Normal p50 / p95 / p99 (ms) | First after both scans release p50 / p95 / p99 (ms) | Median paired difference (ms) |
| --- | ---: | ---: | ---: | ---: |
| 16 / 2 | 500 | 0.654 / 2.546 / 7.451 | 0.710 / 2.417 / 3.410 | +0.059 |
| 256 / 8 | 200 | 0.707 / 0.975 / 4.789 | 1.001 / 1.969 / 3.583 | +0.283 |

Times cover one complete public durable Put. Percentiles use nearest rank on all independent episodes of the given arm. A difference is first-after-release minus the matched normal call for that pair. Compound metadata operations and storage sync latency also contribute to individual durations.

In the 16-step profile, the post-release measurement began with 406–442 queued pages (median 429). Its oldest Scan was held for 0.051–0.355 s (median 0.070 s). The normal arm began with zero queued retirement records. In 394 of 500 pairs the first-after-release call took longer. Normal p99 by round in ms was 3.342 / 5.146 / 7.451 / 8.114 / 2.574. First-after-release p99 by round in ms was 3.020 / 2.905 / 3.262 / 4.598 / 3.276. Each round contained 100 pairs.

In the 256-step profile, the post-release measurement began with 6,506–6,552 queued pages (median 6,530). Its oldest Scan was held for 0.770–8.098 s (median 0.937 s). The normal arm began with zero queued retirement records. In 192 of 200 pairs the first-after-release call took longer. Normal p99 by round in ms was 6.618 / 3.545. First-after-release p99 by round in ms was 3.722 / 3.236. Each round contained 100 pairs.

## Workload and controls

Keys were 16 bytes and values were 64 bytes. Each step performed a durable BatchPut overwriting 32 keys, a BatchDelete deleting eight other keys and a BatchPut reinserting those eight keys. The first Scan began before any churn and the second after step two. Both retained their different generations through all Compacts. Each Compact published a new checkpoint and reported nonzero pinned retired DATA. The two complete snapshot results were checked against independently copied maps and left suspended on their final yielded entries. Both release orders and both arm orders were balanced within each profile.

Both arms performed the same two final durable overwrites of distinct pre-existing keys, using the same key choices within each pair. The normal arm released the scans before the conditioning overwrite; that overwrite drained the backlog. The first-after-release arm performed its conditioning overwrite while both scans were pinned, released them, and timed its first subsequent Put. The timer enclosed the API call only. The keys, writes, Compacts, normalized initial options, total observed scan reads and final logical state matched within each pair. Independent automatic cleaning and parity workers were parked on both arms. The scans remained usable across manual compaction. The environment was Docker Desktop Linux/arm64 on macOS with an anonymous ext4 volume, four CPU quota and GOMAXPROCS=4. Both arms were executed serially in the same process per profile. The 16-step checkpoint and reopen returned sequence 1,282; the 256-step profile returned sequence 12,802. They used the same official Bookworm image digest and Go 1.27.4, resolved at the start of the campaign.

Every timed Put emptied the retirement queue, switched to a different root ID that had already been allocated, and left the allocation ID high water unchanged. The newest Put still had two pending COW pages in the public backend metrics. The standalone ID probe also walked live pages and read the IDs from the retirement queue before release, establishing that they were disjoint. After the timed Put it inserted and read back 4,096 temporary keys to demand new leaf pages. It recorded formerly queued IDs found under the new live root, deleted the temporary keys, and rechecked the original live contents and the extended commit sequence of 20,994 after reopen.

The independent witness stretched the same 256-step churn over at least 64 ms per step. The four episodes held the oldest snapshot for 18.24–18.38 s. Each episode found 41–42 distinct previously queued IDs under the current root after this growth. Its read-only probes and paced episodes are excluded from both latency distributions.

An initial identity fixture (`8e051f6`) overwrote the same 512 keys and its sampled live pages did not include IDs from the pre-release queue. The successful witness explicitly grew the tree to require more new pages and directly tracked those IDs. The pilot at `d2ba2a3` initially expected the backend’s total retired-page metric to reach zero. The metric includes the just-committed Put’s pending COW pages. Subsequent profiles separately verified that the queue itself reached zero and recorded both values.

## Evidence and reproduction

The three compressed JSONL files contain every sample, in acquisition order. [16-step observations](measured.jsonl.gz) · [256-step observations](sustained.jsonl.gz) · [page ID witness](identity.jsonl.gz). The environment and test logs accompany each file. The summary JSON files include per-round and order-split distributions. Recompute a summary with the command below.

```sh
gzip -dc sustained.jsonl.gz > /tmp/tarp-corner145-sustained.jsonl
python3 summarize.py /tmp/tarp-corner145-sustained.jsonl
```

Run the following commands from the qualification branch root with fresh output directories and the reviewed production base. The current driver supports all three shapes. Use the recorded qualification commit for an exact-source replay of a historical run. The 16-step source at `caef230` supports its round and pair parameters and uses 16 steps internally.

```sh
export CORNER145_GO_VERSION=go1.27.4
export CORNER145_GO_IMAGE=docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea
TARP_SCAN_RELEASE_STEPS=16 TARP_SCAN_RELEASE_ROUNDS=5 TARP_SCAN_RELEASE_PAIRS=100 benchmark/scripts/corner145-scans.sh /path/to/new/shallow
TARP_SCAN_RELEASE_STEPS=256 TARP_SCAN_RELEASE_ROUNDS=2 TARP_SCAN_RELEASE_PAIRS=100 benchmark/scripts/corner145-scans.sh /path/to/new/sustained
TARP_SCAN_RELEASE_STEPS=256 TARP_SCAN_RELEASE_ROUNDS=1 TARP_SCAN_RELEASE_PAIRS=2 TARP_SCAN_RELEASE_WITNESS=1 benchmark/scripts/corner145-scans.sh /path/to/new/witness
```
