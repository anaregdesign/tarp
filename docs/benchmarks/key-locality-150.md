# Physical key locality with append-only updates

[Issue #150](https://github.com/anaregdesign/tarp/issues/150) evaluates a layout for reading all outgoing edges of one vertex. Each edge occupies its own record, with the source vertex first in the encoded key. This report measures what happens to the latest values in DATA as updates accumulate.

Today each successful Open starts an incremental cleaner. It reclaims obsolete records and repacks eligible small sealed files. Ordinary full Compact is explicit and copies live records in source order. Increasing cleaning frequency alone therefore gives no encoded-key ordering guarantee. [Current compaction behavior](../../README.md#backup-restore-and-maintenance)

**Evaluation decision.** Prototype an opt-in, encoded-key-ordered full checkpoint. Keep each foreground Put or Delete on the ordinary append path. Defer the next layout checkpoint until the expected savings for prefixes actually read can pay for it. Repeat updates to one key can leave a single exception indefinitely. Where possible, carry forward entire unchanged snapshot files at the next global checkpoint. The measured locality benefit supports this direction. File reuse, sustained write latency at production scale, and authoritative recovery under failure still require qualification before adoption.

## What a real Tarp prefix read saw

The fixture starts with 16,384 edges in shuffled insertion order, organized as 256 source vertices with 64 edges each. Records are 512 B including key and header. We compared the existing checkpoint writer, a control that stages validated records in source order, and an experimental writer that stages those same records and sorts by encoded key. All use the same B+tree and the same prefix reader from [draft PR #149](https://github.com/anaregdesign/tarp/pull/149). Each arm wrote a real checkpoint and backup. Each trace restored the backup, appended updates in batches of 128, closed, reopened, and verified every live key and value. Background cleaning and the asynchronous parity worker were parked for this comparison. Checkpoint publication and its own parity protection ran normally.

| Updates since checkpoint | Source-order pages/prefix | Key-order pages/prefix | Source-order cold read | Key-order cold read |
| --- | ---: | ---: | ---: | ---: |
| None | 70.89 | 9.00 | 296 KiB · 2.34 ms | 112 KiB · 0.084 ms |
| 4,096 to the first key | 70.90 | 9.01 | 296 KiB · 1.61 ms | 112 KiB · 0.134 ms |
| 4,096 to the last key | 70.90 | 9.01 | 300 KiB · 1.77 ms | 112 KiB · 0.092 ms |
| 4,096 randomly distributed | 71.16 | 24.82 | 288 KiB · 1.67 ms | 180 KiB · 0.487 ms |

Page counts cover all 256 prefixes. Each page is a distinct (DATA file, 4 KiB file offset page) containing any part of a latest record. Cold read bytes and time are medians over three sampled vertices (first, middle, last) in each of three trials, with DATA evicted ahead of each read and the index already warm. The byte counter is Linux process-attributed storage I/O, including read-ahead, in Docker Desktop's VM. The host filesystem and SSD decide physical block placement. The runner checks a positive full-DATA read control and verifies every value returned by each measured prefix scan.

The all-vertex average masks the extra lookup for the single frequently updated edge. On the affected first vertex the sorted layout moved from **9 to 11 pages**, and the median cold read moved from 112 to 120 KiB. The middle vertex stayed at 9 pages. Both first and last traces displaced one live record and touched only one of nine base files. The first file held 1,048,128 B and the short last file held 4,160 B. File reuse could thus preserve eight files, although even a deferred first-key rewrite would eventually replace up to approximately 1 MiB at this segment setting. Spread changed 3,667 distinct keys and touched all nine files. The dirty inventory measures possible reuse for these overwrites. The experiment did not publish a checkpoint reusing old files.

All three checkpoints produced 8,389,184 B of snapshot DATA. Median checkpoint times were 139.58 ms in source order, 137.12 ms staged in source order and 109.91 ms sorted. The three sorted timings were 241.95, 109.91 and 108.45 ms. The in-memory staging prototype caps encoded payload at 32 MiB and rows at 65,536. It cannot establish a speedup for a production external sort. Cumulative Go allocation during checkpoint was 274.67, 286.62 and 176.36 MiB respectively. Peak resident memory, scratch disk and overlap with real writer traffic during this sorted checkpoint were not measured.

## When a bounded rewrite buys locality

A separate regular-file experiment starts with 4,096 sorted keys in sixteen 128 KiB ranges. Each range holds 256 fixed-size records. It appends 2,048 updates (1 MiB of foreground records) and issues one simulated 32-edge prefix lookup after every two updates. Query footprints are computed from actual record addresses as the stream progresses. Maintenance copies live bytes between files and checks results against a separate logical oracle. This experiment omits sync, publication, parity, concurrent reader pins and crash recovery.

Each cell gives **mean 4 KiB pages per query / extra written DATA bytes as a multiple of foreground record bytes**. Seed writes and foreground writes are excluded from the numerator. An extra 0.50× means 1.50× total DATA output during the update stream, before metadata or parity.

| Policy | First key | Hot 96 keys | Uniform updates | Hot range with deletes/reinserts |
| --- | ---: | ---: | ---: | ---: |
| Append | 4.75 / 0× | 15.50 / 0× | 10.75 / 0× | 16.75 / 0× |
| Rewrite all ranges after each 128 updates | 4.75 / 32× | 9.26 / 32× | 4.52 / 32× | 6.91 / 31.21× |
| One range when at least 25% displaced, every epoch | 4.75 / 0× | 9.57 / 1.88× | 9.45 / 1.13× | 8.08 / 1.12× |
| Read-weighted whole range, budget 0.50× | 4.75 / 0× | 14.03 / 0.50× | 10.41 / 0.38× | 12.10 / 0.40× |
| Read-weighted sorted patch, budget 0.25× | 4.75 / 0× | 14.03 / 0.19× | 10.38 / 0.14× | 16.75 / 0.02× |

The last-key trace also gave 4.75 pages for every policy. The full rewrite restored order after each epoch, but by the next sampled read that same key had already moved again. For the hot or churn traces 75% of queries target the hot vertices, plus accidental hits among the remaining random queries. Uniform uses uniform reads. The churn trace directs 75% of writes to the first range and schedules a Delete approximately every fifth operation. Read-weighted policies rank candidates using the previous epoch's query counts.

The budgeted whole-range arm allows a single range of up to 128 KiB after every four epochs if at least 64 distinct live keys have moved. The patch arm instead needs at least 32 currently live keys in the log and packs up to 128 displaced keys into a single sorted file of up to 64 KiB, incorporating surviving prior patch entries. It can postpone a range that exceeds the cap. Hot-96 achieved the same page footprint with 192 KiB of patch output versus 512 KiB of full-range output. Under churn, the bounded whole-range copy reduced the footprint by 27.7%, while the patch yielded effectively zero savings. Both gave only a 3–4% reduction for uniform reads and writes at these budgets. A patch adds implementation and read-path complexity. Keep it as a secondary option for a proven stable hot set.

## Foreground write interference

The third experiment used unmodified Tarp with default options, the normal cleaner and parity workers, and a single writer issuing durable public Put calls. The fixture had 8,192 keys with 512 B values. Each of eight paired trials per trace ran all four arms on closed clones with the identical sequence of 256 updates. Arm order rotated. Each trial reopened and checked all 8,192 keys. The near-budget side worker copied 16 KiB after the start of each 64-Put interval on the same filesystem and synced its output file and directory. The stress arm copied 1 MiB per interval. The Compact arm ran the actual full checkpoint once per interval. Work began alongside foreground writes and each interval joined the worker.

| Worker | First key, throughput | Spread, throughput | First key, Put p99 | Spread, Put p99 |
| --- | ---: | ---: | ---: | ---: |
| Side copy, 0.5× value bytes | 98.5% | 98.0% | 1.04× | 1.00× |
| Side copy, 32× value bytes | 94.8% | 96.3% | 0.98× | 1.03× |
| Full Compact every 64 puts | 64.3% | 67.8% | 1.71× | 1.63× |

These are medians of paired ratios to default. Throughput includes the worker joins. The no-extra-worker median throughput was 656 and 603 updates/s for the first-key and spread traces. The small side copy directly overlapped just 34 and 36 of 2,048 Put calls respectively. Its trial-level p99 ratios spanned 0.70–2.41× and 0.60–4.10×. This measures I/O interference from a simple warm-source copy. A full implementation must also find live data, validate it, generate parity and publish the manifest. Actual full Compact overlapped 1,942 and 1,933 calls, reducing throughput in every paired trial. Applying it every 64 writes is an intentionally aggressive scheduling control.

## Proposed scheduling and implementation

1. **Write each Edge once on the foreground DATA path.** Encode its source and destination with an unambiguous prefix boundary. Existing Put/Delete advances the winning version at the end of the log, so an overwrite of the smallest key has no reason to shift its neighbors. A prefix scan uses the index to resolve the current addresses in both the sorted base and the log. Use the same encoded-key ordering and layout policy for all index modes. The measured prefix reader used the B+tree.
2. **Pay for a sorted base at an existing full checkpoint or after initial bulk loading.** Preserve original versions, verify input and bound the sort workspace. On a later global checkpoint, reuse each immutable sorted base file whose entire key range and winning versions still match. Write changed spans into new sorted files. Include every captured insertion and deletion, then publish one complete manifest at a single cutoff with the concurrent tail preserved. File reuse can lower output I/O. An initial implementation may still need to validate the entire input at each checkpoint. [Publication, repair, backup and recovery design](key-locality-integration.md)
3. **Schedule based on future reads and the bytes the job must move.** Track a bounded set of scanned ranges, displaced *live* bytes, observed pages per scan, and read frequency. A thousand updates of one key count as one live exception. Sealed-log garbage collection continues on its own existing criteria. For a candidate, estimate how many cold page misses it can save before updates scatter it again. Compare those expected reads with source reads, DATA and parity writes, global validation, sync and staging overhead. For scale, a 128 KiB range copy that reads and writes its full contents transfers 256 KiB. It takes 16 cold scans saving four 4 KiB pages each just to match that byte count, before overhead or differences in read/write cost.
4. **Start with deferral and a budget.** For an evaluation build, start considering a range near 25% displaced live bytes. Use positive estimated read savings and a cap of 0.5 additional DATA bytes per committed foreground record byte as initial tuning values. The range-policy measurements used equal-size records and counted keys. Calibrate the byte-based version with variable-size real edges. A global checkpoint can run only when its *whole* job fits the accumulated allowance and available disk and writer headroom. A file that changes once in each of many spans can make a wide scattered update too expensive. Defer it until a read-heavy period or an already necessary full checkpoint. A patch or independent per-range publication would be a separate design and must preserve recovery semantics.
5. **Gate adoption on end-to-end measurements.** Proposed acceptance bounds are at least 95% of baseline sustained foreground throughput and at most 10% higher Put p99, with demonstrable reductions in cold read bytes on a representative Lantern trace. Test with at least 1,000 Puts actually overlapping maintenance per skew pattern, paired repetitions and uncertainty estimates. Record peak scratch, retained files, full validation bytes, DATA/PARITY write amplification and reader latency as well as throughput. Exercise default 4 MiB and smaller segment sizes, variable record lengths, mixed deletes and inserts across file fences, and files larger than the sort budget. Validate failure, retry, online backup and reopened versions before any authoritative publication change. The current 0.5× side-copy result is insufficient to accept those bounds as met.

## Reproduction

The decision run used experiment commit `9041abccfe2283eca5c048bfc48bbb5b81dfd3c1`, prefix reader `678dd2c9096e6cac3a12e6cc737a91dde8cf0e41`, Go 1.27.4 on linux/arm64, and the official image digest `sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. One private ext4 volume and CPU affinity 2–3 were shared by all arms. The source patches, binary digests, kernel, filesystem and raw samples are in the [decision archive](key-locality-150-raw.tar.gz). Its SHA-256 is `1a78dc3536ac94c4d936790460f7e43c34edca282f686919754e88a05c006f4e`.

To inspect the existing results from the repository root:

```sh
tar -xzf docs/benchmarks/key-locality-150-raw.tar.gz -C /path/to/empty/directory
python3 benchmark/scripts/key-locality-summary.py /path/to/empty/directory/decision
```

To rerun on a Linux Docker daemon with two available CPU IDs and an ext4 volume, make the pinned reader available in the local clone and use a fresh results path:

```sh
git fetch origin refs/pull/149/head
bash benchmark/scripts/container-key-locality.sh /path/to/new/results 9041abccfe2283eca5c048bfc48bbb5b81dfd3c1
python3 benchmark/scripts/key-locality-summary.py /path/to/new/results
```

Use `TARP_LOCALITY_CPUSET` to select two valid CPU IDs on another machine and record the change. The summary includes all policy arms, individual cold prefixes, and both throughput definitions.
