# Default index performance in the hosted VM

Tarp v0.8.0 and the selectable-index candidate ran the same measurement workload with the default options. The candidate used the Memory backend. For 64 B, 1,024 B and 4,096 B values, the changes in single-worker hot Get p50 were +25%, -11%, -33%, respectively. Manual full-checkpoint elapsed times changed by +41%, -53%, -19%, and automatic single-segment cleaning changed by +35%, -61%, -33%. Explicit B+tree mode has a separate cardinality and page-read [backend comparison](index-modes.md).

On the larger foreground-traffic fixture, Compact elapsed time changed by +304%, +24%, +4%. Warm Open after an existing manual checkpoint changed by +37%, +14%, -24%. At 64 B, every foreground Compact copied the same 4,097 records. All five final observations (1.87–2.88 s) exceeded the five baseline observations (0.49–0.85 s). This is a regression on the measured VM pair. A [same-host follow-up](compact-64-same-host.md) reproduced the 64 B slowdown and measured writer-lock contention during relocation.

Each cell shows **v0.8.0 → candidate (relative change)**. Positive percentages mean more time or memory for the named operation. Each size/commit combination used one shared hosted runner and five measurement windows within that VM. The host CPU models are listed below. Host differences and QEMU emulation can affect both the direction and size of timing deltas, especially for CPU-bound hot reads. The campaign defined no numeric SLO. [Detailed metrics by workload and size](index-vm.csv)

## Point reads and durable writes

Time in microseconds. Entries show pooled p50 except for the explicitly labeled tight loop, which reports the median of five means. The loops issue individual Get calls. The guest clears its page cache before each guest-OS-cold Get. Host page-cache and physical-media state are uncontrolled.

| Operation | 64 B | 1,024 B | 4,096 B |
| --- | ---: | ---: | ---: |
| Hot Get, 1 worker | 2.31 → 2.88 (+25%) | 2.25 → 2.01 (-11%) | 7.98 → 5.36 (-33%) |
| Hot Get, 4 workers | 2.46 → 3.15 (+28%) | 2.82 → 2.81 (-0%) | 8.61 → 5.87 (-32%) |
| Hot Get loop, 1 worker (mean per call) | 1.45 → 2.35 (+62%) | 6.75 → 3.54 (-47%) | 19.37 → 13.87 (-28%) |
| Value-LRU miss, guest OS warm | 11.54 → 18.45 (+60%) | 18.16 → 22.00 (+21%) | 76.68 → 53.26 (-31%) |
| Guest-OS-cold Get | 817.61 → 1,046.28 (+28%) | 1,032.75 → 811.28 (-21%) | 1,131.24 → 873.45 (-23%) |
| Mixed Get | 19.30 → 39.61 (+105%) | 40.80 → 40.05 (-2%) | 337.51 → 204.46 (-39%) |
| Mixed Put | 35,158.81 → 47,766.32 (+36%) | 64,624.02 → 31,156.59 (-52%) | 59,084.47 → 44,283.85 (-25%) |
| Durable Put without rotation | 8,056.53 → 10,937.71 (+36%) | 15,490.77 → 7,976.36 (-49%) | 13,297.14 → 8,997.61 (-32%) |
| Durable Put with rotation | 13,440.31 → 17,064.46 (+27%) | 20,853.53 → 12,310.60 (-41%) | 19,118.42 → 12,588.92 (-34%) |
| Durable Delete without rotation | 8,342.04 → 10,915.77 (+31%) | 13,813.97 → 7,532.06 (-45%) | 12,047.86 → 8,362.28 (-31%) |
| Durable Delete with rotation | 13,408.79 → 17,896.84 (+33%) | 21,822.43 → 12,710.15 (-42%) | 19,364.87 → 12,747.71 (-34%) |

## Upper tails

Pooled p99 in microseconds. Each cold Get profile supplied 160 observations, giving the two slowest calls substantial influence on its tail. Foreground interference uses the driver's bounded samples.

| Operation | 64 B | 1,024 B | 4,096 B |
| --- | ---: | ---: | ---: |
| Hot Get, 1 worker | 8.74 → 15.55 (+78%) | 67.91 → 23.16 (-66%) | 327.92 → 185.89 (-43%) |
| Hot Get, 4 workers | 45.59 → 60.94 (+34%) | 188.34 → 73.19 (-61%) | 1,158.65 → 624.90 (-46%) |
| Value-LRU miss, guest OS warm | 64.95 → 119.37 (+84%) | 391.09 → 130.92 (-67%) | 678.09 → 465.52 (-31%) |
| Guest-OS-cold Get | 2,030.97 → 2,334.61 (+15%) | 5,237.22 → 2,635.25 (-50%) | 4,981.88 → 3,493.51 (-30%) |
| Mixed Put | 389,951.55 → 67,077.37 (-83%) | 143,779.06 → 41,969.12 (-71%) | 77,767.65 → 497,170.56 (+539%) |
| Foreground Put during Backup + Compact | 139,327.03 → 77,156.68 (-45%) | 120,334.68 → 141,147.74 (+17%) | 151,560.02 → 198,685.12 (+31%) |

## Maintenance and recovery

Median elapsed time over five windows in milliseconds. The compact and cleaner fixture targets 32 full records per segment and has three surviving keys. Foreground-traffic measurements use a separate 4,097-key fixture. Cold Open purges the guest page cache immediately before reopening the captured layout.

| Operation | 64 B | 1,024 B | 4,096 B |
| --- | ---: | ---: | ---: |
| Full warm Scan | 62.63 → 71.99 (+15%) | 116.31 → 158.42 (+36%) | 361.18 → 462.44 (+28%) |
| Compact during paused Scan | 31.35 → 55.22 (+76%) | 65.77 → 39.55 (-40%) | 64.67 → 50.38 (-22%) |
| Automatic cleaner, one segment | 15.92 → 21.47 (+35%) | 44.71 → 17.43 (-61%) | 27.37 → 18.23 (-33%) |
| Manual full checkpoint | 28.13 → 39.70 (+41%) | 65.03 → 30.25 (-53%) | 47.37 → 38.16 (-19%) |
| Repack during foreground writes | 80.11 → 153.35 (+91%) | 155.47 → 85.52 (-45%) | 274.06 → 148.28 (-46%) |
| Backup during foreground traffic | 207.15 → 254.07 (+23%) | 1,213.14 → 1,543.05 (+27%) | 6,821.27 → 5,097.00 (-25%) |
| Compact during foreground traffic | 571.19 → 2,306.42 (+304%) | 1,448.42 → 1,789.74 (+24%) | 4,459.81 → 4,631.03 (+4%) |
| Warm Open, default history | 74.17 → 103.52 (+40%) | 193.11 → 199.21 (+3%) | 831.49 → 638.31 (-23%) |
| Cold Open, default history | 119.64 → 122.15 (+2%) | 268.37 → 266.00 (-1%) | 1,272.12 → 896.64 (-30%) |
| Warm Open, checkpoint | 27.07 → 37.07 (+37%) | 59.77 → 68.13 (+14%) | 274.28 → 207.25 (-24%) |
| Cold Open, checkpoint | 33.22 → 46.03 (+39%) | 84.16 → 95.98 (+14%) | 429.03 → 304.98 (-29%) |

## Memory

The dedicated release probe forces GC, releases the rebuilt in-memory key map and file registry, then samples the retained heap again. It divides the difference by the number of live keys. Values include Go map storage and key bytes. Peak process memory and allocator headroom have different meanings.

| Operation | 64 B | 1,024 B | 4,096 B |
| --- | ---: | ---: | ---: |
| HeapAlloc after full GC (MiB) | 1.24 → 1.26 (+2%) | 1.25 → 1.28 (+2%) | 1.25 → 1.29 (+3%) |
| Rebuilt index and registry, default history (B/key) | 147.46 → 147.76 (+0%) | 148.10 → 148.45 (+0%) | 150.39 → 150.95 (+0%) |
| Rebuilt index and registry, checkpoint (B/key) | 147.47 → 147.84 (+0%) | 147.69 → 148.01 (+0%) | 148.45 → 148.81 (+0%) |

## Verification and reproduction

All six artifacts passed the primary and diagnostic measurement oracles and the flushed/unflushed device controls. Each primary run used 4,096 keys, five repetitions, 10,240 history mutations, 160 guest-OS-cold Gets and ten cold Opens. The diagnostic run used 512 keys. The restored cardinalities, capture sequences and state digests matched in all 90 primary Backup windows. Each size also produced the same compaction and repack reopened-state digests across commits. The paused Scan retained its source files through checkpoint publication and released them afterward. The separate power-loss qualification campaign has its own workflow scope.

The guests used Linux/amd64, kernel `6.8.0-147-generic`, ext4 ordered mode, QEMU TCG and two virtual CPUs. Both arms used `go1.27.4`, `GOMAXPROCS=2` and `docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. Both used the same base image SHA-256 `04e8f2461a16a44e78a370284471f29616cdd9baf1893df25807b71f985b6c94`. The library-source manifest SHA-256 was `2b31e4b984cff2b5badfe891e930e74012b7d6e1a657b48de9f8636aac767f08` for v0.8.0 and `905abd3aaea9dbf72a5b9ebbe0fc56175e7e7e63f7cb62195483d1daeba2aba2` for the candidate.

| Value | v0.8.0, `dbb553eac611` | Candidate, `53479b39d2a9` |
| ---: | --- | --- |
| 64 B | AMD EPYC 9V74 80-Core, [run 44743204274](https://github.com/anaregdesign/tarp/actions/runs/44743204274) | AMD EPYC 7763 64-Core, [run 44746708269](https://github.com/anaregdesign/tarp/actions/runs/44746708269) |
| 1,024 B | AMD EPYC 9V45 96-Core, [run 44743211524](https://github.com/anaregdesign/tarp/actions/runs/44743211524) | Intel Xeon 6975P-C, [run 44746715090](https://github.com/anaregdesign/tarp/actions/runs/44746715090) |
| 4,096 B | AMD EPYC 7763 64-Core, [run 44743219566](https://github.com/anaregdesign/tarp/actions/runs/44743219566) | AMD EPYC 9V74 80-Core, [run 44746720645](https://github.com/anaregdesign/tarp/actions/runs/44746720645) |

The preliminary candidate `531b8136035c` was also measured before a memory-mode replay optimization. Its median warm Open times after checkpoint were 35.54 ms, 82.03 ms, 210.76 ms in ascending Value size. [64 B intermediate run](https://github.com/anaregdesign/tarp/actions/runs/44744932884) [1,024 B intermediate run](https://github.com/anaregdesign/tarp/actions/runs/44744940766) [4,096 B intermediate run](https://github.com/anaregdesign/tarp/actions/runs/44744948472)

Each run contains a `qualification-<run>-1` artifact retained for 30 days. Verify `control-verdict.json`, `measurement-verdict.json` and both `measurement_pass` records in `measurement.jsonl`. For call latency, concatenate the `samples_ns` arrays from matching `*_raw` rows in offset order within each repetition, pool all five repetitions, sort, and select nearest-rank p50, p95 or p99. For elapsed times, resource fields and bytes per key, take the median of the five per-window observations. Relative change is `100 × (candidate / baseline − 1)`. Percentages with a zero baseline have empty CSV cells.
