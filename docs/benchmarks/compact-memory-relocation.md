# Memory-index checkpoint relocation

[Issue #108](https://github.com/anaregdesign/tarp/issues/108) follows the [same-host investigation](compact-64-same-host.md) into manual Compact with the default in-memory index. Each comparison uses v0.9.0 as its baseline and runs both binaries on the same guest.

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
