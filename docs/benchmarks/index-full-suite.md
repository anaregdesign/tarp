# Default index compared with v0.8.0

The full Go benchmark suite compares released v0.8.0 (`dbb553e`) and the new default Memory index at [`53479b39`](https://github.com/anaregdesign/tarp/tree/53479b39d2a970da29e1c8ac67a4706b05170c06). Both arms ran on the same Mac using the Docker Desktop Linux/arm64 VM, four CPUs, `GOMAXPROCS=4` and Go 1.27.4 from `docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. Each process used an immutable, read-only source snapshot.

All 96 workloads completed five repetitions on each revision. The geometric mean of the candidate-to-baseline median ns/op ratios is 1.017×. 11 workloads increased by more than 10%, 8 decreased by more than 10%, and the remaining 77 stayed within 10%. This calculation gives equal weight to each benchmark. Each round used separate Go test processes in the same pinned container, and the starting revision alternated between rounds. Guest filesystem page caches could remain warm. The CSV also reports the median change within matched rounds. The 100 ms target and five repetitions describe this run and provide no significance threshold.

The largest observed changes in median time were as follows.

| Workload | v0.8.0 ns/op | New Memory ns/op | Change |
| --- | ---: | ---: | ---: |
| `Scan/4096/paused-checkpoint` | 25,434,744.0 | 34,453,000.0 | +35.5% |
| `BatchWrite/raw/Put/keys=1/serial` | 635,785.0 | 848,918.0 | +33.5% |
| `BatchGet/hot-1/raw/BatchGet` | 60.5 | 74.2 | +22.8% |
| `MixedWriters/multi-writer` | 148,164,625.0 | 175,862,625.0 | +18.7% |
| `BatchWrite/raw/Put/keys=1/batch` | 1,042,540.0 | 665,203.0 | -36.2% |
| `Get/4096/application-hit-parallel` | 1,439.0 | 931.7 | -35.3% |
| `Scan/1024/paused-checkpoint` | 25,005,396.0 | 20,880,368.0 | -16.5% |
| `BatchWrite/raw/Put/keys=16/batch` | 944,986.0 | 795,359.0 | -15.8% |

The [complete workload table](index-full-suite.csv) also reports changes in allocations. The [baseline log](index-full-baseline.txt.gz) and [candidate log](index-full-candidate.txt.gz) retain every repetition, in-test correctness check and custom measurement. The [matched index comparison](index-modes.md) measures explicit B+tree and Memory selection. The [disposable guest profiles](index-vm.md) exercise three value sizes and independently verify the resulting database state.

## One-second follow-up

Six hot-read, durable-write and Scan workloads received a longer target of one second per repetition. Five rounds alternated which binary ran first in the same container. The median within-round change compares each candidate with its nearest baseline. The [complete timing log](index-full-followup.txt.gz) includes the run order and allocation counters.

| Workload | v0.8.0 median ns/op | Final median ns/op | Ratio of medians | Median within-round change |
| --- | ---: | ---: | ---: | ---: |
| `Get/4096/application-hit-parallel` | 1,755.0 | 1,347.0 | -23.2% | -22.3% |
| `DurablePut/4096/default-target` | 717,686.0 | 702,068.0 | -2.2% | +3.9% |
| `BatchWrite/raw/Put/keys=1/serial` | 755,314.0 | 762,004.0 | +0.9% | +4.8% |
| `BatchWrite/raw/Put/keys=16/serial` | 13,228,864.0 | 11,538,181.0 | -12.8% | -13.8% |
| `BatchGet/hot-1/typed-string/BatchGet` | 120.2 | 135.1 | +12.4% | +12.0% |
| `Scan/4096/full-warm` | 250,911.0 | 287,534.0 | +14.6% | +14.4% |

## Reproduction

Run both read-only source snapshots inside the same pinned container. Perform five rounds, alternating the starting revision. In each source directory run

```sh
go version
go env GOOS GOARCH
go test ./... -run '^$' -bench . -benchtime=100ms -benchmem -count=1
```

## Memory replay during Open

An intermediate build constructed a transaction map during replay and cloned the full index to calculate live statistics. The final Memory builder validates writes in a private map, measures its final entries and publishes that same map once. B+tree continues to bulk-load sorted runs. Compare the released library, the [intermediate implementation](https://github.com/anaregdesign/tarp/tree/fb5593bd0fcd72cf33a031966c816044a46b428b), and the final builder on the same closed fixtures.

| Layout | v0.8.0 ms | Intermediate ms | Final ms | Final vs v0.8.0 |
| --- | ---: | ---: | ---: | ---: |
| History | 6.73 | 12.96 | 7.43 | +10.5% |
| Checkpoint | 5.33 | 8.81 | 5.04 | -5.6% |

The same Linux/arm64 container and Go digest ran all three binaries. The released binary created one history DB and one checkpointed DB with 16,384 live keys, 28,672 total writes and 128 B values. The cleaner stayed parked. Six rounds rotated the three binaries, using one warmup followed by two timed Opens per layout and round. Each Open checked the live-key count and each Close succeeded. The [probe](../../benchmark/index_open_test.go) and [complete timings](index-open-replay.txt.gz) preserve the fixture definition and individual observations.
