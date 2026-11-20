# Default index compared with v0.8.0

The full Go benchmark suite compares released v0.8.0 (`dbb553e`) and the new default Memory index at [`fb5593bd`](https://github.com/anaregdesign/tarp/tree/fb5593bd0fcd72cf33a031966c816044a46b428b). Both arms ran on the same Mac using the Docker Desktop Linux/arm64 VM, four CPUs, `GOMAXPROCS=4` and Go 1.27.4 from `docker.io/library/golang@sha256:4fa7c968ba93f1635670522dbb01a2c78150443e1385746924566079f36570ea`. Each process used an immutable, read-only source snapshot.

All 96 workloads completed five repetitions on each revision. The geometric mean of the candidate-to-baseline median ns/op ratios is 0.979×. 14 workloads increased by more than 10%, 20 decreased by more than 10%, and the remaining 62 stayed within 10%. This calculation gives equal weight to each benchmark. The runs were sequential, in separate containers with warm guest filesystem caches possible. The 100 ms target and five repetitions describe this run and provide no significance threshold.

The largest observed changes in median time were as follows.

| Workload | v0.8.0 ns/op | New Memory ns/op | Change |
| --- | ---: | ---: | ---: |
| `BatchWrite/raw/Put/keys=16/serial` | 11,468,185.0 | 16,812,965.0 | +46.6% |
| `DurablePut/4096/default-target` | 646,242.0 | 880,541.0 | +36.3% |
| `Scan/1024/paused-checkpoint` | 16,950,286.0 | 21,639,016.0 | +27.7% |
| `Get/4096/application-hit-parallel` | 1,146.0 | 1,463.0 | +27.7% |
| `BatchWrite/raw/Put/keys=1/batch` | 1,363,362.0 | 708,021.0 | -48.1% |
| `BatchWrite/raw/Put/keys=1/serial` | 1,459,571.0 | 815,777.0 | -44.1% |
| `DurablePut/4096/one-record-target` | 2,260,412.0 | 1,417,228.0 | -37.3% |
| `BatchWrite/typed/Put/keys=16/batch` | 1,344,607.0 | 916,264.0 | -31.9% |

The [complete workload table](index-full-suite.csv) also reports changes in allocations. The [baseline log](index-full-baseline.txt.gz) and [candidate log](index-full-candidate.txt.gz) retain every repetition, in-test correctness check and custom measurement. The [matched index comparison](index-modes.md) measures explicit B+tree and Memory selection. The [disposable guest profiles](index-vm.md) exercise three value sizes and independently verify the resulting database state.

## Alternating follow-up

The short full-suite run showed higher candidate medians for three selected workloads. A second run kept the same Docker image, Go settings and source revisions, compiled each binary once and used a one-second benchmark target. Each of five rounds ran both binaries. The starting revision alternated between rounds. The table compares the median of five observations on each revision.

| Workload | v0.8.0 ns/op | New Memory ns/op | Ratio of medians | Median within-round change |
| --- | ---: | ---: | ---: | ---: |
| `Get/4096/application-hit-parallel` | 1,358.0 | 1,595.0 | +17.5% | +4.5% |
| `DurablePut/4096/default-target` | 826,027.0 | 783,121.0 | -5.2% | +4.4% |
| `BatchWrite/raw/Put/keys=16/serial` | 12,481,890.0 | 14,116,090.0 | +13.1% | +0.1% |

The [paired run log](index-full-followup.txt.gz) retains the arm order, every observation and allocation counters. Comparing each candidate with the baseline in its own round controls for some timing drift. The variation between rounds limits how precisely these ratios estimate a lasting change.

The command inside each pinned container for the full suite was

```sh
go version
go env GOOS GOARCH
go test ./... -run '^$' -bench . -benchtime=100ms -benchmem -count=5
```
